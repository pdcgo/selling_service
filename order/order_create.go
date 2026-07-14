package order

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"connectrpc.com/connect"
	"github.com/pdcgo/invoice_service/invoice_v2"
	inventory_iface "github.com/pdcgo/schema/services/inventory_iface/v1"
	invoice_iface "github.com/pdcgo/schema/services/invoice_iface/v2"
	product_iface "github.com/pdcgo/schema/services/product_iface/v1"
	selling_iface "github.com/pdcgo/schema/services/selling_iface/v3"
	warehouse_iface "github.com/pdcgo/schema/services/warehouse_iface/v1"
	"github.com/pdcgo/selling_service/selling_models"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/user_service/access_interceptors"
	"gorm.io/gorm"
)

// OrderCreate implements [selling_ifaceconnect.OrderServiceHandler] (v3). Flow A —
// the full create dialogue (plans/brainstroming.md §6):
//
//	pre-tx:  shop belongs to team → ProductOrderInfo (unknown / cross-locked reject)
//	         → GetWarehouseFee
//	one tx:  order + customer + status log → inventory TransactionCreate{order}
//	         (stock OUT + racks auto-picked; short stock hard-rejects) → items with
//	         frozen fee context (fee = count × Markup(unit_cost) per non-owned item)
//	         → PostBalanceLog PRODUCT_FEE per owner + WAREHOUSE_FEE (in-process,
//	         creditor-side RECEIVABLE — the debtor PAYABLE mirror is automatic)
//	         → freeze inventory_transaction_id → commit
//
// The inventory call is the only remote write; a local failure after it triggers a
// best-effort compensating TransactionCancel.
func (s *orderServiceImpl) OrderCreate(
	ctx context.Context,
	req *connect.Request[selling_iface.OrderCreateRequest],
) (*connect.Response[selling_iface.OrderCreateResponse], error) {
	pay := req.Msg
	if pay.GetTeamId() == 0 || pay.GetShopId() == 0 || pay.GetWarehouseId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("team_id, shop_id and warehouse_id are required"))
	}
	if pay.GetRefId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("ref_id is required"))
	}
	if len(pay.GetItems()) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("items are required"))
	}
	for _, it := range pay.GetItems() {
		if it.GetProductId() == 0 || it.GetCount() <= 0 || it.GetPrice() < 0 {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("each item needs product_id, count > 0 and price >= 0"))
		}
	}

	db := s.db.WithContext(ctx)

	// Shop belongs to the team (alive — gorm auto-excludes soft-deleted shops).
	var shop selling_models.Shop
	res := db.
		Where("id = ? AND team_id = ?", pay.GetShopId(), pay.GetTeamId()).
		Limit(1).
		Find(&shop)
	if res.Error != nil {
		return nil, res.Error
	}
	if shop.ID == 0 {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("shop not found in this team"))
	}

	// Product data — delegated to product_service (one batched call).
	productIDs := []uint64{}
	seen := map[uint64]bool{}
	for _, it := range pay.GetItems() {
		if !seen[it.GetProductId()] {
			seen[it.GetProductId()] = true
			productIDs = append(productIDs, it.GetProductId())
		}
	}
	infoRes, err := s.product.ProductOrderInfo(ctx, connect.NewRequest(&product_iface.ProductOrderInfoRequest{
		ProductIds: productIDs,
		TeamId:     pay.GetTeamId(),
	}))
	if err != nil {
		return nil, err
	}
	info := infoRes.Msg.GetItems()
	for _, pid := range productIDs {
		pi, ok := info[pid]
		if !ok {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown product %d", pid))
		}
		if !pi.GetOwned() && pi.GetCrossLocked() {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("product %d is cross-locked and not owned by this team", pid))
		}
	}

	// Order value (selling prices) → warehouse fee (delegated, pre-tx read).
	orderValue := 0.0
	for _, it := range pay.GetItems() {
		orderValue += float64(it.GetCount()) * it.GetPrice()
	}
	feeRes, err := s.warehouse.GetWarehouseFee(ctx, connect.NewRequest(&warehouse_iface.GetWarehouseFeeRequest{
		WarehouseId: pay.GetWarehouseId(),
		OrderValue:  orderValue,
	}))
	if err != nil {
		return nil, err
	}
	warehouseFee := feeRes.Msg.GetFee()

	// The caller (set by the access interceptor); 0 for direct/in-process calls.
	var callerID uint64
	if identity, idErr := access_interceptors.GetIdentityFromCtx(ctx); idErr == nil {
		callerID = uint64(identity.IdentityId)
	}

	// Owe-limit gate (explicit, pre-tx): block if the ordering team already owes any
	// creditor (fulfilling warehouse + each cross-product owner) at/above its configured
	// threshold. Debt is read from the invoice v2 ledger in-process; the fee-posting path
	// enforces nothing. Creditor ids are all known pre-tx from ProductOrderInfo.
	creditorSet := map[uint64]struct{}{}
	if pay.GetWarehouseId() != pay.GetTeamId() {
		creditorSet[pay.GetWarehouseId()] = struct{}{}
	}
	for _, it := range pay.GetItems() {
		pi := info[it.GetProductId()]
		if pi != nil && !pi.GetOwned() && pi.GetOwnerTeamId() != pay.GetTeamId() {
			creditorSet[pi.GetOwnerTeamId()] = struct{}{}
		}
	}
	if len(creditorSet) > 0 {
		creditorIDs := make([]uint64, 0, len(creditorSet))
		for cid := range creditorSet {
			creditorIDs = append(creditorIDs, cid)
		}
		sort.Slice(creditorIDs, func(i, j int) bool { return creditorIDs[i] < creditorIDs[j] })
		canOwe, oweErr := invoice_v2.EvaluateOweLimits(db, pay.GetTeamId(), creditorIDs)
		if oweErr != nil {
			return nil, oweErr
		}
		for _, cid := range creditorIDs {
			allow := canOwe[cid]
			if allow != nil && !allow.GetAllow() {
				return nil, connect.NewError(connect.CodeFailedPrecondition,
					fmt.Errorf("owe limit reached for team %d (owes %.0f of %.0f)", cid, allow.GetActiveAmount(), allow.GetThreshold()))
			}
		}
	}

	now := time.Now()
	resp := &selling_iface.OrderCreateResponse{}
	var invTxID uint64

	err = db.Transaction(func(tx *gorm.DB) error {
		// Duplicate ref pre-check (the unique index remains the race guard).
		var existing selling_models.Order
		res := tx.
			Where("team_id = ? AND ref_id = ?", pay.GetTeamId(), pay.GetRefId()).
			Limit(1).
			Find(&existing)
		if res.Error != nil {
			return res.Error
		}
		if existing.ID != 0 {
			return connect.NewError(connect.CodeAlreadyExists, errors.New("an order with this ref_id already exists"))
		}

		ord := selling_models.Order{
			TeamID:           pay.GetTeamId(),
			ShopID:           pay.GetShopId(),
			WarehouseID:      pay.GetWarehouseId(),
			RefID:            pay.GetRefId(),
			Status:           selling_models.OrderStatusCreated,
			ShipmentStatus:   selling_models.ShipmentStatusNone,
			ShippingFee:      pay.GetShippingFee(),
			Total:            orderValue + pay.GetShippingFee(),
			EstRevenueAmount: pay.GetEstRevenueAmount(),
			WarehouseFee:     warehouseFee,
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		err := tx.Create(&ord).Error
		if err != nil {
			return err
		}

		if c := pay.GetCustomer(); c != nil {
			customer := selling_models.OrderCustomer{
				OrderID:    ord.ID,
				Name:       c.GetName(),
				Phone:      c.GetPhone(),
				Province:   c.GetProvince(),
				City:       c.GetCity(),
				District:   c.GetDistrict(),
				PostalCode: c.GetPostalCode(),
				Address:    c.GetAddress(),
			}
			err = tx.Create(&customer).Error
			if err != nil {
				return err
			}
		}

		err = appendOrderStatusLog(tx, ord.ID, selling_models.OrderDimensionFulfillment,
			"", string(selling_models.OrderStatusCreated), "", now)
		if err != nil {
			return err
		}

		// Stock OUT (remote): valued at the warehouse's average cost, racks
		// auto-picked per ProductConfig, short stock hard-rejects (FailedPrecondition
		// passes through). The response carries the per-product cost valuation.
		tcItems := make([]*inventory_iface.TransactionItem, 0, len(pay.GetItems()))
		for _, it := range pay.GetItems() {
			tcItems = append(tcItems, &inventory_iface.TransactionItem{
				ProductId: it.GetProductId(),
				Count:     it.GetCount(),
				// price is ignored for the order kind (inventory values at cost).
			})
		}
		tcRes, err := s.inventory.TransactionCreate(ctx, connect.NewRequest(&inventory_iface.TransactionCreateRequest{
			TeamId:      pay.GetTeamId(),
			WarehouseId: pay.GetWarehouseId(),
			Tx: &inventory_iface.TransactionCreateRequest_Order{
				Order: &inventory_iface.TransactionOrder{Items: tcItems},
			},
		}))
		if err != nil {
			return err
		}
		invTxID = tcRes.Msg.GetTransactionId()
		unitCost := map[uint64]float64{}
		for _, ci := range tcRes.Msg.GetItems() {
			unitCost[ci.GetProductId()] = ci.GetUnitCost()
		}

		// Items with the frozen fee context. Product fee (legacy formula) =
		// count × Markup(unit_cost) per NON-OWNED item, grouped per owning team.
		productFeeByOwner := map[uint64]float64{}
		for _, it := range pay.GetItems() {
			pi := info[it.GetProductId()]
			fee := 0.0
			if !pi.GetOwned() {
				markedUp := db_models.MarkupValue(pi.GetMarkupPercent()).Markup(unitCost[it.GetProductId()])
				fee = float64(it.GetCount()) * markedUp
				productFeeByOwner[pi.GetOwnerTeamId()] += fee
			}
			item := selling_models.OrderItem{
				OrderID:       ord.ID,
				ProductID:     it.GetProductId(),
				ProductName:   pi.GetName(),
				Count:         it.GetCount(),
				Price:         it.GetPrice(),
				Subtotal:      float64(it.GetCount()) * it.GetPrice(),
				Owned:         pi.GetOwned(),
				OwnerTeamID:   pi.GetOwnerTeamId(),
				MarkupPercent: pi.GetMarkupPercent(),
				ProductFee:    fee,
			}
			err = tx.Create(&item).Error
			if err != nil {
				return err
			}
		}

		// Fees → invoice v2 ledger, in-process and atomic with the order. Posted
		// creditor-side with RECEIVABLE; the debtor PAYABLE mirror is created inside
		// by the double-entry (restock-accept precedent). orderSrc attributes both
		// legs to this order so its fee history is queryable via ListTeamBalanceLog.
		orderSrc := &invoice_v2.OrderSource{
			OrderSystem: invoice_iface.OrderSystem_ORDER_SYSTEM_V3,
			OrderID:     ord.ID,
			TeamID:      pay.GetTeamId(),
			WarehouseID: pay.GetWarehouseId(),
		}
		owners := make([]uint64, 0, len(productFeeByOwner))
		for owner := range productFeeByOwner {
			owners = append(owners, owner)
		}
		sort.Slice(owners, func(i, j int) bool { return owners[i] < owners[j] })
		for _, owner := range owners {
			fee := productFeeByOwner[owner]
			if fee <= 0 || owner == pay.GetTeamId() {
				continue
			}
			err = invoice_v2.PostBalanceLog(tx, owner, pay.GetTeamId(),
				invoice_iface.BalanceChangeType_BALANCE_CHANGE_TYPE_PRODUCT_FEE,
				fee, invoice_iface.BalanceType_BALANCE_TYPE_RECEIVABLE,
				fmt.Sprintf("order %s cross product fee", ord.RefID), callerID, now,
				orderSrc)
			if err != nil {
				return err
			}
		}
		if warehouseFee > 0 && pay.GetWarehouseId() != pay.GetTeamId() {
			err = invoice_v2.PostBalanceLog(tx, pay.GetWarehouseId(), pay.GetTeamId(),
				invoice_iface.BalanceChangeType_BALANCE_CHANGE_TYPE_WAREHOUSE_FEE,
				warehouseFee, invoice_iface.BalanceType_BALANCE_TYPE_RECEIVABLE,
				fmt.Sprintf("order %s warehouse fee", ord.RefID), callerID, now,
				orderSrc)
			if err != nil {
				return err
			}
		}

		err = tx.
			Model(&selling_models.Order{}).
			Where("id = ?", ord.ID).
			Update("inventory_transaction_id", invTxID).
			Error
		if err != nil {
			return err
		}

		resp.OrderId = ord.ID
		return nil
	})
	if err != nil {
		// The stock-out is the only remote write; if anything after it failed the
		// local tx rolled back — compensate the orphaned inventory transaction
		// (best effort; TransactionCancel is idempotent).
		if invTxID != 0 {
			_, cancelErr := s.inventory.TransactionCancel(ctx, connect.NewRequest(&inventory_iface.TransactionCancelRequest{
				TransactionId: invTxID,
				WarehouseId:   pay.GetWarehouseId(),
			}))
			if cancelErr != nil {
				slog.Error("order create compensation failed: orphaned inventory transaction",
					"transaction_id", invTxID, "err", cancelErr)
			}
		}
		return nil, err
	}

	return connect.NewResponse(resp), nil
}
