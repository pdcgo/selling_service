package order

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"connectrpc.com/connect"
	"github.com/pdcgo/invoice_service/invoice_v2"
	inventory_iface "github.com/pdcgo/schema/services/inventory_iface/v1"
	invoice_iface "github.com/pdcgo/schema/services/invoice_iface/v2"
	selling_iface "github.com/pdcgo/schema/services/selling_iface/v3"
	"github.com/pdcgo/selling_service/selling_models"
	"github.com/pdcgo/user_service/access_interceptors"
	"gorm.io/gorm"
)

// OrderCancel implements [selling_ifaceconnect.OrderServiceHandler] (v3). Full
// reversal of a not-yet-shipped order: mirrored reverse fee entries (invoice
// push_handler's cancel pattern — pair swapped, balance type flipped to PAYABLE),
// the canceled flag + a flag-dimension status log, and — the last step, so a remote
// failure rolls everything back — inventory TransactionCancel (stock + placements
// return). Idempotent: canceling a canceled order is a no-op.
func (s *orderServiceImpl) OrderCancel(
	ctx context.Context,
	req *connect.Request[selling_iface.OrderCancelRequest],
) (*connect.Response[selling_iface.OrderCancelResponse], error) {
	pay := req.Msg
	if pay.GetOrderId() == 0 || pay.GetTeamId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("order_id and team_id are required"))
	}

	var callerID uint64
	if identity, idErr := access_interceptors.GetIdentityFromCtx(ctx); idErr == nil {
		callerID = uint64(identity.IdentityId)
	}

	now := time.Now()
	err := s.db.
		WithContext(ctx).
		Transaction(func(tx *gorm.DB) error {
			var ord selling_models.Order
			res := tx.
				Where("id = ? AND team_id = ?", pay.GetOrderId(), pay.GetTeamId()).
				Limit(1).
				Find(&ord)
			if res.Error != nil {
				return res.Error
			}
			if ord.ID == 0 {
				return connect.NewError(connect.CodeNotFound, errors.New("order not found"))
			}
			if ord.CanceledAt != nil {
				return nil // idempotent
			}
			if ord.Status == selling_models.OrderStatusShipped ||
				ord.ShipmentStatus != selling_models.ShipmentStatusNone {
				return connect.NewError(connect.CodeFailedPrecondition,
					errors.New("order already shipped — cancel is no longer possible (use the return flow)"))
			}

			// Reverse the fee postings (mirror direction: pair swapped, PAYABLE — the
			// exact push_handler OrderCanceled pattern; create+cancel nets to zero).
			var items []selling_models.OrderItem
			err := tx.
				Where("order_id = ?", ord.ID).
				Find(&items).
				Error
			if err != nil {
				return err
			}
			productFeeByOwner := map[uint64]float64{}
			for _, it := range items {
				if !it.Owned && it.ProductFee > 0 {
					productFeeByOwner[it.OwnerTeamID] += it.ProductFee
				}
			}
			// Reversal legs carry the same order attribution as the create legs, so the
			// order's full fee history (post + reversal, netting to zero) is queryable.
			orderSrc := &invoice_v2.OrderSource{
				OrderSystem: invoice_iface.OrderSystem_ORDER_SYSTEM_V3,
				OrderID:     ord.ID,
				TeamID:      ord.TeamID,
				WarehouseID: ord.WarehouseID,
			}
			owners := make([]uint64, 0, len(productFeeByOwner))
			for owner := range productFeeByOwner {
				owners = append(owners, owner)
			}
			sort.Slice(owners, func(i, j int) bool { return owners[i] < owners[j] })
			for _, owner := range owners {
				fee := productFeeByOwner[owner]
				if fee <= 0 || owner == ord.TeamID {
					continue
				}
				err = invoice_v2.PostBalanceLog(tx, ord.TeamID, owner,
					invoice_iface.BalanceChangeType_BALANCE_CHANGE_TYPE_PRODUCT_FEE,
					fee, invoice_iface.BalanceType_BALANCE_TYPE_PAYABLE,
					fmt.Sprintf("cancel order %s cross product fee", ord.RefID), callerID, now,
					orderSrc)
				if err != nil {
					return err
				}
			}
			if ord.WarehouseFee > 0 && ord.WarehouseID != ord.TeamID {
				err = invoice_v2.PostBalanceLog(tx, ord.TeamID, ord.WarehouseID,
					invoice_iface.BalanceChangeType_BALANCE_CHANGE_TYPE_WAREHOUSE_FEE,
					ord.WarehouseFee, invoice_iface.BalanceType_BALANCE_TYPE_PAYABLE,
					fmt.Sprintf("cancel order %s warehouse fee", ord.RefID), callerID, now,
					orderSrc)
				if err != nil {
					return err
				}
			}

			// Canceled flag + flag-dimension log.
			err = tx.
				Model(&selling_models.Order{}).
				Where("id = ?", ord.ID).
				Updates(map[string]interface{}{
					"canceled_at": now,
					"updated_at":  now,
				}).
				Error
			if err != nil {
				return err
			}
			err = appendOrderStatusLog(tx, ord.ID, selling_models.OrderDimensionFlag,
				"", "canceled", "", now)
			if err != nil {
				return err
			}

			// Remote reversal LAST: stock + placements return (TransactionCancel is
			// idempotent). Its failure rolls back everything local — the order stays
			// active and consistent.
			if ord.InventoryTransactionID != 0 {
				_, err = s.inventory.TransactionCancel(ctx, connect.NewRequest(&inventory_iface.TransactionCancelRequest{
					TransactionId: ord.InventoryTransactionID,
					WarehouseId:   ord.WarehouseID,
				}))
				if err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&selling_iface.OrderCancelResponse{}), nil
}
