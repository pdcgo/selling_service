package order_test

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/golang/mock/gomock"
	"github.com/pdcgo/invoice_service/invoice_models"
	common "github.com/pdcgo/schema/services/common/v1"
	inventory_iface "github.com/pdcgo/schema/services/inventory_iface/v1"
	"github.com/pdcgo/schema/services/inventory_iface/v1/inventory_iface_mock"
	invoice_iface "github.com/pdcgo/schema/services/invoice_iface/v2"
	product_iface "github.com/pdcgo/schema/services/product_iface/v1"
	"github.com/pdcgo/schema/services/product_iface/v1/product_iface_mock"
	selling_iface "github.com/pdcgo/schema/services/selling_iface/v3"
	warehouse_iface "github.com/pdcgo/schema/services/warehouse_iface/v1"
	"github.com/pdcgo/schema/services/warehouse_iface/v1/warehouse_iface_mock"
	"github.com/pdcgo/selling_service/order"
	"github.com/pdcgo/selling_service/selling_models"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/pdcgo/shared/pkg/moretest/moretest_mock"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

var commonPage = common.PageFilter{Page: 1, Limit: 20}

// Authorization is enforced by the access interceptor (proto request_policy); these
// tests call the impl directly with gomock connect clients for the cross-service
// dialogues (inventory / product / warehouse) and assert the DB + invoice-ledger
// effects.
func TestOrderService(t *testing.T) {
	var scenario moretest_mock.DbScenario
	moretest.Suite(t, "v3 order service",
		moretest.SetupListFunc{moretest_mock.MockPostgresDatabase(&scenario)},
		func(t *testing.T) {
			scenario(t, func(tx *gorm.DB) {
				assert.NoError(t, tx.AutoMigrate(
					&selling_models.Order{},
					&selling_models.OrderItem{},
					&selling_models.OrderCustomer{},
					&selling_models.OrderStatusLog{},
					&selling_models.Shop{},
					// Fees post to the invoice v2 ledger in-process (restock precedent).
					&invoice_models.TeamBalance{},
					&invoice_models.BalanceChangeLog{},
					&invoice_models.TeamBalanceDailyLog{},
					&invoice_models.BalanceChangeOrderSource{},
					// The pre-tx owe-limit gate reads this config table.
					&db_models.OweLimitConfiguration{},
				))

				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				inv := inventory_iface_mock.NewMockInventoryServiceClient(ctrl)
				prod := product_iface_mock.NewMockProductServiceClient(ctrl)
				wh := warehouse_iface_mock.NewMockWarehouseServiceClient(ctrl)
				svc := order.NewOrderService(tx, inv, prod, wh)
				ctx := context.Background()

				// A shop of team 1.
				shop := selling_models.Shop{TeamID: 1, ShopName: "Toko", ShopUsername: "toko_1"}
				assert.NoError(t, tx.Create(&shop).Error)

				// product_service behavior: 1 = owned, 2 = cross (owner 8, markup 20%),
				// 3 = cross-locked (owner 8). Unknown ids omitted.
				catalog := map[uint64]*product_iface.ProductOrderInfoItem{
					1: {Name: "Owned", OwnerTeamId: 1, Owned: true},
					2: {Name: "Cross", OwnerTeamId: 8, Owned: false, MarkupPercent: 20},
					3: {Name: "Locked", OwnerTeamId: 8, Owned: false, CrossLocked: true},
				}
				prod.EXPECT().ProductOrderInfo(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, req *connect.Request[product_iface.ProductOrderInfoRequest]) (*connect.Response[product_iface.ProductOrderInfoResponse], error) {
						items := map[uint64]*product_iface.ProductOrderInfoItem{}
						for _, pid := range req.Msg.GetProductIds() {
							if it, ok := catalog[pid]; ok {
								items[pid] = it
							}
						}
						return connect.NewResponse(&product_iface.ProductOrderInfoResponse{Items: items}), nil
					}).AnyTimes()

				// warehouse fee: flat 1000 for every order value.
				wh.EXPECT().GetWarehouseFee(gomock.Any(), gomock.Any()).Return(
					connect.NewResponse(&warehouse_iface.GetWarehouseFeeResponse{Fee: 1000}), nil).AnyTimes()

				balance := func(team, forTeam uint64, bt invoice_iface.BalanceType) float64 {
					var b invoice_models.TeamBalance
					res := tx.
						Where("team_id = ? AND for_team_id = ? AND balance_type = ?", team, forTeam, bt).
						Limit(1).
						Find(&b)
					assert.NoError(t, res.Error)
					return b.Balance
				}

				var orderID uint64

				t.Run("create happy path — order + items + customer + log + stock + fees", func(t *testing.T) {
					inv.EXPECT().TransactionCreate(gomock.Any(), gomock.Any()).Return(
						connect.NewResponse(&inventory_iface.TransactionCreateResponse{
							TransactionId: 555,
							Items: []*inventory_iface.TransactionCostItem{
								{ProductId: 1, Count: 2, UnitCost: 4, TotalCost: 8},
								{ProductId: 2, Count: 1, UnitCost: 10, TotalCost: 10},
							},
						}), nil).Times(1)

					res, err := svc.OrderCreate(ctx, connect.NewRequest(&selling_iface.OrderCreateRequest{
						TeamId:      1,
						ShopId:      shop.ID,
						WarehouseId: 9,
						RefId:       "MP-001",
						Items: []*selling_iface.OrderItemInput{
							{ProductId: 1, Count: 2, Price: 100},
							{ProductId: 2, Count: 1, Price: 50},
						},
						Customer: &selling_iface.OrderCustomer{
							Name: "Budi", Phone: "0812", Province: "Jawa Barat",
							City: "Bandung", District: "Coblong", PostalCode: "40132",
							Address: "Jl. Dago 1",
						},
						ShippingFee:      20,
						EstRevenueAmount: 260,
					}))
					assert.NoError(t, err)
					orderID = res.Msg.OrderId
					assert.NotZero(t, orderID)

					var ord selling_models.Order
					assert.NoError(t, tx.First(&ord, orderID).Error)
					assert.Equal(t, selling_models.OrderStatusCreated, ord.Status)
					assert.Equal(t, selling_models.ShipmentStatusNone, ord.ShipmentStatus)
					assert.Nil(t, ord.CanceledAt)
					assert.Equal(t, uint64(555), ord.InventoryTransactionID)
					assert.InDelta(t, 270, ord.Total, 0.001) // 2×100 + 1×50 + 20 shipping
					assert.InDelta(t, 1000, ord.WarehouseFee, 0.001)
					assert.InDelta(t, 260, ord.EstRevenueAmount, 0.001)

					// Items: cross item fee = count × Markup(unit_cost) =
					// 1 × ceil(10×100/(100−20)) = 13; owned item fee 0.
					var items []selling_models.OrderItem
					assert.NoError(t, tx.Where("order_id = ?", orderID).Order("product_id ASC").Find(&items).Error)
					assert.Len(t, items, 2)
					assert.True(t, items[0].Owned)
					assert.InDelta(t, 0, items[0].ProductFee, 0.001)
					assert.False(t, items[1].Owned)
					assert.Equal(t, uint64(8), items[1].OwnerTeamID)
					assert.InDelta(t, 13, items[1].ProductFee, 0.001)
					assert.Equal(t, "Cross", items[1].ProductName)

					// Customer 1:1 row.
					var cust selling_models.OrderCustomer
					assert.NoError(t, tx.Where("order_id = ?", orderID).First(&cust).Error)
					assert.Equal(t, "Budi", cust.Name)
					assert.Equal(t, "40132", cust.PostalCode)

					// Fulfillment create log.
					var logs []selling_models.OrderStatusLog
					assert.NoError(t, tx.Where("order_id = ?", orderID).Find(&logs).Error)
					assert.Len(t, logs, 1)
					assert.Equal(t, selling_models.OrderDimensionFulfillment, logs[0].Dimension)
					assert.Equal(t, "created", logs[0].ToStatus)

					// Ledger: creditor-side RECEIVABLE + mirrored debtor PAYABLE.
					assert.InDelta(t, 13, balance(8, 1, invoice_iface.BalanceType_BALANCE_TYPE_RECEIVABLE), 0.001)
					assert.InDelta(t, -13, balance(1, 8, invoice_iface.BalanceType_BALANCE_TYPE_PAYABLE), 0.001)
					assert.InDelta(t, 1000, balance(9, 1, invoice_iface.BalanceType_BALANCE_TYPE_RECEIVABLE), 0.001)
					assert.InDelta(t, -1000, balance(1, 9, invoice_iface.BalanceType_BALANCE_TYPE_PAYABLE), 0.001)
				})

				t.Run("duplicate ref is AlreadyExists (inventory never called)", func(t *testing.T) {
					_, err := svc.OrderCreate(ctx, connect.NewRequest(&selling_iface.OrderCreateRequest{
						TeamId: 1, ShopId: shop.ID, WarehouseId: 9, RefId: "MP-001",
						Items: []*selling_iface.OrderItemInput{{ProductId: 1, Count: 1, Price: 10}},
					}))
					assert.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
				})

				t.Run("unknown product / cross-locked / foreign shop reject", func(t *testing.T) {
					_, err := svc.OrderCreate(ctx, connect.NewRequest(&selling_iface.OrderCreateRequest{
						TeamId: 1, ShopId: shop.ID, WarehouseId: 9, RefId: "MP-X",
						Items: []*selling_iface.OrderItemInput{{ProductId: 999, Count: 1, Price: 10}},
					}))
					assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

					_, err = svc.OrderCreate(ctx, connect.NewRequest(&selling_iface.OrderCreateRequest{
						TeamId: 1, ShopId: shop.ID, WarehouseId: 9, RefId: "MP-X",
						Items: []*selling_iface.OrderItemInput{{ProductId: 3, Count: 1, Price: 10}},
					}))
					assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

					_, err = svc.OrderCreate(ctx, connect.NewRequest(&selling_iface.OrderCreateRequest{
						TeamId: 1, ShopId: 424242, WarehouseId: 9, RefId: "MP-X",
						Items: []*selling_iface.OrderItemInput{{ProductId: 1, Count: 1, Price: 10}},
					}))
					assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
				})

				t.Run("insufficient stock passes FailedPrecondition through, nothing written", func(t *testing.T) {
					inv.EXPECT().TransactionCreate(gomock.Any(), gomock.Any()).Return(
						nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("insufficient stock"))).Times(1)

					_, err := svc.OrderCreate(ctx, connect.NewRequest(&selling_iface.OrderCreateRequest{
						TeamId: 1, ShopId: shop.ID, WarehouseId: 9, RefId: "MP-002",
						Items: []*selling_iface.OrderItemInput{{ProductId: 1, Count: 100, Price: 10}},
					}))
					assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

					var n int64
					assert.NoError(t, tx.Model(&selling_models.Order{}).Where("ref_id = ?", "MP-002").Count(&n).Error)
					assert.Equal(t, int64(0), n) // rolled back
				})

				t.Run("list + filters", func(t *testing.T) {
					list := func(req *selling_iface.OrderListRequest) *selling_iface.OrderListResponse {
						req.Page = &commonPage
						res, err := svc.OrderList(ctx, connect.NewRequest(req))
						assert.NoError(t, err)
						return res.Msg
					}
					res := list(&selling_iface.OrderListRequest{TeamId: 1})
					assert.Len(t, res.Data, 1)
					assert.Equal(t, "MP-001", res.Data[0].RefId)
					assert.Equal(t, "Budi", res.Data[0].CustomerName)
					assert.Equal(t, int64(3), res.Data[0].ItemCount)
					assert.Equal(t, selling_iface.OrderStatus_ORDER_STATUS_CREATED, res.Data[0].Status)

					assert.Len(t, list(&selling_iface.OrderListRequest{TeamId: 1, Search: "budi"}).Data, 1)
					assert.Len(t, list(&selling_iface.OrderListRequest{TeamId: 1, Search: "nomatch"}).Data, 0)
					assert.Len(t, list(&selling_iface.OrderListRequest{TeamId: 2}).Data, 0)

					f := false
					assert.Len(t, list(&selling_iface.OrderListRequest{TeamId: 1, Canceled: &f}).Data, 1)
					tr := true
					assert.Len(t, list(&selling_iface.OrderListRequest{TeamId: 1, Canceled: &tr}).Data, 0)
				})

				t.Run("detail", func(t *testing.T) {
					res, err := svc.OrderDetail(ctx, connect.NewRequest(&selling_iface.OrderDetailRequest{
						OrderId: orderID, TeamId: 1,
					}))
					assert.NoError(t, err)
					assert.Equal(t, "MP-001", res.Msg.RefId)
					assert.Len(t, res.Msg.Items, 2)
					assert.NotNil(t, res.Msg.Customer)
					assert.Equal(t, "Budi", res.Msg.Customer.Name)
					assert.Len(t, res.Msg.StatusLogs, 1)
					assert.InDelta(t, 1000, res.Msg.WarehouseFee, 0.001)
					assert.InDelta(t, 260, res.Msg.EstRevenueAmount, 0.001)

					_, err = svc.OrderDetail(ctx, connect.NewRequest(&selling_iface.OrderDetailRequest{
						OrderId: orderID, TeamId: 2,
					}))
					assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
				})

				t.Run("cancel reverses fees + stock, idempotent", func(t *testing.T) {
					inv.EXPECT().TransactionCancel(gomock.Any(), gomock.Any()).Return(
						connect.NewResponse(&inventory_iface.TransactionCancelResponse{}), nil).Times(1)

					_, err := svc.OrderCancel(ctx, connect.NewRequest(&selling_iface.OrderCancelRequest{
						OrderId: orderID, TeamId: 1,
					}))
					assert.NoError(t, err)

					var ord selling_models.Order
					assert.NoError(t, tx.First(&ord, orderID).Error)
					assert.NotNil(t, ord.CanceledAt)

					// Fees net to zero on every pair balance.
					assert.InDelta(t, 0, balance(8, 1, invoice_iface.BalanceType_BALANCE_TYPE_RECEIVABLE), 0.001)
					assert.InDelta(t, 0, balance(1, 8, invoice_iface.BalanceType_BALANCE_TYPE_PAYABLE), 0.001)
					assert.InDelta(t, 0, balance(9, 1, invoice_iface.BalanceType_BALANCE_TYPE_RECEIVABLE), 0.001)
					assert.InDelta(t, 0, balance(1, 9, invoice_iface.BalanceType_BALANCE_TYPE_PAYABLE), 0.001)

					// Flag-dimension log written.
					var logs []selling_models.OrderStatusLog
					assert.NoError(t, tx.Where("order_id = ? AND dimension = ?", orderID, selling_models.OrderDimensionFlag).Find(&logs).Error)
					assert.Len(t, logs, 1)
					assert.Equal(t, "canceled", logs[0].ToStatus)

					// Idempotent: no second TransactionCancel (Times(1) above enforces it).
					_, err = svc.OrderCancel(ctx, connect.NewRequest(&selling_iface.OrderCancelRequest{
						OrderId: orderID, TeamId: 1,
					}))
					assert.NoError(t, err)

					// Canceled orders show up under the canceled filter.
					tr := true
					res, err := svc.OrderList(ctx, connect.NewRequest(&selling_iface.OrderListRequest{
						TeamId: 1, Canceled: &tr, Page: &commonPage,
					}))
					assert.NoError(t, err)
					assert.Len(t, res.Msg.Data, 1)
				})

				t.Run("cancel unknown order is NotFound", func(t *testing.T) {
					_, err := svc.OrderCancel(ctx, connect.NewRequest(&selling_iface.OrderCancelRequest{
						OrderId: 999999, TeamId: 1,
					}))
					assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
				})
			})
		})
}

// The pre-tx owe-limit gate blocks a create when the ordering team already owes a
// creditor at/above its threshold — inventory is never touched, nothing is created.
func TestOrderCreateOweLimitBlocked(t *testing.T) {
	var scenario moretest_mock.DbScenario
	moretest.Suite(t, "v3 order owe-limit gate",
		moretest.SetupListFunc{moretest_mock.MockPostgresDatabase(&scenario)},
		func(t *testing.T) {
			scenario(t, func(tx *gorm.DB) {
				assert.NoError(t, tx.AutoMigrate(
					&selling_models.Order{},
					&selling_models.OrderItem{},
					&selling_models.OrderCustomer{},
					&selling_models.OrderStatusLog{},
					&selling_models.Shop{},
					&invoice_models.TeamBalance{},
					&db_models.OweLimitConfiguration{},
				))

				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				inv := inventory_iface_mock.NewMockInventoryServiceClient(ctrl)
				prod := product_iface_mock.NewMockProductServiceClient(ctrl)
				wh := warehouse_iface_mock.NewMockWarehouseServiceClient(ctrl)
				svc := order.NewOrderService(tx, inv, prod, wh)
				ctx := context.Background()

				shop := selling_models.Shop{TeamID: 1, ShopName: "Toko", ShopUsername: "toko_1"}
				assert.NoError(t, tx.Create(&shop).Error)

				// Cross owner 8 already owed 10 by team 1, default threshold 5 → over limit.
				assert.NoError(t, tx.Create(&db_models.OweLimitConfiguration{TeamID: 8, IsDefault: true, Threshold: 5}).Error)
				assert.NoError(t, tx.Create(&invoice_models.TeamBalance{
					TeamID: 1, ForTeamID: 8,
					BalanceType: invoice_iface.BalanceType_BALANCE_TYPE_PAYABLE, Balance: -10,
				}).Error)

				prod.EXPECT().ProductOrderInfo(gomock.Any(), gomock.Any()).Return(
					connect.NewResponse(&product_iface.ProductOrderInfoResponse{
						Items: map[uint64]*product_iface.ProductOrderInfoItem{
							2: {Name: "Cross", OwnerTeamId: 8, Owned: false, MarkupPercent: 20},
						},
					}), nil).AnyTimes()
				wh.EXPECT().GetWarehouseFee(gomock.Any(), gomock.Any()).Return(
					connect.NewResponse(&warehouse_iface.GetWarehouseFeeResponse{Fee: 1000}), nil).AnyTimes()
				// Gate blocks pre-tx → inventory is never touched.
				inv.EXPECT().TransactionCreate(gomock.Any(), gomock.Any()).Times(0)

				_, err := svc.OrderCreate(ctx, connect.NewRequest(&selling_iface.OrderCreateRequest{
					TeamId:      1,
					ShopId:      shop.ID,
					WarehouseId: 9,
					RefId:       "OWE-1",
					Items: []*selling_iface.OrderItemInput{
						{ProductId: 2, Count: 1, Price: 50},
					},
					Customer: &selling_iface.OrderCustomer{
						Name: "Budi", Phone: "0812", Province: "Jawa Barat",
						City: "Bandung", District: "Coblong", PostalCode: "40132",
						Address: "Jl. Dago 1",
					},
					ShippingFee:      20,
					EstRevenueAmount: 100,
				}))
				assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

				var count int64
				assert.NoError(t, tx.Model(&selling_models.Order{}).Count(&count).Error)
				assert.Equal(t, int64(0), count)
			})
		},
	)
}
