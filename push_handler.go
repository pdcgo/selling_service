package selling_service

import (
	"context"
	"fmt"
	"net/http"

	"buf.build/go/protovalidate"
	"github.com/pdcgo/event_source"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/selling_models"
	"github.com/pdcgo/shared/pkg/common_helper"
	"google.golang.org/protobuf/encoding/protojson"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SellingPushHandler event_source.PushHandler

func NewSellingPushHandler(db *gorm.DB) SellingPushHandler {
	return func(ctx context.Context, msg *event_source.PushRequest) error {
		var err error

		var event selling_iface.SellingEvent
		err = protojson.Unmarshal(msg.Message.Data, &event)
		if err != nil {
			return err
		}

		// validating message
		err = protovalidate.GlobalValidator.Validate(&event)
		if err != nil {
			return err
		}

		return db.Transaction(func(tx *gorm.DB) error {
			handler := common_helper.NewChainParam(
				func(next common_helper.NextFuncParam[*selling_iface.SellingEvent]) common_helper.NextFuncParam[*selling_iface.SellingEvent] {
					return func(event *selling_iface.SellingEvent) (*selling_iface.SellingEvent, error) { // untuk seeding product cross
						var err error
						switch eventData := event.Data.(type) {
						case *selling_iface.SellingEvent_OrderCreated:
							crossProduct := selling_models.TeamCrossProduct{}
							err = tx.
								Table("order_items oi").
								Joins("left join orders o on o.id = oi.order_id").
								Where("oi.order_id = ?", eventData.OrderCreated.OrderId).
								Where("oi.owned = ?", false).
								Select([]string{
									"o.team_id",
									"oi.product_id",
									"o.order_mp_id as shop_id",
									"o.created_by_id as user_id",
								}).
								Find(&crossProduct).
								Error

							if err != nil {
								return nil, err
							}

							if crossProduct.ProductId == 0 || crossProduct.ShopId == 0 {
								return next(event)
							}

							err = tx.
								Clauses(clause.OnConflict{
									Columns: []clause.Column{
										{Name: "team_id"},
										{Name: "product_id"},
										{Name: "shop_id"},
										{Name: "user_id"},
									},
									DoNothing: true,
								}).
								Create(&crossProduct).
								Error

							if err != nil {
								return nil, err
							}

						}

						return next(event)
					}
				},
				func(next common_helper.NextFuncParam[*selling_iface.SellingEvent]) common_helper.NextFuncParam[*selling_iface.SellingEvent] {
					return func(event *selling_iface.SellingEvent) (*selling_iface.SellingEvent, error) { // seeding koneksi toko ke gudang
						var err error

						switch eventData := event.Data.(type) {
						case *selling_iface.SellingEvent_OrderCreated:

							shopWarehouse := selling_models.ShopWarehouse{}
							err = tx.
								Table("orders o").
								Joins("left join inv_transactions it on it.id = o.invertory_tx_id").
								Where("o.id = ?", eventData.OrderCreated.OrderId).
								Select([]string{
									"o.order_mp_id as shop_id",
									"it.warehouse_id as warehouse_id",
									"o.created_by_id as user_id",
									"it.created as last_order_at",
								}).
								Find(&shopWarehouse).
								Error

							if err != nil {
								return nil, err
							}

							if shopWarehouse.ShopId == 0 || shopWarehouse.WarehouseId == 0 {
								return event, fmt.Errorf("event order created %d have shopid and warehouseid 0", eventData.OrderCreated.OrderId)
							}

							err = tx.
								Clauses(clause.OnConflict{
									Columns: []clause.Column{
										{Name: "shop_id"},
										{Name: "warehouse_id"},
										{Name: "user_id"},
									},
									DoUpdates: clause.AssignmentColumns([]string{"last_order_at"}),
								}).
								Create(&shopWarehouse).
								Error

							if err != nil {
								return nil, err
							}

						}

						return next(event)
					}
				},
			)

			_, err = handler(&event)
			if err != nil {
				return err
			}

			return nil
		})

	}
}

type SellingPushHttpHandler http.HandlerFunc

func NewSellingPushHttpHandler(handler SellingPushHandler) SellingPushHttpHandler {
	return SellingPushHttpHandler(event_source.NewMuxPushhandler(event_source.PushHandler(handler)))
}

// func NewSeedSupplierOrderLog(db *gorm.DB) common_helper.NextHandlerParam[*selling_iface.SellingEvent] {
// 	return func(next common_helper.NextFuncParam[*selling_iface.SellingEvent]) common_helper.NextFuncParam[*selling_iface.SellingEvent] {
// 		return func(event *selling_iface.SellingEvent) (*selling_iface.SellingEvent, error) {
// 			var err error
// 			switch eventData := event.Data.(type) {
// 			case *selling_iface.SellingEvent_OrderCreated:

// 			}

// 		}
// 	}
// }
