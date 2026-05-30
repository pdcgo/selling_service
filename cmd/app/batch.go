package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"buf.build/go/protovalidate"
	"cloud.google.com/go/pubsub/v2"
	"github.com/pdcgo/san_collection/san_pubsub"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/stat_logs/v1"
	"github.com/pdcgo/selling_service/selling_models"
	"github.com/pdcgo/shared/custom_connect"
	"github.com/pdcgo/shared/db_models"
	"github.com/urfave/cli/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"gorm.io/gorm"
)

type BatchConfig struct {
	SubName string
}

func NewBatchConfig() *BatchConfig {
	subName := os.Getenv("BATCH_SUB_NAME")
	if subName == "" {
		subName = "tmp-sub"
	}

	return &BatchConfig{
		SubName: subName,
	}
}

type BatchFunc cli.ActionFunc

func NewBatchFunc(
	cfg *BatchConfig,
	batchHandler BatchHandler,
) BatchFunc {
	return func(ctx context.Context, c *cli.Command) error {
		canceltrace, err := custom_connect.InitTracer("selling-service")
		if err != nil {
			return err
		}

		defer canceltrace(ctx)

		client, err := san_pubsub.NewPubSubClientWithContext(ctx)
		if err != nil {
			panic(err)
		}

		subscribe := client.Subscriber(cfg.SubName)
		slog.Info("listening", "subname", cfg.SubName)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		iddleDuration := time.Minute
		timeout := time.AfterFunc(iddleDuration, cancel)

		err = subscribe.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			timeout.Reset(iddleDuration)

			ctx, span := otel.Tracer("").Start(ctx, "event/receive")
			defer span.End()

			err := batchHandler(ctx, m)
			if err != nil {
				slog.Error("error processing event", "err", err.Error())
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				m.Nack()
				return
			}
			m.Ack()
		})

		if err != nil {
			return err
		}

		slog.Info("stopped", "subname", cfg.SubName)

		return err
	}
}

type BatchHandler func(ctx context.Context, m *pubsub.Message) error

func NewBatchHandler(

	db *gorm.DB,

) BatchHandler {
	return func(ctx context.Context, m *pubsub.Message) error {
		event, err := decodeMessage(m)
		if err != nil {
			return err
		}

		switch data := event.Data.(type) {
		case *selling_iface.SellingEvent_OrderCreated:
			var ord db_models.Order
			err = db.
				Model(&ord).
				Where("id = ?", data.OrderCreated.OrderId).
				Select([]string{
					"invertory_tx_id",
					"id",
					"team_id",
					"created_at",
				}).
				Find(&ord).
				Error

			if err != nil {
				return err
			}
			txID := *ord.InvertoryTxID

			if txID == 0 {
				return nil
			}

			// select
			// 	ih.sku_id,
			// 	s.product_id,
			// 	s.team_id as team_product_id,
			// 	s.warehouse_id,
			// 	ih.in_tx_id,
			// 	ih.count,
			// 	(ih.price + coalesce(ih.ext_price, 0) * ih.count) as amount

			// from invertory_histories ih
			// left join skus s on s.id = ih.sku_id

			// where ih.tx_id = 1637354

			// getting history log
			historyLog := []*InvertoryLog{}

			err = db.
				Table("invertory_histories ih").
				Select([]string{
					"ih.sku_id",
					"s.product_id",
					"s.team_id as team_product_id",
					"s.warehouse_id",
					"ih.in_tx_id",
					"ih.count",
					"(ih.price + coalesce(ih.ext_price, 0) * ih.count) as amount",
				}).
				Joins("left join skus s on s.id = ih.sku_id").
				Where("ih.tx_id = ?", txID).
				Find(&historyLog).
				Error

			if err != nil {
				return err
			}

			// getting supplier log
			for _, log := range historyLog {
				if log.SkuID == "" {
					continue
				}

				if log.InTxID == 0 {
					continue
				}

				// select
				// 	iti.sku_id,
				// 	vsm.supplier_id,
				// 	siti.supplier_id as child_supplier_id,
				// 	vsm.mp_type

				// from v2_supplier_inv_tx_items siti
				// left join inv_tx_items iti on iti.id = siti.inv_tx_item_id
				// left join v2_supplier_marketplaces vsm on vsm.id = siti.supplier_id

				// where
				// 	iti.inv_transaction_id = 1627932
				// 	and iti.sku_id = '2244433632973620'
				supplierLog := SupplierLog{}

				err = db.
					Table("v2_supplier_inv_tx_items siti").
					Select([]string{
						"iti.sku_id",
						"vsm.supplier_id",
						"siti.supplier_id as child_supplier_id",
						"vsm.mp_type",
					}).
					Joins("left join inv_tx_items iti on iti.id = siti.inv_tx_item_id").
					Joins("left join v2_supplier_marketplaces vsm on vsm.id = siti.supplier_id").
					Where("iti.inv_transaction_id = ?", log.InTxID).
					Where("iti.sku_id = ?", log.SkuID).
					Find(&supplierLog).
					Error

				if err != nil {
					return err
				}

				if supplierLog.SupplierID == 0 {
					continue
				}

				// writing to database
				supplierLogModel := &selling_models.SupplierOrderLog{
					LogType:    stat_logs.SupplierLogType_SUPPLIER_LOG_TYPE_ORDER,
					SupplierID: supplierLog.SupplierID,
					ProductID:  log.ProductID,
					TeamID:     log.TeamProductID,
					Count:      int64(log.Count),
					Amount:     log.Amount,
					EventAt:    ord.CreatedAt,
					OrderID:    data.OrderCreated.OrderId,
				}

				err = db.Create(supplierLogModel).Error

				if err != nil {
					return err
				}

			}

			slog.Info("processed stat supplier", "order_id", data.OrderCreated.OrderId)

		}

		return nil
	}
}

func decodeMessage(msg *pubsub.Message) (*selling_iface.SellingEvent, error) {
	var event selling_iface.SellingEvent
	err := protojson.Unmarshal(msg.Data, &event)
	if err != nil {
		return &event, err
	}

	// validating message
	err = protovalidate.GlobalValidator.Validate(&event)
	if err != nil {
		return &event, err
	}

	return &event, err
}

type InvertoryLog struct {
	SkuID         db_models.SkuID
	ProductID     uint64
	TeamProductID uint64
	WarehouseID   uint64
	InTxID        uint64
	Count         uint64
	Amount        float64
}

type SupplierLog struct {
	SkuID           db_models.SkuID
	SupplierID      uint64
	ChildSupplierID uint64
	MpType          int
}
