package selling_service_test

import (
	"testing"
	"time"

	"github.com/pdcgo/event_source/event_source_mock"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service"
	"github.com/pdcgo/selling_service/selling_models"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/pdcgo/shared/pkg/moretest/moretest_mock"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestOnOrderCreated(t *testing.T) {
	var scenario moretest_mock.DbScenario
	moretest.Suite(
		t,
		"testing on order",
		moretest.SetupListFunc{
			moretest_mock.MockPostgresDatabase(&scenario),
		},
		func(t *testing.T) {
			scenario(t, func(db *gorm.DB) {
				var err error

				err = db.AutoMigrate(
					&selling_models.ShopWarehouse{},
					&selling_models.TeamCrossProduct{},
					&db_models.OrderItem{},
					&db_models.Order{},
				)
				assert.NoError(t, err)

				txs := db_models.InvTransaction{
					ID:          1,
					WarehouseID: 3,
					Created:     time.Now(),
				}
				err = db.Create(&txs).Error
				assert.NoError(t, err)

				var txId uint = 1
				orders := []db_models.Order{
					{
						ID:            1,
						OrderMpID:     1,
						InvertoryTxID: &txId,
						Items: []*db_models.OrderItem{
							{
								OrderID:   1,
								ProductID: 1,
								Owned:     false,
							},
						},
					},
				}
				err = db.Create(&orders).Error
				assert.NoError(t, err)

				handler := selling_service.NewSellingPushHandler(db)

				msg := event_source_mock.NewMockEvent(t, &selling_iface.SellingEvent{
					Data: &selling_iface.SellingEvent_OrderCreated{
						OrderCreated: &selling_iface.OrderCreated{
							OrderId: 1,
						},
					},
				})

				err = handler(t.Context(), msg)
				assert.NoError(t, err)

			})
		},
	)

}
