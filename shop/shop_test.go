package shop_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/shop"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/pdcgo/shared/pkg/moretest/moretest_mock"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

// Authorization is enforced by the access interceptor (proto request_policy), not in
// the handlers — these tests call the impl directly to validate CRUD/DB behavior.

func migrateShop(t *testing.T, tx *gorm.DB) {
	assert.NoError(t, tx.AutoMigrate(&db_models.V2Shop{}, &db_models.V2ShopUser{}))
}

func TestShopCrud(t *testing.T) {
	var scenario moretest_mock.DbScenario
	moretest.Suite(t, "shop crud",
		moretest.SetupListFunc{moretest_mock.MockPostgresDatabase(&scenario)},
		func(t *testing.T) {
			scenario(t, func(tx *gorm.DB) {
				migrateShop(t, tx)
				svc := shop.NewShopService(tx)
				ctx := context.Background()

				// Create
				created, err := svc.ShopCreate(ctx, connect.NewRequest(&selling_iface.ShopCreateRequest{
					TeamId:          1,
					ShopName:        "Toko Berkah",
					ShopUsername:    "berkah_id",
					MarketplaceType: common.MarketplaceType_MARKETPLACE_TYPE_SHOPEE,
					Uri:             "https://shopee.co.id/berkah_id",
				}))
				assert.NoError(t, err)
				assert.NotZero(t, created.Msg.Id)

				// Another team's shop (should not appear in team 1 list)
				_, err = svc.ShopCreate(ctx, connect.NewRequest(&selling_iface.ShopCreateRequest{
					TeamId:       2,
					ShopName:     "Other Team Shop",
					ShopUsername: "other",
				}))
				assert.NoError(t, err)

				// List team 1
				listed, err := svc.ShopList(ctx, connect.NewRequest(&selling_iface.ShopListRequest{
					TeamId: 1,
					Page:   &common.PageFilter{Page: 1, Limit: 10},
				}))
				assert.NoError(t, err)
				assert.Len(t, listed.Msg.Data, 1)
				assert.Equal(t, "berkah_id", listed.Msg.Data[0].ShopUsername)
				assert.Equal(t, common.MarketplaceType_MARKETPLACE_TYPE_SHOPEE, listed.Msg.Data[0].MarketplaceType)

				// Search filter
				searched, err := svc.ShopList(ctx, connect.NewRequest(&selling_iface.ShopListRequest{
					TeamId: 1,
					Q:      "BERKAH",
					Page:   &common.PageFilter{Page: 1, Limit: 10},
				}))
				assert.NoError(t, err)
				assert.Len(t, searched.Msg.Data, 1)

				// Update
				_, err = svc.ShopUpdate(ctx, connect.NewRequest(&selling_iface.ShopUpdateRequest{
					Id:              created.Msg.Id,
					TeamId:          1,
					ShopName:        "Toko Berkah Jaya",
					ShopUsername:    "berkah_id",
					MarketplaceType: common.MarketplaceType_MARKETPLACE_TYPE_TOKOPEDIA,
					Uri:             "https://tokopedia.com/berkah",
				}))
				assert.NoError(t, err)

				var updated db_models.V2Shop
				assert.NoError(t, tx.First(&updated, created.Msg.Id).Error)
				assert.Equal(t, "Toko Berkah Jaya", updated.ShopName)
				assert.Equal(t, int32(common.MarketplaceType_MARKETPLACE_TYPE_TOKOPEDIA), updated.MarketplaceType)

				// Delete (soft delete + username rename)
				_, err = svc.ShopDelete(ctx, connect.NewRequest(&selling_iface.ShopDeleteRequest{
					Id:     created.Msg.Id,
					TeamId: 1,
				}))
				assert.NoError(t, err)

				afterDelete, err := svc.ShopList(ctx, connect.NewRequest(&selling_iface.ShopListRequest{
					TeamId: 1,
					Page:   &common.PageFilter{Page: 1, Limit: 10},
				}))
				assert.NoError(t, err)
				assert.Len(t, afterDelete.Msg.Data, 0)

				// The renamed, soft-deleted row frees the username → re-create succeeds.
				recreated, err := svc.ShopCreate(ctx, connect.NewRequest(&selling_iface.ShopCreateRequest{
					TeamId:       1,
					ShopName:     "Berkah Reopened",
					ShopUsername: "berkah_id",
				}))
				assert.NoError(t, err)
				assert.NotZero(t, recreated.Msg.Id)
				assert.NotEqual(t, created.Msg.Id, recreated.Msg.Id)
			})
		})
}

func TestShopUserUpdate(t *testing.T) {
	var scenario moretest_mock.DbScenario
	moretest.Suite(t, "shop user update",
		moretest.SetupListFunc{moretest_mock.MockPostgresDatabase(&scenario)},
		func(t *testing.T) {
			scenario(t, func(tx *gorm.DB) {
				migrateShop(t, tx)
				svc := shop.NewShopService(tx)
				ctx := context.Background()

				created, err := svc.ShopCreate(ctx, connect.NewRequest(&selling_iface.ShopCreateRequest{
					TeamId:       1,
					ShopName:     "Shop A",
					ShopUsername: "shop_a",
				}))
				assert.NoError(t, err)
				shopID := created.Msg.Id

				// Add two users
				_, err = svc.ShopUserUpdate(ctx, connect.NewRequest(&selling_iface.ShopUserUpdateRequest{
					ShopId:       shopID,
					TeamId:       1,
					AddedUserIds: []uint64{10, 20},
				}))
				assert.NoError(t, err)

				listed, err := svc.ShopUserList(ctx, connect.NewRequest(&selling_iface.ShopUserListRequest{
					ShopId: shopID,
					TeamId: 1,
				}))
				assert.NoError(t, err)
				assert.Len(t, listed.Msg.Data, 2)

				// Remove one, add another (and re-add an existing one is idempotent)
				_, err = svc.ShopUserUpdate(ctx, connect.NewRequest(&selling_iface.ShopUserUpdateRequest{
					ShopId:         shopID,
					TeamId:         1,
					AddedUserIds:   []uint64{20, 30},
					RemovedUserIds: []uint64{10},
				}))
				assert.NoError(t, err)

				listed2, err := svc.ShopUserList(ctx, connect.NewRequest(&selling_iface.ShopUserListRequest{
					ShopId: shopID,
					TeamId: 1,
				}))
				assert.NoError(t, err)
				got := map[uint64]bool{}
				for _, u := range listed2.Msg.Data {
					got[u.UserId] = true
				}
				assert.Equal(t, map[uint64]bool{20: true, 30: true}, got)

				// Re-add a previously removed user reactivates the row (no duplicate).
				_, err = svc.ShopUserUpdate(ctx, connect.NewRequest(&selling_iface.ShopUserUpdateRequest{
					ShopId:       shopID,
					TeamId:       1,
					AddedUserIds: []uint64{10},
				}))
				assert.NoError(t, err)

				var activeCount int64
				assert.NoError(t, tx.Model(&db_models.V2ShopUser{}).
					Where("shop_id = ? AND user_id = ?", shopID, uint64(10)).
					Count(&activeCount).Error)
				assert.Equal(t, int64(1), activeCount)

				// Wrong team is rejected.
				_, err = svc.ShopUserList(ctx, connect.NewRequest(&selling_iface.ShopUserListRequest{
					ShopId: shopID,
					TeamId: 999,
				}))
				assert.Error(t, err)
				assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
			})
		})
}
