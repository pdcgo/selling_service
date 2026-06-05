package supplier_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/selling_models"
	"github.com/pdcgo/selling_service/supplier"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/pdcgo/shared/pkg/moretest/moretest_mock"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestSupplierReviewCreate(t *testing.T) {
	var scenario moretest_mock.DbScenario
	moretest.Suite(t, "supplier review create",
		moretest.SetupListFunc{moretest_mock.MockPostgresDatabase(&scenario)},
		func(t *testing.T) {
			scenario(t, func(tx *gorm.DB) {
				assert.NoError(t, tx.AutoMigrate(&selling_models.SupplierReview{}))
				svc := supplier.NewSupplierService(tx)

				_, err := svc.SupplierReviewCreate(context.Background(),
					connect.NewRequest(&selling_iface.SupplierReviewCreateRequest{
						TeamId:     1,
						SupplierId: 2,
						UserId:     3,
						Review:     "good",
						Rating:     5,
					}))
				assert.NoError(t, err)

				var row selling_models.SupplierReview
				assert.NoError(t, tx.First(&row).Error)
				assert.Equal(t, uint64(2), row.SupplierID)
				assert.Equal(t, uint64(1), row.TeamID)
				assert.Equal(t, uint64(3), row.UserID)
				assert.Equal(t, "good", row.Review)
				assert.Equal(t, uint64(5), row.Rating)
				assert.NotZero(t, row.ID)
				assert.False(t, row.CreatedAt.IsZero())
			})
		})
}

func TestSupplierReviewList(t *testing.T) {
	var scenario moretest_mock.DbScenario
	moretest.Suite(t, "supplier review list",
		moretest.SetupListFunc{moretest_mock.MockPostgresDatabase(&scenario)},
		func(t *testing.T) {
			scenario(t, func(tx *gorm.DB) {
				assert.NoError(t, tx.AutoMigrate(&selling_models.SupplierReview{}))
				svc := supplier.NewSupplierService(tx)
				ctx := context.Background()

				seed := func(r *selling_models.SupplierReview) *selling_models.SupplierReview {
					assert.NoError(t, tx.Create(r).Error)
					return r
				}
				r1 := seed(&selling_models.SupplierReview{SupplierID: 10, TeamID: 1, UserID: 100, Review: "great seller", Rating: 5})
				_ = seed(&selling_models.SupplierReview{SupplierID: 10, TeamID: 1, UserID: 101, Review: "bad packaging", Rating: 2})
				r3 := seed(&selling_models.SupplierReview{SupplierID: 20, TeamID: 2, UserID: 102, Review: "great price", Rating: 4})

				t.Run("page nil returns invalid argument", func(t *testing.T) {
					_, err := svc.SupplierReviewList(ctx,
						connect.NewRequest(&selling_iface.SupplierReviewListRequest{Page: nil}))
					assert.Error(t, err)
					assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
				})

				t.Run("list all ordered id desc", func(t *testing.T) {
					res, err := svc.SupplierReviewList(ctx,
						connect.NewRequest(&selling_iface.SupplierReviewListRequest{
							Page: &common.PageFilter{Page: 1, Limit: 10},
						}))
					assert.NoError(t, err)
					assert.Len(t, res.Msg.Data, 3)
					assert.Equal(t, int64(3), res.Msg.PageInfo.TotalItems)
					assert.Equal(t, r3.ID, res.Msg.Data[0].Id)
				})

				t.Run("filter supplier_id", func(t *testing.T) {
					res, err := svc.SupplierReviewList(ctx,
						connect.NewRequest(&selling_iface.SupplierReviewListRequest{
							Page:   &common.PageFilter{Page: 1, Limit: 10},
							Filter: &selling_iface.SupplierReviewListFilter{SupplierId: 10},
						}))
					assert.NoError(t, err)
					assert.Len(t, res.Msg.Data, 2)
					for _, item := range res.Msg.Data {
						assert.Equal(t, uint64(10), item.SupplierId)
					}
				})

				t.Run("filter team_id", func(t *testing.T) {
					res, err := svc.SupplierReviewList(ctx,
						connect.NewRequest(&selling_iface.SupplierReviewListRequest{
							Page:   &common.PageFilter{Page: 1, Limit: 10},
							Filter: &selling_iface.SupplierReviewListFilter{TeamId: 2},
						}))
					assert.NoError(t, err)
					assert.Len(t, res.Msg.Data, 1)
					assert.Equal(t, uint64(2), res.Msg.Data[0].TeamId)
				})

				t.Run("filter user_id", func(t *testing.T) {
					res, err := svc.SupplierReviewList(ctx,
						connect.NewRequest(&selling_iface.SupplierReviewListRequest{
							Page:   &common.PageFilter{Page: 1, Limit: 10},
							Filter: &selling_iface.SupplierReviewListFilter{UserId: 102},
						}))
					assert.NoError(t, err)
					assert.Len(t, res.Msg.Data, 1)
					assert.Equal(t, uint64(102), res.Msg.Data[0].UserId)
				})

				t.Run("filter q matches review text", func(t *testing.T) {
					res, err := svc.SupplierReviewList(ctx,
						connect.NewRequest(&selling_iface.SupplierReviewListRequest{
							Page:   &common.PageFilter{Page: 1, Limit: 10},
							Filter: &selling_iface.SupplierReviewListFilter{Q: "GREAT"},
						}))
					assert.NoError(t, err)
					assert.Len(t, res.Msg.Data, 2)
				})

				t.Run("pagination limits page size", func(t *testing.T) {
					res, err := svc.SupplierReviewList(ctx,
						connect.NewRequest(&selling_iface.SupplierReviewListRequest{
							Page: &common.PageFilter{Page: 1, Limit: 2},
						}))
					assert.NoError(t, err)
					assert.Len(t, res.Msg.Data, 2)
					assert.Equal(t, int64(3), res.Msg.PageInfo.TotalItems)
					assert.Equal(t, int64(2), res.Msg.PageInfo.TotalPage)
				})

				_ = r1
			})
		})
}

func TestSupplierReviewDelete(t *testing.T) {
	var scenario moretest_mock.DbScenario
	moretest.Suite(t, "supplier review delete",
		moretest.SetupListFunc{moretest_mock.MockPostgresDatabase(&scenario)},
		func(t *testing.T) {
			scenario(t, func(tx *gorm.DB) {
				assert.NoError(t, tx.AutoMigrate(&selling_models.SupplierReview{}))
				svc := supplier.NewSupplierService(tx)
				ctx := context.Background()

				r1 := &selling_models.SupplierReview{SupplierID: 1, TeamID: 1, UserID: 1, Review: "a", Rating: 5}
				r2 := &selling_models.SupplierReview{SupplierID: 1, TeamID: 2, UserID: 1, Review: "b", Rating: 5}
				assert.NoError(t, tx.Create(r1).Error)
				assert.NoError(t, tx.Create(r2).Error)

				t.Run("delete success", func(t *testing.T) {
					_, err := svc.SupplierReviewDelete(ctx,
						connect.NewRequest(&selling_iface.SupplierReviewDeleteRequest{Id: r1.ID, TeamId: 1}))
					assert.NoError(t, err)

					var count int64
					assert.NoError(t, tx.Model(&selling_models.SupplierReview{}).
						Where("id = ?", r1.ID).Count(&count).Error)
					assert.Equal(t, int64(0), count)
				})

				t.Run("wrong team keeps row", func(t *testing.T) {
					_, err := svc.SupplierReviewDelete(ctx,
						connect.NewRequest(&selling_iface.SupplierReviewDeleteRequest{Id: r2.ID, TeamId: 999}))
					assert.NoError(t, err)

					var count int64
					assert.NoError(t, tx.Model(&selling_models.SupplierReview{}).
						Where("id = ?", r2.ID).Count(&count).Error)
					assert.Equal(t, int64(1), count)
				})
			})
		})
}
