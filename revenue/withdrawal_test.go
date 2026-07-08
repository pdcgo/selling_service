package revenue_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	common "github.com/pdcgo/schema/services/common/v1"
	selling_iface "github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/revenue"
	"github.com/pdcgo/selling_service/selling_models"
	"github.com/pdcgo/shared/pkg/moretest"
	"github.com/pdcgo/shared/pkg/moretest/moretest_mock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

// Authorization is enforced by the access interceptor (proto request_policy), not in the
// handlers — these tests call the impl directly to validate the CRUD/ledger behavior.

func TestWithdrawalCrud(t *testing.T) {
	var scenario moretest_mock.DbScenario
	moretest.Suite(t, "withdrawal crud",
		moretest.SetupListFunc{moretest_mock.MockPostgresDatabase(&scenario)},
		func(t *testing.T) {
			scenario(t, func(tx *gorm.DB) {
				assert.NoError(t, tx.AutoMigrate(&selling_models.Withdrawal{}, &selling_models.WithdrawalLog{}))
				svc := revenue.NewRevenueService(tx)
				ctx := context.Background()

				funded := time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC)

				// Create manual (ref kept)
				created, err := svc.WithdrawalCreate(ctx, connect.NewRequest(&selling_iface.WithdrawalCreateRequest{
					TeamId:   1,
					ShopId:   7,
					Amount:   150000,
					FundedAt: timestamppb.New(funded),
					RefId:    "INV-001",
					Source:   selling_iface.WithdrawalSource_WITHDRAWAL_SOURCE_MANUAL,
				}))
				assert.NoError(t, err)
				assert.NotZero(t, created.Msg.Id)

				// Manual without ref_id is rejected.
				_, err = svc.WithdrawalCreate(ctx, connect.NewRequest(&selling_iface.WithdrawalCreateRequest{
					TeamId: 1, ShopId: 7, Amount: 10, FundedAt: timestamppb.New(funded),
					Source: selling_iface.WithdrawalSource_WITHDRAWAL_SOURCE_MANUAL,
				}))
				assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

				// Create importer (ref computed = shop/y/m/d)
				imported, err := svc.WithdrawalCreate(ctx, connect.NewRequest(&selling_iface.WithdrawalCreateRequest{
					TeamId:   1,
					ShopId:   7,
					Amount:   99000,
					FundedAt: timestamppb.New(funded),
					RefId:    "ignored",
					Source:   selling_iface.WithdrawalSource_WITHDRAWAL_SOURCE_IMPORTER,
				}))
				assert.NoError(t, err)
				var importedRow selling_models.Withdrawal
				assert.NoError(t, tx.First(&importedRow, imported.Msg.Id).Error)
				assert.Equal(t, "7/2026/03/05", importedRow.RefID)

				// Another team's withdrawal (must not appear in team 1's list)
				_, err = svc.WithdrawalCreate(ctx, connect.NewRequest(&selling_iface.WithdrawalCreateRequest{
					TeamId: 2, ShopId: 9, Amount: 5, FundedAt: timestamppb.New(funded), RefId: "OTHER",
					Source: selling_iface.WithdrawalSource_WITHDRAWAL_SOURCE_MANUAL,
				}))
				assert.NoError(t, err)

				// List team 1 → 2 rows, newest first
				listed, err := svc.WithdrawalList(ctx, connect.NewRequest(&selling_iface.WithdrawalListRequest{
					TeamId: 1,
					Page:   &common.PageFilter{Page: 1, Limit: 10},
				}))
				assert.NoError(t, err)
				assert.Len(t, listed.Msg.Data, 2)
				assert.Equal(t, int64(2), listed.Msg.PageInfo.TotalItems)

				// Source filter
				manualOnly, err := svc.WithdrawalList(ctx, connect.NewRequest(&selling_iface.WithdrawalListRequest{
					TeamId: 1,
					Source: selling_iface.WithdrawalSource_WITHDRAWAL_SOURCE_MANUAL,
					Page:   &common.PageFilter{Page: 1, Limit: 10},
				}))
				assert.NoError(t, err)
				assert.Len(t, manualOnly.Msg.Data, 1)
				assert.Equal(t, "INV-001", manualOnly.Msg.Data[0].RefId)

				// Update amount → change_amount log
				_, err = svc.WithdrawalUpdate(ctx, connect.NewRequest(&selling_iface.WithdrawalUpdateRequest{
					Id:       created.Msg.Id,
					TeamId:   1,
					Amount:   175000,
					FundedAt: timestamppb.New(funded),
				}))
				assert.NoError(t, err)
				var afterUpdate selling_models.Withdrawal
				assert.NoError(t, tx.First(&afterUpdate, created.Msg.Id).Error)
				assert.Equal(t, float64(175000), afterUpdate.Amount)

				// Wrong team update is NotFound
				_, err = svc.WithdrawalUpdate(ctx, connect.NewRequest(&selling_iface.WithdrawalUpdateRequest{
					Id: created.Msg.Id, TeamId: 999, Amount: 1, FundedAt: timestamppb.New(funded),
				}))
				assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

				// Delete (soft delete + ref rename)
				_, err = svc.WithdrawalDelete(ctx, connect.NewRequest(&selling_iface.WithdrawalDeleteRequest{
					Id:     created.Msg.Id,
					TeamId: 1,
				}))
				assert.NoError(t, err)

				afterDelete, err := svc.WithdrawalList(ctx, connect.NewRequest(&selling_iface.WithdrawalListRequest{
					TeamId: 1,
					Page:   &common.PageFilter{Page: 1, Limit: 10},
				}))
				assert.NoError(t, err)
				assert.Len(t, afterDelete.Msg.Data, 1) // only the importer row remains

				// The renamed, soft-deleted row frees the ref → re-create succeeds.
				recreated, err := svc.WithdrawalCreate(ctx, connect.NewRequest(&selling_iface.WithdrawalCreateRequest{
					TeamId:   1,
					ShopId:   7,
					Amount:   200000,
					FundedAt: timestamppb.New(funded),
					RefId:    "INV-001",
					Source:   selling_iface.WithdrawalSource_WITHDRAWAL_SOURCE_MANUAL,
				}))
				assert.NoError(t, err)
				assert.NotEqual(t, created.Msg.Id, recreated.Msg.Id)

				// Change-log of the first withdrawal: created → change_amount → deleted
				logs, err := svc.WithdrawalLogList(ctx, connect.NewRequest(&selling_iface.WithdrawalLogListRequest{
					TeamId:       1,
					WithdrawalId: created.Msg.Id,
					Page:         &common.PageFilter{Page: 1, Limit: 10},
				}))
				assert.NoError(t, err)
				assert.Len(t, logs.Msg.Data, 3)
				// id DESC → deleted first, created last
				assert.Equal(t, selling_iface.WithdrawalLogChangeType_WITHDRAWAL_LOG_CHANGE_TYPE_DELETED, logs.Msg.Data[0].ChangeType)
				assert.Equal(t, selling_iface.WithdrawalLogChangeType_WITHDRAWAL_LOG_CHANGE_TYPE_CHANGE_AMOUNT, logs.Msg.Data[1].ChangeType)
				assert.InDelta(t, 175000, logs.Msg.Data[1].Amount, 0.001)
				assert.Equal(t, selling_iface.WithdrawalLogChangeType_WITHDRAWAL_LOG_CHANGE_TYPE_CREATED, logs.Msg.Data[2].ChangeType)

				// Log-list is team-scoped.
				otherLogs, err := svc.WithdrawalLogList(ctx, connect.NewRequest(&selling_iface.WithdrawalLogListRequest{
					TeamId: 999,
					Page:   &common.PageFilter{Page: 1, Limit: 10},
				}))
				assert.NoError(t, err)
				assert.Len(t, otherLogs.Msg.Data, 0)
			})
		})
}

// General rule: a withdrawal may be edited/deleted only within 7 days of creation.
func TestWithdrawalEditWindow(t *testing.T) {
	var scenario moretest_mock.DbScenario
	moretest.Suite(t, "withdrawal edit window",
		moretest.SetupListFunc{moretest_mock.MockPostgresDatabase(&scenario)},
		func(t *testing.T) {
			scenario(t, func(tx *gorm.DB) {
				assert.NoError(t, tx.AutoMigrate(&selling_models.Withdrawal{}, &selling_models.WithdrawalLog{}))
				svc := revenue.NewRevenueService(tx)
				ctx := context.Background()

				funded := time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC)
				create := func(ref string) uint64 {
					res, err := svc.WithdrawalCreate(ctx, connect.NewRequest(&selling_iface.WithdrawalCreateRequest{
						TeamId: 1, ShopId: 7, Amount: 100, FundedAt: timestamppb.New(funded), RefId: ref,
						Source: selling_iface.WithdrawalSource_WITHDRAWAL_SOURCE_MANUAL,
					}))
					assert.NoError(t, err)
					return res.Msg.Id
				}

				// A recent withdrawal is still mutable (update + delete OK).
				recent := create("RECENT")
				_, err := svc.WithdrawalUpdate(ctx, connect.NewRequest(&selling_iface.WithdrawalUpdateRequest{
					Id: recent, TeamId: 1, Amount: 120, FundedAt: timestamppb.New(funded),
				}))
				assert.NoError(t, err)
				_, err = svc.WithdrawalDelete(ctx, connect.NewRequest(&selling_iface.WithdrawalDeleteRequest{
					Id: recent, TeamId: 1,
				}))
				assert.NoError(t, err)

				// An 8-day-old withdrawal is locked: update and delete both rejected.
				old := create("OLD")
				assert.NoError(t, tx.
					Model(&selling_models.Withdrawal{}).
					Where("id = ?", old).
					Update("created_at", time.Now().Add(-8*24*time.Hour)).
					Error)

				_, err = svc.WithdrawalUpdate(ctx, connect.NewRequest(&selling_iface.WithdrawalUpdateRequest{
					Id: old, TeamId: 1, Amount: 999, FundedAt: timestamppb.New(funded),
				}))
				assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

				_, err = svc.WithdrawalDelete(ctx, connect.NewRequest(&selling_iface.WithdrawalDeleteRequest{
					Id: old, TeamId: 1,
				}))
				assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

				// The locked row is untouched: still present, not soft-deleted, amount unchanged.
				var oldRow selling_models.Withdrawal
				assert.NoError(t, tx.First(&oldRow, old).Error)
				assert.False(t, oldRow.DeletedAt.Valid)
				assert.Equal(t, float64(100), oldRow.Amount)
				assert.Equal(t, "OLD", oldRow.RefID)
			})
		})
}
