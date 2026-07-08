package revenue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	selling_iface "github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/selling_models"
	"gorm.io/gorm"
)

// WithdrawalCreate implements [selling_ifaceconnect.RevenueServiceHandler]. It records
// a per-shop withdrawal and a CREATED WithdrawalLog in one transaction. For MANUAL
// source ref_id must be supplied; for IMPORTER it is computed from shop_id + the funded
// date (`<shop_id>/<year>/<month>/<day>`), one entry per shop per day.
func (s *revenueServiceImpl) WithdrawalCreate(
	ctx context.Context,
	req *connect.Request[selling_iface.WithdrawalCreateRequest],
) (*connect.Response[selling_iface.WithdrawalCreateResponse], error) {
	pay := req.Msg
	if pay.GetFundedAt() == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("funded_at is required"))
	}
	funded := pay.GetFundedAt().AsTime()

	refID, err := resolveRefID(pay.GetSource(), pay.GetRefId(), pay.GetShopId(), funded)
	if err != nil {
		return nil, err
	}

	resp := &selling_iface.WithdrawalCreateResponse{}
	now := time.Now()

	err = s.db.
		WithContext(ctx).
		Transaction(func(tx *gorm.DB) error {
			withdrawal := selling_models.Withdrawal{
				ShopID:    pay.GetShopId(),
				TeamID:    pay.GetTeamId(),
				Amount:    pay.GetAmount(),
				FundedAt:  funded,
				RefID:     refID,
				Source:    int32(pay.GetSource()),
				CreatedAt: now,
				UpdatedAt: now,
			}
			err := tx.Create(&withdrawal).Error
			if err != nil {
				return err
			}

			err = appendWithdrawalLog(tx, &withdrawal,
				selling_iface.WithdrawalLogChangeType_WITHDRAWAL_LOG_CHANGE_TYPE_CREATED,
				withdrawal.Amount, now)
			if err != nil {
				return err
			}

			resp.Id = withdrawal.ID
			return nil
		})
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(resp), nil
}

// resolveRefID returns the ref_id to store: the caller's (required) value for MANUAL,
// or the computed `<shop_id>/<year>/<month>/<day>` for IMPORTER.
func resolveRefID(source selling_iface.WithdrawalSource, refID string, shopID uint64, funded time.Time) (string, error) {
	if source == selling_iface.WithdrawalSource_WITHDRAWAL_SOURCE_IMPORTER {
		return fmt.Sprintf("%d/%d/%02d/%02d", shopID, funded.Year(), int(funded.Month()), funded.Day()), nil
	}
	if refID == "" {
		return "", connect.NewError(connect.CodeInvalidArgument, errors.New("ref_id is required for manual source"))
	}
	return refID, nil
}

// withdrawalEditWindow is how long after creation a withdrawal stays mutable (General
// rule: "can be edited if created_at > 7 days ago" — i.e. only within 7 days).
const withdrawalEditWindow = 7 * 24 * time.Hour

// requireWithdrawalMutable enforces the General rule: a withdrawal may be edited or
// deleted only within 7 days of creation; once it is 7+ days old it is locked.
func requireWithdrawalMutable(w *selling_models.Withdrawal) error {
	if time.Since(w.CreatedAt) >= withdrawalEditWindow {
		return connect.NewError(connect.CodeFailedPrecondition,
			errors.New("withdrawal can only be edited within 7 days of creation"))
	}
	return nil
}

// appendWithdrawalLog writes one change-history row for a withdrawal event, in the same
// transaction as the event itself (WithdrawalLogList reads these back).
func appendWithdrawalLog(
	tx *gorm.DB,
	withdrawal *selling_models.Withdrawal,
	changeType selling_iface.WithdrawalLogChangeType,
	amount float64,
	at time.Time,
) error {
	log := selling_models.WithdrawalLog{
		WithdrawalID: withdrawal.ID,
		ShopID:       withdrawal.ShopID,
		TeamID:       withdrawal.TeamID,
		ChangeType:   int32(changeType),
		Amount:       amount,
		CreatedAt:    at,
	}
	return tx.Create(&log).Error
}
