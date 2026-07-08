package revenue

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"
	selling_iface "github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/selling_models"
	"gorm.io/gorm"
)

// WithdrawalUpdate implements [selling_ifaceconnect.RevenueServiceHandler]. It changes
// the withdrawal's amount (and funded date) and records a CHANGE_AMOUNT WithdrawalLog in
// one transaction. Scoped to team_id; a missing/foreign withdrawal is NotFound.
func (s *revenueServiceImpl) WithdrawalUpdate(
	ctx context.Context,
	req *connect.Request[selling_iface.WithdrawalUpdateRequest],
) (*connect.Response[selling_iface.WithdrawalUpdateResponse], error) {
	pay := req.Msg
	if pay.GetFundedAt() == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("funded_at is required"))
	}
	funded := pay.GetFundedAt().AsTime()
	now := time.Now()

	err := s.db.
		WithContext(ctx).
		Transaction(func(tx *gorm.DB) error {
			var withdrawal selling_models.Withdrawal
			err := tx.
				Where("id = ? AND team_id = ?", pay.GetId(), pay.GetTeamId()).
				First(&withdrawal).
				Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return connect.NewError(connect.CodeNotFound, errors.New("withdrawal not found"))
				}
				return err
			}

			err = requireWithdrawalMutable(&withdrawal)
			if err != nil {
				return err
			}

			err = tx.
				Model(&selling_models.Withdrawal{}).
				Where("id = ?", withdrawal.ID).
				Updates(map[string]interface{}{
					"amount":     pay.GetAmount(),
					"funded_at":  funded,
					"updated_at": now,
				}).
				Error
			if err != nil {
				return err
			}

			return appendWithdrawalLog(tx, &withdrawal,
				selling_iface.WithdrawalLogChangeType_WITHDRAWAL_LOG_CHANGE_TYPE_CHANGE_AMOUNT,
				pay.GetAmount(), now)
		})
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&selling_iface.WithdrawalUpdateResponse{}), nil
}
