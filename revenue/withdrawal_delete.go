package revenue

import (
	"context"
	"errors"
	"strconv"
	"time"

	"connectrpc.com/connect"
	selling_iface "github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/selling_models"
	"gorm.io/gorm"
)

// WithdrawalDelete implements [selling_ifaceconnect.RevenueServiceHandler]. Soft delete:
// ref_id is renamed (`[ref_id]_deleted_[unix_second]`) so a same-ref withdrawal can be
// re-created without colliding on the (team_id, ref_id, source) unique index, and a
// DELETED WithdrawalLog is recorded — all in one transaction. Scoped to team_id.
func (s *revenueServiceImpl) WithdrawalDelete(
	ctx context.Context,
	req *connect.Request[selling_iface.WithdrawalDeleteRequest],
) (*connect.Response[selling_iface.WithdrawalDeleteResponse], error) {
	pay := req.Msg
	now := time.Now()
	ts := strconv.FormatInt(now.Unix(), 10)

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
					"ref_id":     gorm.Expr("ref_id || ?", "_deleted_"+ts),
					"deleted_at": now,
					"updated_at": now,
				}).
				Error
			if err != nil {
				return err
			}

			return appendWithdrawalLog(tx, &withdrawal,
				selling_iface.WithdrawalLogChangeType_WITHDRAWAL_LOG_CHANGE_TYPE_DELETED,
				withdrawal.Amount, now)
		})
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&selling_iface.WithdrawalDeleteResponse{}), nil
}
