package revenue

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	common "github.com/pdcgo/schema/services/common/v1"
	selling_iface "github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/selling_models"
	"github.com/pdcgo/shared/db_connect"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

// WithdrawalLogList implements [selling_ifaceconnect.RevenueServiceHandler]. It returns
// the team's withdrawal change-log rows, most recent first, optionally scoped to one
// withdrawal and/or shop.
func (s *revenueServiceImpl) WithdrawalLogList(
	ctx context.Context,
	req *connect.Request[selling_iface.WithdrawalLogListRequest],
) (*connect.Response[selling_iface.WithdrawalLogListResponse], error) {
	pay := req.Msg
	if pay.GetPage() == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("page is required"))
	}

	result := &selling_iface.WithdrawalLogListResponse{
		Data:     []*selling_iface.WithdrawalLogItem{},
		PageInfo: &common.PageInfo{},
	}
	db := s.db.WithContext(ctx)

	var rows []*selling_models.WithdrawalLog
	paginated, pageInfo, err := db_connect.SetPaginationQuery(db, func() (*gorm.DB, error) {
		query := db.
			Model(&selling_models.WithdrawalLog{}).
			Scopes(func(d *gorm.DB) *gorm.DB {
				d = d.Where("team_id = ?", pay.GetTeamId())
				if pay.GetWithdrawalId() > 0 {
					d = d.Where("withdrawal_id = ?", pay.GetWithdrawalId())
				}
				if pay.GetShopId() > 0 {
					d = d.Where("shop_id = ?", pay.GetShopId())
				}
				return d
			})

		return query, nil
	}, pay.GetPage())
	if err != nil {
		return nil, err
	}

	err = paginated.
		Order("id DESC").
		Find(&rows).
		Error
	if err != nil {
		return nil, err
	}

	result.PageInfo = pageInfo
	for _, row := range rows {
		if row == nil {
			continue
		}
		result.Data = append(result.Data, &selling_iface.WithdrawalLogItem{
			Id:           row.ID,
			WithdrawalId: row.WithdrawalID,
			ShopId:       row.ShopID,
			ChangeType:   selling_iface.WithdrawalLogChangeType(row.ChangeType),
			Amount:       row.Amount,
			CreatedAt:    timestamppb.New(row.CreatedAt),
		})
	}

	return connect.NewResponse(result), nil
}
