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

// WithdrawalList implements [selling_ifaceconnect.RevenueServiceHandler]. It returns the
// team's withdrawals (soft-deleted rows excluded by gorm), most recent first, optionally
// filtered by shop_id and/or source.
func (s *revenueServiceImpl) WithdrawalList(
	ctx context.Context,
	req *connect.Request[selling_iface.WithdrawalListRequest],
) (*connect.Response[selling_iface.WithdrawalListResponse], error) {
	pay := req.Msg
	if pay.GetPage() == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("page is required"))
	}

	result := &selling_iface.WithdrawalListResponse{
		Data:     []*selling_iface.WithdrawalItem{},
		PageInfo: &common.PageInfo{},
	}
	db := s.db.WithContext(ctx)

	var rows []*selling_models.Withdrawal
	paginated, pageInfo, err := db_connect.SetPaginationQuery(db, func() (*gorm.DB, error) {
		query := db.
			Model(&selling_models.Withdrawal{}).
			Scopes(func(d *gorm.DB) *gorm.DB {
				d = d.Where("team_id = ?", pay.GetTeamId())
				if pay.GetShopId() > 0 {
					d = d.Where("shop_id = ?", pay.GetShopId())
				}
				if pay.GetSource() != selling_iface.WithdrawalSource_WITHDRAWAL_SOURCE_UNSPECIFIED {
					d = d.Where("source = ?", int32(pay.GetSource()))
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
		result.Data = append(result.Data, withdrawalToProto(row))
	}

	return connect.NewResponse(result), nil
}

// withdrawalToProto maps a Withdrawal model to its proto item.
func withdrawalToProto(row *selling_models.Withdrawal) *selling_iface.WithdrawalItem {
	item := &selling_iface.WithdrawalItem{
		Id:        row.ID,
		TeamId:    row.TeamID,
		ShopId:    row.ShopID,
		Amount:    row.Amount,
		RefId:     row.RefID,
		Source:    selling_iface.WithdrawalSource(row.Source),
		CreatedAt: timestamppb.New(row.CreatedAt),
	}
	if !row.FundedAt.IsZero() {
		item.FundedAt = timestamppb.New(row.FundedAt)
	}
	return item
}
