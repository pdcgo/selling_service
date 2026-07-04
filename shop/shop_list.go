package shop

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_connect"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

// ShopList implements [selling_ifaceconnect.ShopServiceHandler].
func (s *shopServiceImpl) ShopList(
	ctx context.Context,
	req *connect.Request[selling_iface.ShopListRequest],
) (*connect.Response[selling_iface.ShopListResponse], error) {
	pay := req.Msg
	if pay.Page == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("page is required"))
	}

	result := &selling_iface.ShopListResponse{
		Data:     []*selling_iface.ShopListItem{},
		PageInfo: &common.PageInfo{},
	}

	db := s.db.WithContext(ctx)

	var rows []*db_models.V2Shop
	paginated, pageInfo, err := db_connect.SetPaginationQuery(db, func() (*gorm.DB, error) {
		query := db.
			Model(&db_models.V2Shop{}).
			Scopes(func(d *gorm.DB) *gorm.DB {

				if pay.TeamId > 0 {
					d = d.Where("team_id = ?", pay.TeamId)
				}

				if pay.MarketplaceType != 0 {
					d = d.Where("marketplace_type = ?", int32(pay.MarketplaceType))
				}

				if pay.Q != "" {
					q := "%" + strings.ToLower(pay.Q) + "%"
					d = d.Where("(lower(shop_name) LIKE ? OR lower(shop_username) LIKE ?)", q, q)
				}

				return d
			})

		return query, nil
	}, pay.Page)
	if err != nil {
		return nil, err
	}

	err = paginated.Order("id DESC").Find(&rows).Error
	if err != nil {
		return nil, err
	}

	result.PageInfo = pageInfo
	for _, row := range rows {
		if row == nil {
			continue
		}
		result.Data = append(result.Data, &selling_iface.ShopListItem{
			Id:              row.ID,
			TeamId:          row.TeamID,
			ShopName:        row.ShopName,
			ShopUsername:    row.ShopUsername,
			MarketplaceType: common.MarketplaceType(row.MarketplaceType),
			Uri:             row.URI,
		})
	}

	return connect.NewResponse(result), nil
}
