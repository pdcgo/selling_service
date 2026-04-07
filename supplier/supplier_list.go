package supplier

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

// SupplierList implements [selling_ifaceconnect.SupplierServiceHandler].
func (s *supplierServiceImpl) SupplierList(
	ctx context.Context,
	req *connect.Request[selling_iface.SupplierListRequest],
) (*connect.Response[selling_iface.SupplierListResponse], error) {
	pay := req.Msg
	if pay.Page == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("page is required"))
	}

	result := &selling_iface.SupplierListResponse{
		Data:     []*selling_iface.SupplierListItem{},
		PageInfo: &common.PageInfo{},
	}

	db := s.db.WithContext(ctx)

	var rows []*db_models.SupplierV2
	paginated, pageInfo, err := db_connect.SetPaginationQuery(db, func() (*gorm.DB, error) {
		query := db.
			Model(&db_models.SupplierV2{}).
			Scopes(func(d *gorm.DB) *gorm.DB {

				if pay.TeamId > 0 {
					d = d.Where("team_id = ?", pay.TeamId)
				}

				if pay.Province != "" {
					d = d.Where("province = ?", pay.Province)
				}

				if pay.City != "" {
					d = d.Where("city = ?", pay.City)
				}

				if pay.Q != "" {
					q := "%" + strings.ToLower(pay.Q) + "%"
					d = d.Where("(lower(code) LIKE ? OR lower(name) LIKE ? OR lower(contact) LIKE ? OR lower(description) LIKE ? OR lower(address) LIKE ?)", q, q, q, q, q)
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
		result.Data = append(result.Data, &selling_iface.SupplierListItem{
			Id:          row.ID,
			TeamId:      row.TeamID,
			Code:        row.Code,
			Name:        row.Name,
			Contact:     row.Contact,
			Description: row.Description,
			Address:     row.Address,
			Province:    row.Province,
			City:        row.City,
		})
	}

	return connect.NewResponse(result), nil
}
