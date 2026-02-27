package supplier

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_connect"
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
		Data:     []*selling_iface.Supplier{},
		PageInfo: &common.PageInfo{},
	}

	db := s.db.WithContext(ctx)

	switch pay.Type {
	case selling_iface.SupplierType_SUPPLIER_TYPE_CUSTOM:
		type customRow struct {
			ID          uint64 `gorm:"column:id"`
			TeamID      uint64 `gorm:"column:team_id"`
			Name        string `gorm:"column:name"`
			Contact     string `gorm:"column:contact"`
			Description string `gorm:"column:description"`
		}

		var rows []*customRow
		paginated, pageInfo, err := db_connect.SetPaginationQuery(db, func() (*gorm.DB, error) {
			query := db.
				Model(&Supplier{}).
				Joins("JOIN selling_supplier_customs c ON c.supplier_id = selling_suppliers.id AND c.deleted_at IS NULL").
				Where("selling_suppliers.team_id = ?", pay.TeamId).
				Where("selling_suppliers.type = ?", pay.Type).
				Select("selling_suppliers.id, selling_suppliers.team_id, c.name, c.contact, c.description")

			if pay.Q != "" {
				q := "%" + strings.ToLower(pay.Q) + "%"
				query = query.Where("(lower(c.name) LIKE ? OR lower(c.contact) LIKE ? OR lower(c.description) LIKE ?)", q, q, q)
			}

			return query, nil
		}, pay.Page)
		if err != nil {
			return nil, err
		}

		err = paginated.Order("selling_suppliers.id DESC").Find(&rows).Error
		if err != nil {
			return nil, err
		}

		result.PageInfo = pageInfo
		for _, row := range rows {
			if row == nil {
				continue
			}
			result.Data = append(result.Data, &selling_iface.Supplier{
				Id:     row.ID,
				TeamId: row.TeamID,
				Data: &selling_iface.Supplier_Custom{
					Custom: &selling_iface.SupplierCustom{
						Name:        row.Name,
						Contact:     row.Contact,
						Description: row.Description,
					},
				},
			})
		}

	case selling_iface.SupplierType_SUPPLIER_TYPE_MARKETPLACE:
		type marketplaceRow struct {
			ID          uint64 `gorm:"column:id"`
			TeamID      uint64 `gorm:"column:team_id"`
			MpType      int32  `gorm:"column:mp_type"`
			Name        string `gorm:"column:name"`
			URI         string `gorm:"column:uri"`
			Description string `gorm:"column:description"`
		}

		var rows []*marketplaceRow
		paginated, pageInfo, err := db_connect.SetPaginationQuery(db, func() (*gorm.DB, error) {
			query := db.
				Model(&Supplier{}).
				Joins("JOIN selling_supplier_marketplaces m ON m.supplier_id = selling_suppliers.id AND m.deleted_at IS NULL").
				Where("selling_suppliers.team_id = ?", pay.TeamId).
				Where("selling_suppliers.type = ?", pay.Type).
				Select("selling_suppliers.id, selling_suppliers.team_id, m.mp_type, m.name, m.uri, m.description")

			if pay.Q != "" {
				q := "%" + strings.ToLower(pay.Q) + "%"
				query = query.Where("(lower(m.name) LIKE ? OR lower(m.uri) LIKE ? OR lower(m.description) LIKE ?)", q, q, q)
			}

			return query, nil
		}, pay.Page)
		if err != nil {
			return nil, err
		}

		err = paginated.Order("selling_suppliers.id DESC").Find(&rows).Error
		if err != nil {
			return nil, err
		}

		result.PageInfo = pageInfo
		for _, row := range rows {
			if row == nil {
				continue
			}
			result.Data = append(result.Data, &selling_iface.Supplier{
				Id:     row.ID,
				TeamId: row.TeamID,
				Data: &selling_iface.Supplier_Marketplace{
					Marketplace: &selling_iface.SupplierMarketplace{
						MpType:      common.MarketplaceType(row.MpType),
						Name:        row.Name,
						Uri:         row.URI,
						Description: row.Description,
					},
				},
			})
		}
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("unsupported supplier type"))
	}

	return connect.NewResponse(result), nil
}
