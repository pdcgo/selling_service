package supplier

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
)

// SupplierGet implements [selling_ifaceconnect.SupplierServiceHandler].
func (s *supplierServiceImpl) SupplierGet(
	ctx context.Context,
	req *connect.Request[selling_iface.SupplierGetRequest],
) (*connect.Response[selling_iface.SupplierGetResponse], error) {
	pay := req.Msg
	db := s.db.WithContext(ctx)

	result := &selling_iface.SupplierGetResponse{
		Data: []*selling_iface.Supplier{},
	}

	type row struct {
		*Supplier
		Custom      *SupplierCustom      `gorm:"foreignKey:SupplierID;references:ID"`
		Marketplace *SupplierMarketplace `gorm:"foreignKey:SupplierID;references:ID"`
	}

	var rows []*row
	err := db.
		Table("suppliers").
		Preload("Custom").
		Preload("Marketplace").
		Where("id IN ?", pay.Ids).
		Find(&rows).
		Error
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		supplier := selling_iface.Supplier{
			Id:     row.ID,
			TeamId: row.TeamID,
		}

		switch row.Type {
		case selling_iface.SupplierType_SUPPLIER_TYPE_CUSTOM:
			supplier.Data = &selling_iface.Supplier_Custom{
				Custom: &selling_iface.SupplierCustom{
					Name:        row.Custom.Name,
					Contact:     row.Custom.Contact,
					Description: row.Custom.Description,
				},
			}

		case selling_iface.SupplierType_SUPPLIER_TYPE_MARKETPLACE:
			supplier.Data = &selling_iface.Supplier_Marketplace{
				Marketplace: &selling_iface.SupplierMarketplace{
					MpType:      common.MarketplaceType(row.Marketplace.MpType),
					ShopName:    row.Marketplace.ShopName,
					ProductName: row.Marketplace.ProductName,
					Uri:         row.Marketplace.URI,
					Description: row.Marketplace.Description,
				},
			}
		}

		result.Data = append(result.Data, &supplier)
	}

	return connect.NewResponse(result), nil
}
