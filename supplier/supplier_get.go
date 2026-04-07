package supplier

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_models"
)

// SupplierGet implements [selling_ifaceconnect.SupplierServiceHandler].
func (s *supplierServiceImpl) SupplierGet(
	ctx context.Context,
	req *connect.Request[selling_iface.SupplierGetRequest],
) (*connect.Response[selling_iface.SupplierGetResponse], error) {
	pay := req.Msg
	db := s.db.WithContext(ctx)

	result := &selling_iface.SupplierGetResponse{
		Data: []*selling_iface.SupplierDetail{},
	}

	type row struct {
		*db_models.SupplierV2
		Childs []*db_models.SupplierMarketplaceV2 `gorm:"foreignKey:SupplierID;references:ID"`
	}

	var rows []*row
	err := db.
		Table("suppliers").
		Preload("Childs").
		Where("id IN ?", pay.Ids).
		Find(&rows).
		Error
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		supplier := selling_iface.SupplierDetail{
			Id:          row.ID,
			TeamId:      row.TeamID,
			Code:        row.Code,
			Name:        row.Name,
			Contact:     row.Contact,
			Description: row.Description,
			Address:     row.Address,
			Province:    row.Province,
			City:        row.City,
		}

		for _, supplierMp := range row.Childs {
			supplier.Childs = append(supplier.Childs, &selling_iface.SupplierMarketplace{
				MpType:      common.MarketplaceType(supplierMp.MpType),
				SupplierId:  supplierMp.ID,
				ShopName:    supplierMp.ShopName,
				ProductName: supplierMp.ProductName,
				Uri:         supplierMp.URI,
				Description: supplierMp.Description,
			})
		}

		result.Data = append(result.Data, &supplier)
	}

	return connect.NewResponse(result), nil
}
