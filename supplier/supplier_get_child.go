package supplier

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_models"
)

// SupplierGet implements [selling_ifaceconnect.SupplierServiceHandler].
func (s *supplierServiceImpl) SupplierGetChild(
	ctx context.Context,
	req *connect.Request[selling_iface.SupplierGetChildRequest],
) (*connect.Response[selling_iface.SupplierGetChildResponse], error) {
	pay := req.Msg
	db := s.db.WithContext(ctx)

	result := &selling_iface.SupplierGetChildResponse{
		Data: []*selling_iface.SupplierMarketplace{},
	}

	var rows []*db_models.V2SupplierMarketplace
	query := db.Model(&db_models.V2SupplierMarketplace{})

	switch pay.Type {
	case selling_iface.SupplierChildType_SUPPLIER_CHILD_TYPE_DETAIL:
		query = query.Where("supplier_id IN ?", pay.Ids)
		break

	case selling_iface.SupplierChildType_SUPPLIER_CHILD_TYPE_CHILD:
		query = query.Where("id IN ?", pay.Ids)
		break

	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("type unsupported"))
	}

	err := query.Find(&rows).Error
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		result.Data = append(result.Data, &selling_iface.SupplierMarketplace{
			MpType:      common.MarketplaceType(row.MpType),
			Id:          row.ID,
			SupplierId:  row.SupplierID,
			ShopName:    row.ShopName,
			ProductName: row.ProductName,
			Uri:         row.URI,
			Description: row.Description,
		})
	}

	return connect.NewResponse(result), nil
}
