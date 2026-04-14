package supplier

import (
	"context"

	"connectrpc.com/connect"
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

	var rows []*db_models.V2Supplier
	err := db.
		Model(db_models.V2Supplier{}).
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

		result.Data = append(result.Data, &supplier)
	}

	return connect.NewResponse(result), nil
}
