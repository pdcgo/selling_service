package supplier

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_models"
)

// SupplierCreate implements [selling_ifaceconnect.SupplierServiceHandler].
func (s *supplierServiceImpl) SupplierCreate(
	ctx context.Context,
	req *connect.Request[selling_iface.SupplierCreateRequest],
) (*connect.Response[selling_iface.SupplierCreateResponse], error) {
	pay := req.Msg
	result := &selling_iface.SupplierCreateResponse{
		Data: &selling_iface.SupplierDetail{
			TeamId: pay.TeamId,
		},
	}

	supplier := &db_models.V2Supplier{
		TeamID:      pay.TeamId,
		Code:        pay.Code,
		Name:        pay.Name,
		Contact:     pay.Contact,
		Province:    pay.Province,
		City:        pay.City,
		Description: pay.Description,
		Address:     pay.Address,
	}

	db := s.db.WithContext(ctx)
	if err := db.Create(supplier).Error; err != nil {
		return nil, err
	}

	result.Id = supplier.ID
	result.Data.Id = supplier.ID
	return connect.NewResponse(result), nil
}
