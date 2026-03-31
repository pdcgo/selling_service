package supplier

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

// SupplierCreate implements [selling_ifaceconnect.SupplierServiceHandler].
func (s *supplierServiceImpl) SupplierCreate(
	ctx context.Context,
	req *connect.Request[selling_iface.SupplierCreateRequest],
) (*connect.Response[selling_iface.SupplierCreateResponse], error) {
	var err error
	pay := req.Msg
	result := &selling_iface.SupplierCreateResponse{
		Data: &selling_iface.Supplier{
			TeamId: pay.TeamId,
		},
	}

	db := s.db.WithContext(ctx)
	err = db.Transaction(func(tx *gorm.DB) error {
		switch payload := pay.Data.(type) {
		case *selling_iface.SupplierCreateRequest_Custom:
			if payload.Custom == nil {
				return connect.NewError(connect.CodeInvalidArgument, errors.New("custom payload is required"))
			}

			supplier := &db_models.Supplier{
				TeamID: pay.TeamId,
				Type:   selling_iface.SupplierType_SUPPLIER_TYPE_CUSTOM,
			}
			if err := tx.Create(supplier).Error; err != nil {
				return err
			}

			custom := &db_models.SupplierCustom{
				SupplierID:  supplier.ID,
				Name:        payload.Custom.Name,
				Contact:     payload.Custom.Contact,
				Description: payload.Custom.Description,
			}
			if err := tx.Create(custom).Error; err != nil {
				return err
			}

			result.Id = supplier.ID
			result.Data.Id = supplier.ID
			result.Data.Data = &selling_iface.Supplier_Custom{
				Custom: &selling_iface.SupplierCustom{
					Name:        custom.Name,
					Contact:     custom.Contact,
					Description: custom.Description,
				},
			}
			return nil
		case *selling_iface.SupplierCreateRequest_Marketplace:
			if payload.Marketplace == nil {
				return connect.NewError(connect.CodeInvalidArgument, errors.New("marketplace payload is required"))
			}

			supplier := &db_models.Supplier{
				TeamID: pay.TeamId,
				Type:   selling_iface.SupplierType_SUPPLIER_TYPE_MARKETPLACE,
			}
			if err := tx.Create(supplier).Error; err != nil {
				return err
			}

			marketplace := &db_models.SupplierMarketplace{
				SupplierID:  supplier.ID,
				MpType:      int32(payload.Marketplace.MpType),
				ShopName:    payload.Marketplace.ShopName,
				ProductName: payload.Marketplace.ProductName,
				URI:         payload.Marketplace.Uri,
				Description: payload.Marketplace.Description,
			}
			if err := tx.Create(marketplace).Error; err != nil {
				return err
			}

			result.Id = supplier.ID
			result.Data.Id = supplier.ID
			result.Data.Data = &selling_iface.Supplier_Marketplace{
				Marketplace: &selling_iface.SupplierMarketplace{
					MpType:      payload.Marketplace.MpType,
					ShopName:    marketplace.ShopName,
					ProductName: marketplace.ProductName,
					Uri:         marketplace.URI,
					Description: marketplace.Description,
				},
			}
			return nil
		default:
			return connect.NewError(connect.CodeInvalidArgument, errors.New("supplier data is required"))
		}

	})
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(result), nil
}
