package supplier

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

type Supplier struct {
	ID        uint64                     `gorm:"primaryKey;autoIncrement"`
	TeamID    uint64                     `gorm:"not null;index"`
	Type      selling_iface.SupplierType `gorm:"not null"`
	DeletedAt gorm.DeletedAt             `gorm:"index"`
}

type SupplierCustom struct {
	SupplierID  uint64         `gorm:"primaryKey"`
	Name        string         `gorm:"not null;size:200"`
	Contact     string         `gorm:"not null;size:50"`
	Description string         `gorm:"not null;size:500"`
	DeletedAt   gorm.DeletedAt `gorm:"index"`
}

type SupplierMarketplace struct {
	SupplierID  uint64         `gorm:"primaryKey"`
	MpType      int32          `gorm:"not null"`
	Name        string         `gorm:"not null;size:200"`
	URI         string         `gorm:"not null;size:500"`
	Description string         `gorm:"not null;size:500"`
	DeletedAt   gorm.DeletedAt `gorm:"index"`
}

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

			supplier := &Supplier{
				TeamID: pay.TeamId,
				Type:   selling_iface.SupplierType_SUPPLIER_TYPE_CUSTOM,
			}
			if err := tx.Create(supplier).Error; err != nil {
				return err
			}

			custom := &SupplierCustom{
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

			supplier := &Supplier{
				TeamID: pay.TeamId,
				Type:   selling_iface.SupplierType_SUPPLIER_TYPE_MARKETPLACE,
			}
			if err := tx.Create(supplier).Error; err != nil {
				return err
			}

			marketplace := &SupplierMarketplace{
				SupplierID:  supplier.ID,
				MpType:      int32(payload.Marketplace.MpType),
				Name:        payload.Marketplace.Name,
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
					Name:        marketplace.Name,
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
