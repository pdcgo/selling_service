package supplier

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_models"
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
	ShopName    string         `gorm:"not null;size:200;default:''"`
	ProductName string         `gorm:"not null;size:200;default:''"`
	URI         string         `gorm:"not null;size:500"`
	Description string         `gorm:"not null;size:500"`
	DeletedAt   gorm.DeletedAt `gorm:"index"`
}

type VariantSupplierV2 struct {
	ID           uint          `json:"id" gorm:"primarykey"`
	TeamID       uint          `json:"team_id"`
	VariantID    uint          `json:"variant_id"`
	SupplierID   uint          `json:"supplier_id"`
	PreOrderTime time.Duration `json:"pre_order_time"`

	Team     *db_models.Team           `json:"team,omitempty"`
	Variant  *db_models.VariationValue `json:"variant,omitempty"`
	Supplier *Supplier                 `json:"supplier,omitempty"`
}

type SupplierInvTxItemV2 struct {
	ID          uint `json:"id" gorm:"primarykey"`
	InvTxItemID uint `json:"inv_tx_item_id"`
	SupplierID  uint `json:"supplier_id"`

	Supplier  *Supplier            `json:"supplier"`
	InvTxItem *db_models.InvTxItem `json:"inv_tx_item"`
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
