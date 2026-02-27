package supplier

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/common/v1"
	"gorm.io/gorm"
)

// SupplierGet implements [selling_ifaceconnect.SupplierServiceHandler].
func (s *supplierServiceImpl) SupplierGet(
	ctx context.Context,
	req *connect.Request[selling_iface.SupplierGetRequest],
) (*connect.Response[selling_iface.SupplierGetResponse], error) {
	pay := req.Msg
	db := s.db.WithContext(ctx)

	base := Supplier{}
	err := db.
		Where("id = ?", pay.Id).
		Where("type = ?", pay.Type).
		First(&base).
		Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("supplier not found id=%d type=%s", pay.Id, pay.Type.String()))
	}
	if err != nil {
		return nil, err
	}

	result := &selling_iface.SupplierGetResponse{
		Data: &selling_iface.Supplier{
			Id:     base.ID,
			TeamId: base.TeamID,
		},
	}

	switch pay.Type {
	case selling_iface.SupplierType_SUPPLIER_TYPE_CUSTOM:
		custom := SupplierCustom{}
		err = db.Where("supplier_id = ?", base.ID).First(&custom).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("custom supplier detail not found for id=%d", pay.Id))
		}
		if err != nil {
			return nil, err
		}

		result.Data.Data = &selling_iface.Supplier_Custom{
			Custom: &selling_iface.SupplierCustom{
				Name:        custom.Name,
				Contact:     custom.Contact,
				Description: custom.Description,
			},
		}
	case selling_iface.SupplierType_SUPPLIER_TYPE_MARKETPLACE:
		mp := SupplierMarketplace{}
		err = db.Where("supplier_id = ?", base.ID).First(&mp).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("marketplace supplier detail not found for id=%d", pay.Id))
		}
		if err != nil {
			return nil, err
		}

		result.Data.Data = &selling_iface.Supplier_Marketplace{
			Marketplace: &selling_iface.SupplierMarketplace{
				MpType:      common.MarketplaceType(mp.MpType),
				Name:        mp.Name,
				Uri:         mp.URI,
				Description: mp.Description,
			},
		}
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unsupported supplier type: %s", pay.Type.String()))
	}

	return connect.NewResponse(result), nil
}
