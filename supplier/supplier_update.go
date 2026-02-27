package supplier

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SupplierUpdate implements [selling_ifaceconnect.SupplierServiceHandler].
func (s *supplierServiceImpl) SupplierUpdate(
	ctx context.Context, req *connect.Request[selling_iface.SupplierUpdateRequest]) (*connect.Response[selling_iface.SupplierUpdateResponse], error) {
	pay := req.Msg
	if pay.GetData() == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("supplier data is required"))
	}
	if pay.Data.GetId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("supplier id is required"))
	}

	db := s.db.WithContext(ctx)
	err := db.Transaction(func(tx *gorm.DB) error {
		base := Supplier{}
		err := tx.
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", pay.Data.Id).
			First(&base).
			Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return connect.NewError(connect.CodeNotFound, fmt.Errorf("supplier not found id=%d", pay.Data.Id))
		}
		if err != nil {
			return err
		}

		updates := map[string]any{}
		if pay.Data.TeamId > 0 {
			updates["team_id"] = pay.Data.TeamId
		}
		if len(updates) > 0 {
			if err := tx.Model(&Supplier{}).Where("id = ?", base.ID).Updates(updates).Error; err != nil {
				return err
			}
		}

		switch data := pay.Data.Data.(type) {
		case *selling_iface.Supplier_Custom:
			if base.Type != selling_iface.SupplierType_SUPPLIER_TYPE_CUSTOM {
				return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("supplier type mismatch, expected %s got custom", base.Type.String()))
			}
			if data.Custom == nil {
				return connect.NewError(connect.CodeInvalidArgument, errors.New("custom payload is required"))
			}

			err := tx.Model(&SupplierCustom{}).
				Where("supplier_id = ?", base.ID).
				Updates(map[string]any{
					"name":        data.Custom.Name,
					"contact":     data.Custom.Contact,
					"description": data.Custom.Description,
				}).
				Error
			if err != nil {
				return err
			}
		case *selling_iface.Supplier_Marketplace:
			if base.Type != selling_iface.SupplierType_SUPPLIER_TYPE_MARKETPLACE {
				return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("supplier type mismatch, expected %s got marketplace", base.Type.String()))
			}
			if data.Marketplace == nil {
				return connect.NewError(connect.CodeInvalidArgument, errors.New("marketplace payload is required"))
			}

			err := tx.Model(&SupplierMarketplace{}).
				Where("supplier_id = ?", base.ID).
				Updates(map[string]any{
					"mp_type":      int32(common.MarketplaceType(data.Marketplace.MpType)),
					"name":         data.Marketplace.Name,
					"uri":          data.Marketplace.Uri,
					"description":  data.Marketplace.Description,
				}).
				Error
			if err != nil {
				return err
			}
		default:
			return connect.NewError(connect.CodeInvalidArgument, errors.New("supplier data payload is required"))
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&selling_iface.SupplierUpdateResponse{}), nil
}
