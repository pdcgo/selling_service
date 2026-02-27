package supplier

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SupplierDelete implements [selling_ifaceconnect.SupplierServiceHandler].
func (s *supplierServiceImpl) SupplierDelete(
	ctx context.Context, req *connect.Request[selling_iface.SupplierDeleteRequest]) (*connect.Response[selling_iface.SupplierDeleteResponse], error) {
	pay := req.Msg

	db := s.db.WithContext(ctx)
	err := db.Transaction(func(tx *gorm.DB) error {
		var supplier Supplier
		err := tx.
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", pay.Id).
			Where("type = ?", pay.Type).
			First(&supplier).
			Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return connect.NewError(connect.CodeNotFound, fmt.Errorf("supplier not found id=%d type=%s", pay.Id, pay.Type.String()))
		}
		if err != nil {
			return err
		}

		switch pay.Type {
		case selling_iface.SupplierType_SUPPLIER_TYPE_CUSTOM:
			if err := tx.Where("supplier_id = ?", pay.Id).Delete(&SupplierCustom{}).Error; err != nil {
				return err
			}
		case selling_iface.SupplierType_SUPPLIER_TYPE_MARKETPLACE:
			if err := tx.Where("supplier_id = ?", pay.Id).Delete(&SupplierMarketplace{}).Error; err != nil {
				return err
			}
		default:
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unsupported supplier type: %s", pay.Type.String()))
		}

		if err := tx.Where("id = ?", pay.Id).Delete(&Supplier{}).Error; err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&selling_iface.SupplierDeleteResponse{}), nil
}
