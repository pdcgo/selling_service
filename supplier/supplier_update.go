package supplier

import (
	"context"
	"errors"
	"strconv"
	"time"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

// SupplierUpdate implements [selling_ifaceconnect.SupplierServiceHandler].
func (s *supplierServiceImpl) SupplierUpdate(
	ctx context.Context, req *connect.Request[selling_iface.SupplierUpdateRequest]) (*connect.Response[selling_iface.SupplierUpdateResponse], error) {

	pay := req.Msg
	db := s.db.WithContext(ctx)

	if detailPay := req.Msg.GetDetail(); detailPay != nil {

		err := db.
			Model(db_models.V2Supplier{}).
			Where("id = ?", pay.Id).
			Updates(map[string]any{
				"code":        detailPay.Code,
				"name":        detailPay.Name,
				"contact":     detailPay.Contact,
				"province":    detailPay.Province,
				"city":        detailPay.City,
				"address":     detailPay.Address,
				"description": detailPay.Description,
			}).
			Error
		if err != nil {
			return nil, err
		}
	}

	if childPay := req.Msg.GetChild(); childPay != nil {
		switch childPay.Type {
		case selling_iface.SupplierChildUpdateType_SUPPLIER_CHILD_UPDATE_TYPE_REMOVE:
			now := time.Now()
			ts := strconv.FormatInt(now.UnixMilli(), 10)

			err := db.Model(db_models.V2SupplierMarketplace{}).
				Where("supplier_id = ? AND id = ?", pay.Id, childPay.Id).
				Updates(map[string]interface{}{
					"product_name": gorm.Expr("product_name || ? || ?", "_"+ts, "_deleted"),
					"deleted_at":   now,
				}).
				Error

			if err != nil {
				return nil, err
			}

		case selling_iface.SupplierChildUpdateType_SUPPLIER_CHILD_UPDATE_TYPE_UPSERT:
			if childPay.GetData() == nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("supplier data is required"))
			}

			if data := childPay.Data; data != nil {
				updata := &db_models.V2SupplierMarketplace{
					SupplierID:  pay.Id,
					MpType:      int32(data.MpType),
					ShopName:    data.ShopName,
					ProductName: data.ProductName,
					URI:         data.Uri,
					Description: data.Description,
				}

				err := db.Transaction(func(tx *gorm.DB) error {
					if childPay.Id == 0 {
						return tx.
							Create(updata).
							Error
					}

					return tx.
						Model(db_models.V2SupplierMarketplace{}).
						Where("id = ?", childPay.Id).
						Updates(updata).
						Error
				})

				if err != nil {
					return nil, err
				}
			}
		}
	}

	return connect.NewResponse(&selling_iface.SupplierUpdateResponse{}), nil
}
