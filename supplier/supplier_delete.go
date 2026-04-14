package supplier

import (
	"context"
	"strconv"
	"time"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

// SupplierDelete implements [selling_ifaceconnect.SupplierServiceHandler].
func (s *supplierServiceImpl) SupplierDelete(
	ctx context.Context, req *connect.Request[selling_iface.SupplierDeleteRequest]) (*connect.Response[selling_iface.SupplierDeleteResponse], error) {
	pay := req.Msg

	db := s.db.WithContext(ctx)

	now := time.Now()
	ts := strconv.FormatInt(now.UnixMilli(), 10)
	err := db.Model(db_models.V2Supplier{}).
		Where("id = ?", pay.Id).
		Updates(map[string]interface{}{
			"name":       gorm.Expr("name || ? || ?", "_"+ts, "_deleted"),
			"deleted_at": now,
		}).
		Error
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&selling_iface.SupplierDeleteResponse{}), nil
}
