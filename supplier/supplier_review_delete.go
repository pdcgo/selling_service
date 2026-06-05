package supplier

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/selling_models"
)

// SupplierReviewDelete implements [selling_ifaceconnect.SupplierServiceHandler].
func (s *supplierServiceImpl) SupplierReviewDelete(
	ctx context.Context,
	req *connect.Request[selling_iface.SupplierReviewDeleteRequest],
) (*connect.Response[selling_iface.SupplierReviewDeleteResponse], error) {
	pay := req.Msg

	db := s.db.WithContext(ctx)
	err := db.
		Where("id = ? AND team_id = ?", pay.Id, pay.TeamId).
		Delete(&selling_models.SupplierReview{}).
		Error
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&selling_iface.SupplierReviewDeleteResponse{}), nil
}
