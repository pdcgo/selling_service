package supplier

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/selling_models"
)

// SupplierReviewCreate implements [selling_ifaceconnect.SupplierServiceHandler].
func (s *supplierServiceImpl) SupplierReviewCreate(
	ctx context.Context,
	req *connect.Request[selling_iface.SupplierReviewCreateRequest],
) (*connect.Response[selling_iface.SupplierReviewCreateResponse], error) {
	pay := req.Msg

	review := &selling_models.SupplierReview{
		SupplierID: pay.SupplierId,
		TeamID:     pay.TeamId,
		UserID:     pay.UserId,
		Review:     pay.Review,
		Rating:     pay.Rating,
	}

	db := s.db.WithContext(ctx)
	if err := db.Create(review).Error; err != nil {
		return nil, err
	}

	return connect.NewResponse(&selling_iface.SupplierReviewCreateResponse{}), nil
}
