package supplier

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/selling_models"
	"github.com/pdcgo/shared/db_connect"
	"gorm.io/gorm"
)

// SupplierReviewList implements [selling_ifaceconnect.SupplierServiceHandler].
func (s *supplierServiceImpl) SupplierReviewList(
	ctx context.Context,
	req *connect.Request[selling_iface.SupplierReviewListRequest],
) (*connect.Response[selling_iface.SupplierReviewListResponse], error) {
	pay := req.Msg
	if pay.Page == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("page is required"))
	}

	result := &selling_iface.SupplierReviewListResponse{
		Data:     []*selling_iface.ReviewItem{},
		PageInfo: &common.PageInfo{},
	}

	db := s.db.WithContext(ctx)

	var rows []*selling_models.SupplierReview
	paginated, pageInfo, err := db_connect.SetPaginationQuery(db, func() (*gorm.DB, error) {
		query := db.
			Model(&selling_models.SupplierReview{}).
			Scopes(func(d *gorm.DB) *gorm.DB {
				f := pay.Filter
				if f == nil {
					return d
				}

				if f.TeamId > 0 {
					d = d.Where("team_id = ?", f.TeamId)
				}

				if f.SupplierId > 0 {
					d = d.Where("supplier_id = ?", f.SupplierId)
				}

				if f.UserId > 0 {
					d = d.Where("user_id = ?", f.UserId)
				}

				if f.Q != "" {
					q := "%" + strings.ToLower(f.Q) + "%"
					d = d.Where("lower(review) LIKE ?", q)
				}

				return d
			})

		return query, nil
	}, pay.Page)
	if err != nil {
		return nil, err
	}

	if err = paginated.Order("id DESC").Find(&rows).Error; err != nil {
		return nil, err
	}

	result.PageInfo = pageInfo
	for _, row := range rows {
		if row == nil {
			continue
		}
		result.Data = append(result.Data, &selling_iface.ReviewItem{
			Id:         row.ID,
			SupplierId: row.SupplierID,
			TeamId:     row.TeamID,
			UserId:     row.UserID,
			Review:     row.Review,
			Rating:     row.Rating,
		})
	}

	return connect.NewResponse(result), nil
}
