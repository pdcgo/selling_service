package order

import (
	"context"
	"errors"
	"strings"
	"time"

	"connectrpc.com/connect"
	common "github.com/pdcgo/schema/services/common/v1"
	selling_iface "github.com/pdcgo/schema/services/selling_iface/v3"
	"github.com/pdcgo/selling_service/selling_models"
	"github.com/pdcgo/shared/db_connect"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

// OrderList implements [selling_ifaceconnect.OrderServiceHandler] (v3). Team-scoped,
// paginated, newest first; filters over the status dimensions + shop + search
// (ref_id / customer name). The UI derives one display status from the returned
// dimensions client-side.
func (s *orderServiceImpl) OrderList(
	ctx context.Context,
	req *connect.Request[selling_iface.OrderListRequest],
) (*connect.Response[selling_iface.OrderListResponse], error) {
	pay := req.Msg
	if pay.GetPage() == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("page is required"))
	}

	result := &selling_iface.OrderListResponse{
		Data:     []*selling_iface.OrderRow{},
		PageInfo: &common.PageInfo{},
	}
	db := s.db.WithContext(ctx)

	var rows []struct {
		selling_models.Order
		CustomerName string
	}
	paginated, pageInfo, err := db_connect.SetPaginationQuery(db, func() (*gorm.DB, error) {
		query := db.
			Table("v3_orders o").
			Joins("LEFT JOIN v3_order_customers c ON c.order_id = o.id").
			Select("o.*, COALESCE(c.name, '') as customer_name").
			Scopes(func(d *gorm.DB) *gorm.DB {
				d = d.Where("o.team_id = ?", pay.GetTeamId())
				if pay.GetShopId() > 0 {
					d = d.Where("o.shop_id = ?", pay.GetShopId())
				}
				if q := pay.GetSearch(); q != "" {
					like := "%" + strings.ToLower(q) + "%"
					d = d.Where("(lower(o.ref_id) LIKE ? OR lower(c.name) LIKE ?)", like, like)
				}
				if statuses := pay.GetStatuses(); len(statuses) > 0 {
					vals := make([]string, 0, len(statuses))
					for _, st := range statuses {
						vals = append(vals, string(orderStatusFromProto(st)))
					}
					d = d.Where("o.status IN ?", vals)
				}
				if shipments := pay.GetShipmentStatuses(); len(shipments) > 0 {
					vals := make([]string, 0, len(shipments))
					for _, st := range shipments {
						vals = append(vals, string(shipmentStatusFromProto(st)))
					}
					d = d.Where("o.shipment_status IN ?", vals)
				}
				if pay.Canceled != nil {
					if pay.GetCanceled() {
						d = d.Where("o.canceled_at IS NOT NULL")
					} else {
						d = d.Where("o.canceled_at IS NULL")
					}
				}
				if pay.Problem != nil {
					d = d.Where("o.problem = ?", pay.GetProblem())
				}
				return d
			})
		return query, nil
	}, pay.GetPage())
	if err != nil {
		return nil, err
	}

	err = paginated.
		Order("o.id DESC").
		Scan(&rows).
		Error
	if err != nil {
		return nil, err
	}

	// Item counts per order for the current page.
	ids := make([]uint64, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.ID)
	}
	itemCounts := map[uint64]int64{}
	if len(ids) > 0 {
		var counts []struct {
			OrderID uint64
			Total   int64
		}
		err = db.
			Table("v3_order_items").
			Select("order_id, COALESCE(SUM(count), 0) as total").
			Where("order_id IN ?", ids).
			Group("order_id").
			Scan(&counts).
			Error
		if err != nil {
			return nil, err
		}
		for _, c := range counts {
			itemCounts[c.OrderID] = c.Total
		}
	}

	result.PageInfo = pageInfo
	for _, r := range rows {
		row := &selling_iface.OrderRow{
			Id:               r.ID,
			RefId:            r.RefID,
			ShopId:           r.ShopID,
			WarehouseId:      r.WarehouseID,
			Status:           orderStatusToProto(r.Status),
			ShipmentStatus:   shipmentStatusToProto(r.ShipmentStatus),
			Problem:          r.Problem,
			CustomerName:     r.CustomerName,
			ItemCount:        itemCounts[r.ID],
			Total:            r.Total,
			EstRevenueAmount: r.EstRevenueAmount,
			CreatedAt:        timestamppb.New(r.CreatedAt),
		}
		if r.CanceledAt != nil {
			row.CanceledAt = timestamppb.New(*r.CanceledAt)
		}
		result.Data = append(result.Data, row)
	}

	return connect.NewResponse(result), nil
}

// touchOrderUpdatedAt bumps updated_at inside a transition tx.
func touchOrderUpdatedAt(tx *gorm.DB, orderID uint64, at time.Time) error {
	return tx.
		Model(&selling_models.Order{}).
		Where("id = ?", orderID).
		Update("updated_at", at).
		Error
}
