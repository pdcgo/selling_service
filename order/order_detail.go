package order

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	selling_iface "github.com/pdcgo/schema/services/selling_iface/v3"
	"github.com/pdcgo/selling_service/selling_models"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// OrderDetail implements [selling_ifaceconnect.OrderServiceHandler] (v3): the order
// header (all dimensions + money), the customer, the items with their frozen fee
// context, and the chronological dimension-aware status log.
func (s *orderServiceImpl) OrderDetail(
	ctx context.Context,
	req *connect.Request[selling_iface.OrderDetailRequest],
) (*connect.Response[selling_iface.OrderDetailResponse], error) {
	pay := req.Msg
	if pay.GetOrderId() == 0 || pay.GetTeamId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("order_id and team_id are required"))
	}
	db := s.db.WithContext(ctx)

	var ord selling_models.Order
	res := db.
		Where("id = ? AND team_id = ?", pay.GetOrderId(), pay.GetTeamId()).
		Limit(1).
		Find(&ord)
	if res.Error != nil {
		return nil, res.Error
	}
	if ord.ID == 0 {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("order not found"))
	}

	resp := &selling_iface.OrderDetailResponse{
		Id:                     ord.ID,
		TeamId:                 ord.TeamID,
		ShopId:                 ord.ShopID,
		WarehouseId:            ord.WarehouseID,
		RefId:                  ord.RefID,
		Status:                 orderStatusToProto(ord.Status),
		ShipmentStatus:         shipmentStatusToProto(ord.ShipmentStatus),
		Problem:                ord.Problem,
		ProblemNote:            ord.ProblemNote,
		InventoryTransactionId: ord.InventoryTransactionID,
		ShippingFee:            ord.ShippingFee,
		Total:                  ord.Total,
		EstRevenueAmount:       ord.EstRevenueAmount,
		WarehouseFee:           ord.WarehouseFee,
		CreatedAt:              timestamppb.New(ord.CreatedAt),
		UpdatedAt:              timestamppb.New(ord.UpdatedAt),
		Items:                  []*selling_iface.OrderDetailItem{},
		StatusLogs:             []*selling_iface.OrderStatusLogItem{},
	}
	if ord.CanceledAt != nil {
		resp.CanceledAt = timestamppb.New(*ord.CanceledAt)
	}

	var customer selling_models.OrderCustomer
	res = db.
		Where("order_id = ?", ord.ID).
		Limit(1).
		Find(&customer)
	if res.Error != nil {
		return nil, res.Error
	}
	if customer.ID != 0 {
		resp.Customer = &selling_iface.OrderCustomer{
			Name:       customer.Name,
			Phone:      customer.Phone,
			Province:   customer.Province,
			City:       customer.City,
			District:   customer.District,
			PostalCode: customer.PostalCode,
			Address:    customer.Address,
		}
	}

	var items []selling_models.OrderItem
	err := db.
		Where("order_id = ?", ord.ID).
		Order("id ASC").
		Find(&items).
		Error
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		resp.Items = append(resp.Items, &selling_iface.OrderDetailItem{
			ProductId:     it.ProductID,
			ProductName:   it.ProductName,
			Count:         it.Count,
			Price:         it.Price,
			Subtotal:      it.Subtotal,
			Owned:         it.Owned,
			MarkupPercent: it.MarkupPercent,
			ProductFee:    it.ProductFee,
		})
	}

	var logs []selling_models.OrderStatusLog
	err = db.
		Where("order_id = ?", ord.ID).
		Order("id ASC").
		Find(&logs).
		Error
	if err != nil {
		return nil, err
	}
	for _, l := range logs {
		resp.StatusLogs = append(resp.StatusLogs, &selling_iface.OrderStatusLogItem{
			Dimension:  dimensionToProto(l.Dimension),
			FromStatus: l.FromStatus,
			ToStatus:   l.ToStatus,
			Note:       l.Note,
			CreatedAt:  timestamppb.New(l.CreatedAt),
		})
	}

	return connect.NewResponse(resp), nil
}
