package order

import (
	selling_iface "github.com/pdcgo/schema/services/selling_iface/v3"
	"github.com/pdcgo/selling_service/selling_models"
	"gorm.io/gorm"
	"time"
)

// orderStatusToProto / orderStatusFromProto map the stored fulfillment status string
// to the proto enum and back.
func orderStatusToProto(s selling_models.OrderStatus) selling_iface.OrderStatus {
	switch s {
	case selling_models.OrderStatusCreated:
		return selling_iface.OrderStatus_ORDER_STATUS_CREATED
	case selling_models.OrderStatusProcess:
		return selling_iface.OrderStatus_ORDER_STATUS_PROCESS
	case selling_models.OrderStatusPicking:
		return selling_iface.OrderStatus_ORDER_STATUS_PICKING
	case selling_models.OrderStatusPacking:
		return selling_iface.OrderStatus_ORDER_STATUS_PACKING
	case selling_models.OrderStatusPackingCompleted:
		return selling_iface.OrderStatus_ORDER_STATUS_PACKING_COMPLETED
	case selling_models.OrderStatusShipped:
		return selling_iface.OrderStatus_ORDER_STATUS_SHIPPED
	default:
		return selling_iface.OrderStatus_ORDER_STATUS_UNSPECIFIED
	}
}

func orderStatusFromProto(s selling_iface.OrderStatus) selling_models.OrderStatus {
	switch s {
	case selling_iface.OrderStatus_ORDER_STATUS_CREATED:
		return selling_models.OrderStatusCreated
	case selling_iface.OrderStatus_ORDER_STATUS_PROCESS:
		return selling_models.OrderStatusProcess
	case selling_iface.OrderStatus_ORDER_STATUS_PICKING:
		return selling_models.OrderStatusPicking
	case selling_iface.OrderStatus_ORDER_STATUS_PACKING:
		return selling_models.OrderStatusPacking
	case selling_iface.OrderStatus_ORDER_STATUS_PACKING_COMPLETED:
		return selling_models.OrderStatusPackingCompleted
	case selling_iface.OrderStatus_ORDER_STATUS_SHIPPED:
		return selling_models.OrderStatusShipped
	default:
		return ""
	}
}

// shipmentStatusToProto / shipmentStatusFromProto map the courier dimension ("" = none
// = UNSPECIFIED).
func shipmentStatusToProto(s selling_models.ShipmentStatus) selling_iface.ShipmentStatus {
	switch s {
	case selling_models.ShipmentStatusInTransit:
		return selling_iface.ShipmentStatus_SHIPMENT_STATUS_IN_TRANSIT
	case selling_models.ShipmentStatusDelivered:
		return selling_iface.ShipmentStatus_SHIPMENT_STATUS_DELIVERED
	case selling_models.ShipmentStatusReturnInTransit:
		return selling_iface.ShipmentStatus_SHIPMENT_STATUS_RETURN_IN_TRANSIT
	case selling_models.ShipmentStatusReturned:
		return selling_iface.ShipmentStatus_SHIPMENT_STATUS_RETURNED
	default:
		return selling_iface.ShipmentStatus_SHIPMENT_STATUS_UNSPECIFIED
	}
}

func shipmentStatusFromProto(s selling_iface.ShipmentStatus) selling_models.ShipmentStatus {
	switch s {
	case selling_iface.ShipmentStatus_SHIPMENT_STATUS_IN_TRANSIT:
		return selling_models.ShipmentStatusInTransit
	case selling_iface.ShipmentStatus_SHIPMENT_STATUS_DELIVERED:
		return selling_models.ShipmentStatusDelivered
	case selling_iface.ShipmentStatus_SHIPMENT_STATUS_RETURN_IN_TRANSIT:
		return selling_models.ShipmentStatusReturnInTransit
	case selling_iface.ShipmentStatus_SHIPMENT_STATUS_RETURNED:
		return selling_models.ShipmentStatusReturned
	default:
		return selling_models.ShipmentStatusNone
	}
}

func dimensionToProto(d string) selling_iface.StatusDimension {
	switch d {
	case selling_models.OrderDimensionFulfillment:
		return selling_iface.StatusDimension_STATUS_DIMENSION_FULFILLMENT
	case selling_models.OrderDimensionShipment:
		return selling_iface.StatusDimension_STATUS_DIMENSION_SHIPMENT
	case selling_models.OrderDimensionFlag:
		return selling_iface.StatusDimension_STATUS_DIMENSION_FLAG
	default:
		return selling_iface.StatusDimension_STATUS_DIMENSION_UNSPECIFIED
	}
}

// appendOrderStatusLog writes one dimension-aware audit row, in the same transaction
// as the transition itself.
func appendOrderStatusLog(tx *gorm.DB, orderID uint64, dimension, from, to, note string, at time.Time) error {
	log := selling_models.OrderStatusLog{
		OrderID:    orderID,
		Dimension:  dimension,
		FromStatus: from,
		ToStatus:   to,
		Note:       note,
		CreatedAt:  at,
	}
	return tx.Create(&log).Error
}
