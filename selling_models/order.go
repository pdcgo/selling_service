package selling_models

import "time"

// OrderStatus is the warehouse-driven fulfillment pipeline dimension (forward-only).
// Stock, racks, and fees all moved at "created" (Flow A); "shipped" ends the pipeline
// and the shipment dimension takes over.
type OrderStatus string

const (
	OrderStatusCreated          OrderStatus = "created"
	OrderStatusProcess          OrderStatus = "process"
	OrderStatusPicking          OrderStatus = "picking"
	OrderStatusPacking          OrderStatus = "packing"
	OrderStatusPackingCompleted OrderStatus = "packing_completed"
	OrderStatusShipped          OrderStatus = "shipped"
)

// ShipmentStatus is the courier dimension ("" = none; dormant until shipped).
type ShipmentStatus string

const (
	ShipmentStatusNone            ShipmentStatus = ""
	ShipmentStatusInTransit       ShipmentStatus = "in_transit"
	ShipmentStatusDelivered       ShipmentStatus = "delivered"
	ShipmentStatusReturnInTransit ShipmentStatus = "return_in_transit"
	ShipmentStatusReturned        ShipmentStatus = "returned"
)

// Status-log dimensions (OrderStatusLog.Dimension).
const (
	OrderDimensionFulfillment = "fulfillment"
	OrderDimensionShipment    = "shipment"
	OrderDimensionFlag        = "flag"
)

// Order is the next-gen selling order (v3 OrderService, design: plans/brainstroming.md).
// Creating one is Flow A: stock exits the fulfilling warehouse (inventory
// TransactionCreate{order}, racks auto-picked per ProductConfig), and PRODUCT_FEE /
// WAREHOUSE_FEE post to the invoice v2 ledger — all atomic with the insert. An order is
// CANCELED (flag + full reversal), never deleted, so (team_id, ref_id) is a plain unique.
type Order struct {
	ID          uint64 `json:"id" gorm:"primaryKey;autoIncrement"`
	TeamID      uint64 `json:"team_id" gorm:"not null;index;uniqueIndex:uidx_v3_order_ref,priority:1"`
	ShopID      uint64 `json:"shop_id" gorm:"not null;index"`
	WarehouseID uint64 `json:"warehouse_id" gorm:"not null;index"`
	// Marketplace order number / reference; unique per team.
	RefID string `json:"ref_id" gorm:"not null;size:200;uniqueIndex:uidx_v3_order_ref,priority:2"`

	// Dimensional statuses (see plans/brainstroming.md §3).
	Status         OrderStatus    `json:"status" gorm:"not null;size:32;default:'created';index"`
	ShipmentStatus ShipmentStatus `json:"shipment_status" gorm:"not null;size:32;default:''"`
	CanceledAt     *time.Time     `json:"canceled_at"`
	Problem        bool           `json:"problem" gorm:"not null;default:false"`
	ProblemNote    string         `json:"problem_note" gorm:"not null;default:''"`

	// The outbound inventory transaction minted at create; TransactionCancel uses it.
	InventoryTransactionID uint64 `json:"inventory_transaction_id" gorm:"not null;default:0"`

	ShippingFee float64 `json:"shipping_fee" gorm:"not null;default:0"`
	// Σ item subtotals + shipping fee (selling prices).
	Total float64 `json:"total" gorm:"not null;default:0"`
	// Estimated revenue: the expected marketplace payout (legacy: order_mp_total).
	EstRevenueAmount float64 `json:"est_revenue_amount" gorm:"not null;default:0"`
	// Frozen at create from warehouse GetWarehouseFee; posted as WAREHOUSE_FEE.
	WarehouseFee float64 `json:"warehouse_fee" gorm:"not null;default:0"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (Order) TableName() string { return "v3_orders" }

// OrderItem is one order line. Price is the per-unit SELLING price; the fee context
// (Owned/OwnerTeamID/MarkupPercent/ProductFee) is FROZEN at create from
// ProductOrderInfo + the inventory cost valuation, for auditability and cancel
// reversal.
type OrderItem struct {
	ID          uint64 `json:"id" gorm:"primaryKey;autoIncrement"`
	OrderID     uint64 `json:"order_id" gorm:"not null;index"`
	ProductID   uint64 `json:"product_id" gorm:"not null"`
	ProductName string `json:"product_name" gorm:"not null;size:200;default:''"` // snapshot at order time
	Count       int64  `json:"count" gorm:"not null;default:0"`

	Price    float64 `json:"price" gorm:"not null;default:0"`
	Subtotal float64 `json:"subtotal" gorm:"not null;default:0"` // count × price

	// NOTE: no gorm `default` tag — with one, gorm omits false values on insert and
	// the DB default would silently overwrite them.
	Owned         bool    `json:"owned" gorm:"not null"`
	OwnerTeamID   uint64  `json:"owner_team_id" gorm:"not null;default:0"`
	MarkupPercent float64 `json:"markup_percent" gorm:"not null;default:0"`
	// count × Markup(unit_cost) for non-owned items (0 when owned) — the PRODUCT_FEE share.
	ProductFee float64 `json:"product_fee" gorm:"not null;default:0"`
}

func (OrderItem) TableName() string { return "v3_order_items" }

// OrderCustomer is the buyer data — same field set as legacy CustomerAddress, 1:1
// with the order.
type OrderCustomer struct {
	ID         uint64 `json:"id" gorm:"primaryKey;autoIncrement"`
	OrderID    uint64 `json:"order_id" gorm:"not null;uniqueIndex"`
	Name       string `json:"name" gorm:"not null;size:200;default:''"`
	Phone      string `json:"phone" gorm:"not null;size:50;default:''"`
	Province   string `json:"province" gorm:"not null;size:100;default:''"`
	City       string `json:"city" gorm:"not null;size:100;default:''"`
	District   string `json:"district" gorm:"not null;size:100;default:''"`
	PostalCode string `json:"postal_code" gorm:"not null;size:20;default:''"`
	Address    string `json:"address" gorm:"not null;size:500;default:''"`
}

func (OrderCustomer) TableName() string { return "v3_order_customers" }

// OrderStatusLog is the dimension-aware audit trail: one row per transition in any
// dimension (fulfillment | shipment | flag), written in the same transaction as the
// transition itself.
type OrderStatusLog struct {
	ID         uint64    `json:"id" gorm:"primaryKey;autoIncrement"`
	OrderID    uint64    `json:"order_id" gorm:"not null;index"`
	Dimension  string    `json:"dimension" gorm:"not null;size:20;default:''"`
	FromStatus string    `json:"from_status" gorm:"not null;size:32;default:''"` // empty on the create row
	ToStatus   string    `json:"to_status" gorm:"not null;size:32;default:''"`
	Note       string    `json:"note" gorm:"not null;default:''"`
	CreatedAt  time.Time `json:"created_at"`
}

func (OrderStatusLog) TableName() string { return "v3_order_status_logs" }
