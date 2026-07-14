package order

import (
	"github.com/pdcgo/schema/services/inventory_iface/v1/inventory_ifaceconnect"
	"github.com/pdcgo/schema/services/product_iface/v1/product_ifaceconnect"
	"github.com/pdcgo/schema/services/warehouse_iface/v1/warehouse_ifaceconnect"
	"gorm.io/gorm"
)

// orderServiceImpl serves selling_iface.v3.OrderService (design:
// plans/brainstroming.md). Authorization is enforced by the access interceptor
// (registered in selling_service/register.go) from the (role_base.v1.request_policy)
// options on the proto, so handlers carry no imperative auth. The create/cancel flows
// dialogue with inventory (stock), product (ProductOrderInfo), and warehouse
// (GetWarehouseFee) via connect clients, and post fees to the invoice v2 ledger
// in-process (invoice_v2.PostBalanceLog, restock-accept precedent).
type orderServiceImpl struct {
	db        *gorm.DB
	inventory inventory_ifaceconnect.InventoryServiceClient
	product   product_ifaceconnect.ProductServiceClient
	warehouse warehouse_ifaceconnect.WarehouseServiceClient
}

func NewOrderService(
	db *gorm.DB,
	inventory inventory_ifaceconnect.InventoryServiceClient,
	product product_ifaceconnect.ProductServiceClient,
	warehouse warehouse_ifaceconnect.WarehouseServiceClient,
) *orderServiceImpl {
	return &orderServiceImpl{
		db:        db,
		inventory: inventory,
		product:   product,
		warehouse: warehouse,
	}
}
