package selling_service

import (
	"net/http"

	"github.com/pdcgo/schema/services/inventory_iface/v1/inventory_ifaceconnect"
	"github.com/pdcgo/schema/services/product_iface/v1/product_ifaceconnect"
	"github.com/pdcgo/schema/services/warehouse_iface/v1/warehouse_ifaceconnect"
	"github.com/pdcgo/shared/configs"
	"github.com/pdcgo/shared/custom_connect"
)

// OrderDependencyClients bundles the connect clients the v3 OrderService dialogues
// with at order create/cancel: inventory (TransactionCreate/Cancel), product
// (ProductOrderInfo), and warehouse (GetWarehouseFee). Built with the default client
// interceptor so the caller's bearer is forwarded (product's ProductOrderInfo is
// team-scoped by the v2 roling system).
type OrderDependencyClients struct {
	Inventory inventory_ifaceconnect.InventoryServiceClient
	Product   product_ifaceconnect.ProductServiceClient
	Warehouse warehouse_ifaceconnect.WarehouseServiceClient
}

// NewOrderDependencyClients is the wire provider. Endpoints come from the app config;
// an empty endpoint falls back to the dev omnibus (everything on localhost:8086).
func NewOrderDependencyClients(cfg *configs.AppConfig) (OrderDependencyClients, error) {
	clientInterceptor, err := custom_connect.NewDefaultClientInterceptor()
	if err != nil {
		return OrderDependencyClients{}, err
	}

	endpoint := func(e string) string {
		if e == "" {
			return "http://localhost:8086"
		}
		return e
	}

	return OrderDependencyClients{
		Inventory: inventory_ifaceconnect.NewInventoryServiceClient(
			http.DefaultClient, endpoint(cfg.InventoryService.Endpoint), clientInterceptor),
		Product: product_ifaceconnect.NewProductServiceClient(
			http.DefaultClient, endpoint(cfg.ProductService.Endpoint), clientInterceptor),
		Warehouse: warehouse_ifaceconnect.NewWarehouseServiceClient(
			http.DefaultClient, endpoint(cfg.WarehouseService.Endpoint), clientInterceptor),
	}, nil
}
