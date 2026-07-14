package selling_service

import (
	"net/http"

	"cloud.google.com/go/firestore"
	"connectrpc.com/connect"
	"github.com/pdcgo/san_collection/san_caches"
	"github.com/pdcgo/schema/services/selling_iface/v1/selling_ifaceconnect"
	selling_ifacev3connect "github.com/pdcgo/schema/services/selling_iface/v3/selling_ifaceconnect"
	"github.com/pdcgo/selling_service/order"
	"github.com/pdcgo/selling_service/revenue"
	"github.com/pdcgo/selling_service/services"
	"github.com/pdcgo/selling_service/shop"
	"github.com/pdcgo/selling_service/stat_service"
	"github.com/pdcgo/selling_service/supplier"
	"github.com/pdcgo/shared/configs"
	"github.com/pdcgo/shared/custom_connect"
	"github.com/pdcgo/shared/interfaces/authorization_iface"
	"github.com/pdcgo/shared/pkg/ware_cache"
	"github.com/pdcgo/user_service/access_interceptors"
	"gorm.io/gorm"
)

type ServiceReflectNames []string
type RegisterHandler func() ServiceReflectNames

func NewRegister(
	mux *http.ServeMux,
	db *gorm.DB,
	cfg *configs.AppConfig,
	auth authorization_iface.Authorization,
	defaultInterceptor custom_connect.DefaultInterceptor,
	client *firestore.Client,
	cache ware_cache.Cache,
	sellingHttpPushHandler SellingPushHttpHandler,
	cacheMgr san_caches.CacheManager,
	orderClients OrderDependencyClients,
) RegisterHandler {

	return func() ServiceReflectNames {
		grpcReflects := ServiceReflectNames{}

		path, handler := selling_ifaceconnect.NewConfigurationLimitServiceHandler(
			services.NewConfigurationService(db, client, auth, cache),
			defaultInterceptor,
		)
		mux.Handle(path, handler)
		grpcReflects = append(grpcReflects, selling_ifaceconnect.ConfigurationLimitServiceName)

		path, handler = selling_ifaceconnect.NewSupplierServiceHandler(
			supplier.NewSupplierService(db),
			defaultInterceptor,
		)
		mux.Handle(path, handler)
		grpcReflects = append(grpcReflects, selling_ifaceconnect.SupplierServiceName)

		// v2 roling: enforce the (role_base.v1.request_policy) declared on each Shop
		// request message (team-scoped via use_scope). Per-handler option, so only
		// ShopService requests are gated.
		shopRoleOpt := connect.WithInterceptors(access_interceptors.NewAccessInterceptor(db, cfg.JwtSecret, cacheMgr))
		path, handler = selling_ifaceconnect.NewShopServiceHandler(
			shop.NewShopService(db),
			defaultInterceptor,
			shopRoleOpt,
		)
		mux.Handle(path, handler)
		grpcReflects = append(grpcReflects, selling_ifaceconnect.ShopServiceName)

		path, handler = selling_ifaceconnect.NewSellingStatServiceHandler(stat_service.NewSellingStatService(db, cacheMgr), defaultInterceptor)
		mux.Handle(path, handler)
		grpcReflects = append(grpcReflects, selling_ifaceconnect.SellingStatServiceName)

		// v2 roling: RevenueService requests are gated by their
		// (role_base.v1.request_policy) via the access interceptor, same as ShopService.
		revenueRoleOpt := connect.WithInterceptors(access_interceptors.NewAccessInterceptor(db, cfg.JwtSecret, cacheMgr))
		path, handler = selling_ifaceconnect.NewRevenueServiceHandler(
			revenue.NewRevenueService(db),
			defaultInterceptor,
			revenueRoleOpt,
		)
		mux.Handle(path, handler)
		grpcReflects = append(grpcReflects, selling_ifaceconnect.RevenueServiceName)

		// v3 OrderService (selling_iface.v3) — the next-gen order system (design:
		// plans/brainstroming.md). Team-scoped via the same v2 roling interceptor.
		orderRoleOpt := connect.WithInterceptors(access_interceptors.NewAccessInterceptor(db, cfg.JwtSecret, cacheMgr))
		path, handler = selling_ifacev3connect.NewOrderServiceHandler(
			order.NewOrderService(db, orderClients.Inventory, orderClients.Product, orderClients.Warehouse),
			defaultInterceptor,
			orderRoleOpt,
		)
		mux.Handle(path, handler)
		grpcReflects = append(grpcReflects, selling_ifacev3connect.OrderServiceName)

		mux.HandleFunc("/selling/push", sellingHttpPushHandler)

		return grpcReflects
	}
}
