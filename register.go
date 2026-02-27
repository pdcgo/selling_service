package selling_service

import (
	"net/http"

	"cloud.google.com/go/firestore"
	"github.com/pdcgo/schema/services/selling_iface/v1/selling_ifaceconnect"
	"github.com/pdcgo/selling_service/services"
	"github.com/pdcgo/selling_service/supplier"
	"github.com/pdcgo/shared/custom_connect"
	"github.com/pdcgo/shared/interfaces/authorization_iface"
	"github.com/pdcgo/shared/pkg/ware_cache"
	"gorm.io/gorm"
)

type ServiceReflectNames []string
type RegisterHandler func() ServiceReflectNames

func NewRegister(
	mux *http.ServeMux,
	db *gorm.DB,
	auth authorization_iface.Authorization,
	defaultInterceptor custom_connect.DefaultInterceptor,
	client *firestore.Client,
	cache ware_cache.Cache,
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

		return grpcReflects
	}
}
