//go:build wireinject
// +build wireinject

package main

import (
	"net/http"

	"github.com/google/wire"
	"github.com/pdcgo/san_collection/san_caches"
	"github.com/pdcgo/selling_service"
	"github.com/pdcgo/shared/configs"
	"github.com/pdcgo/shared/custom_connect"
	"github.com/urfave/cli/v3"
)

var environtment = wire.NewSet(
	NewCache,
	NewDatabase,
	NewFirestoreClient,
	NewRedisDatabase,
	san_caches.NewRedisCacheManager,
	NewBatchConfig,
)

func InitializeApp() (*cli.Command, error) {
	wire.Build(
		configs.NewProductionConfig,
		http.NewServeMux,
		custom_connect.NewRegisterReflect,
		environtment,
		custom_connect.NewDefaultInterceptor,
		NewAuthorization,
		selling_service.NewSellingPushHandler,
		selling_service.NewSellingPushHttpHandler,
		selling_service.NewRegister,

		NewBatchHandler,
		NewBatchFunc,
		NewServiceApiFunc,
		NewApp,
	)

	return &cli.Command{}, nil
}
