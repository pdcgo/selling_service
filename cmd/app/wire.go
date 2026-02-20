//go:build wireinject
// +build wireinject

package main

import (
	"net/http"

	"github.com/google/wire"
	"github.com/pdcgo/selling_service"
	"github.com/pdcgo/shared/configs"
	"github.com/pdcgo/shared/custom_connect"
)

func InitializeApp() (*App, error) {
	wire.Build(
		configs.NewProductionConfig,
		http.NewServeMux,
		custom_connect.NewRegisterReflect,
		NewCache,
		NewDatabase,
		NewFirestoreClient,
		custom_connect.NewDefaultInterceptor,
		NewAuthorization,
		selling_service.NewRegister,
		NewApp,
	)

	return &App{}, nil
}
