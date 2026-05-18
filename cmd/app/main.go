package main

import (
	"context"
	"os"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"cloud.google.com/go/firestore"
	"github.com/pdcgo/shared/authorization"
	"github.com/pdcgo/shared/configs"
	"github.com/pdcgo/shared/db_connect"
	"github.com/pdcgo/shared/interfaces/authorization_iface"
	"github.com/pdcgo/shared/pkg/cloud_logging"
	"github.com/pdcgo/shared/pkg/ware_cache"
	"github.com/urfave/cli/v3"
	"gorm.io/gorm"
)

func NewCache(cfg *configs.AppConfig) (ware_cache.Cache, error) {
	return ware_cache.NewCustomCache(cfg.CacheService.Endpoint), nil
}

func NewCloudTaskClient() (*cloudtasks.Client, error) {
	return cloudtasks.NewClient(context.Background())
}

func NewFirestoreClient() (*firestore.Client, error) {
	return firestore.NewClient(context.Background(), os.Getenv("GOOGLE_CLOUD_PROJECT"))
}

func NewAuthorization(
	cfg *configs.AppConfig,
	db *gorm.DB,
	cache ware_cache.Cache,
) authorization_iface.Authorization {
	return authorization.NewAuthorization(cache, db, cfg.JwtSecret)
}

func NewDatabase(cfg *configs.AppConfig) (*gorm.DB, error) {
	return db_connect.NewProductionDatabase("selling_service", &cfg.Database)
}

func NewApp(

	serviceApiFunc ServiceApiFunc,
) *cli.Command {

	return &cli.Command{
		Name:   "run",
		Action: cli.ActionFunc(serviceApiFunc),
		Commands: []*cli.Command{
			{
				Name: "prepare-stat",
			},
		},
	}

}

func main() {
	if os.Getenv("DISABLE_CLOUD_LOGGING") == "" {
		cloud_logging.SetCloudLoggingDefault()
	}
	app, err := InitializeApp()
	if err != nil {
		panic(err)
	}

	err = app.Run(context.Background(), os.Args)
	if err != nil {
		panic(err)
	}
}
