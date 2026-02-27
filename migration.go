package selling_service

import (
	"github.com/pdcgo/selling_service/supplier"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

type MigrationHandler func() error

func NewMigrationHandler(db *gorm.DB) MigrationHandler {
	return func() error {

		err := db.AutoMigrate(
			&db_models.OweLimitConfiguration{},
			&supplier.Supplier{},
			&supplier.SupplierCustom{},
			&supplier.SupplierMarketplace{},
		)
		return err
	}
}
