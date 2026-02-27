package selling_service

import (
	"github.com/pdcgo/selling_service/supplier"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

type MigrationHandler func(db *gorm.DB) error

func NewMigrationHandler() MigrationHandler {
	return func(db *gorm.DB) error {

		err := db.AutoMigrate(
			&db_models.OweLimitConfiguration{},
			&supplier.Supplier{},
			&supplier.SupplierCustom{},
			&supplier.SupplierMarketplace{},
		)
		return err
	}
}
