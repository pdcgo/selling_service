package selling_service

import (
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

type MigrationHandler func(db *gorm.DB) error

func NewMigrationHandler() MigrationHandler {
	return func(db *gorm.DB) error {

		err := db.AutoMigrate(
			&db_models.OweLimitConfiguration{},
			&db_models.Supplier{},
			&db_models.SupplierCustom{},
			&db_models.SupplierMarketplace{},
			&db_models.VariantSupplierV2{},
			&db_models.SupplierInvTxItemV2{},
			&db_models.RestockSupplierTemp{},
		)
		return err
	}
}
