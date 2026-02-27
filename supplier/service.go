package supplier

import (
	"gorm.io/gorm"
)

type supplierServiceImpl struct {
	db *gorm.DB
}

func NewSupplierService(db *gorm.DB) *supplierServiceImpl {
	return &supplierServiceImpl{
		db: db,
	}
}
