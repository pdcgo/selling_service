package stat_service

import (
	"gorm.io/gorm"
)

type statServiceImpl struct {
	db *gorm.DB
}

func NewSellingStatService(db *gorm.DB) *statServiceImpl {
	return &statServiceImpl{db}
}
