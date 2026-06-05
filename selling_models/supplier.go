package selling_models

import "time"

type SupplierReview struct {
	ID         uint64    `gorm:"primarykey"`
	SupplierID uint64    `gorm:"not null;index"`
	TeamID     uint64    `gorm:"not null;index"`
	UserID     uint64    `gorm:"not null;index"`
	Review     string    `gorm:"not null"`
	Rating     uint64    `gorm:"not null"`
	CreatedAt  time.Time `gorm:"not null;default:now()"`
	UpdatedAt  time.Time `gorm:"not null;default:now()"`
}
