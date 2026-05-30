package selling_models

import (
	"time"

	"github.com/pdcgo/schema/services/selling_iface/v1/stat_logs/v1"
)

type SupplierOrderLog struct {
	ID         uint                      `gorm:"primarykey"`
	LogType    stat_logs.SupplierLogType `gorm:"not null"`
	SupplierID uint64                    `gorm:"not null;index"`
	ProductID  uint64                    `gorm:"not null"`
	OrderID    uint64                    `gorm:"not null;index"`
	TeamID     uint64                    `gorm:"not null;index"`
	Count      int64                     `gorm:"not null;default:0"`
	Amount     float64                   `gorm:"not null;default:0"`
	CreatedAt  time.Time                 `gorm:"not null;default:now()"`
	EventAt    time.Time                 `gorm:"not null;index"`
}
