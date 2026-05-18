package selling_models

import "time"

type ShopWarehouse struct {
	ID          uint   `gorm:"primarykey"`
	ShopId      uint64 `gorm:"uniqueIndex:idx_shop_warehouse"`
	WarehouseId uint64 `gorm:"uniqueIndex:idx_shop_warehouse"`
	LastOrderAt time.Time
	CreatedAt   time.Time
}
