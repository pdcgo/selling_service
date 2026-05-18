package selling_models

import "time"

type ShopWarehouse struct {
	ID          uint   `gorm:"primarykey"`
	ShopId      uint64 `gorm:"uniqueIndex:idx_shop_warehouse_user"`
	WarehouseId uint64 `gorm:"uniqueIndex:idx_shop_warehouse_user"`
	UserId      uint64 `gorm:"uniqueIndex:idx_shop_warehouse_user"`
	LastOrderAt time.Time
	CreatedAt   time.Time
}
