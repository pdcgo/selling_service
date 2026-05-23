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

type TeamCrossProduct struct {
	ID        uint      `gorm:"primaryKey"`
	TeamId    uint64    `gorm:"not null;default:0;uniqueIndex:uniq_tcp_team_product_shop_user"`
	ProductId uint64    `gorm:"not null;default:0;uniqueIndex:uniq_tcp_team_product_shop_user"`
	ShopId    uint64    `gorm:"not null;default:0;uniqueIndex:uniq_tcp_team_product_shop_user"`
	UserId    uint64    `gorm:"not null;default:0;uniqueIndex:uniq_tcp_team_product_shop_user"`
	CreatedAt time.Time `gorm:"not null;default:now()"`
}
