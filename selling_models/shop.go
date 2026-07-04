package selling_models

import (
	"time"

	"gorm.io/gorm"
)

// Shop is a team-owned marketplace shop, managed by selling_service ShopService.
// It maps to the existing `v2_shops` table (canonical goose migration
// db_migrations/00007_add_shop_model.sql).
//
// Soft delete uses gorm.DeletedAt; on delete the username is also renamed
// (`[username]_deleted_[unix]`) so the (team_id, shop_username) unique index can be
// reused after a shop is removed and re-created. The canonical migration makes the
// unique indexes PARTIAL (`WHERE deleted_at IS NULL`); the gorm tags below declare a
// plain composite unique used by AutoMigrate/tests.
type Shop struct {
	ID              uint64         `json:"id" gorm:"primaryKey;autoIncrement"`
	TeamID          uint64         `json:"team_id" gorm:"not null;index;uniqueIndex:uidx_v2_shop_username,priority:1"`
	ShopName        string         `json:"shop_name" gorm:"not null;size:200;default:''"`
	ShopUsername    string         `json:"shop_username" gorm:"not null;size:200;default:'';uniqueIndex:uidx_v2_shop_username,priority:2"`
	MarketplaceType int32          `json:"marketplace_type" gorm:"not null;default:0"`
	URI             string         `json:"uri" gorm:"size:500;default:''"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
	DeletedAt       gorm.DeletedAt `json:"-" gorm:"index"`
}

func (Shop) TableName() string { return "v2_shops" }

// ShopUser maps a user to a shop they may access (table `v2_shop_users`). Removing
// access soft-deletes the row.
type ShopUser struct {
	ID        uint64         `json:"id" gorm:"primaryKey;autoIncrement"`
	ShopID    uint64         `json:"shop_id" gorm:"not null;index;uniqueIndex:uidx_v2_shop_user,priority:1"`
	TeamID    uint64         `json:"team_id" gorm:"not null;index"`
	UserID    uint64         `json:"user_id" gorm:"not null;index;uniqueIndex:uidx_v2_shop_user,priority:2"`
	CreatedAt time.Time      `json:"created_at"`
	DeletedAt gorm.DeletedAt `json:"-" gorm:"index"`
}

func (ShopUser) TableName() string { return "v2_shop_users" }
