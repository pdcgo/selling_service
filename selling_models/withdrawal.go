package selling_models

import (
	"time"

	"gorm.io/gorm"
)

// Withdrawal is a team-owned, per-shop record of funds withdrawn from a marketplace,
// managed by selling_service RevenueService. It maps to the `v3_withdrawals` table
// (canonical goose migration db_migrations/00008_add_withdrawal_model.sql).
//
// Soft delete uses gorm.DeletedAt; on delete ref_id is renamed
// (`[ref_id]_deleted_[unix]`) so the (team_id, ref_id, source) unique index can be
// reused after a withdrawal is removed and re-created. The canonical migration makes
// the unique index PARTIAL (`WHERE deleted_at IS NULL`); the gorm tags below declare a
// plain composite unique used by AutoMigrate/tests.
type Withdrawal struct {
	ID        uint64         `json:"id" gorm:"primaryKey;autoIncrement"`
	ShopID    uint64         `json:"shop_id" gorm:"not null;index"`
	TeamID    uint64         `json:"team_id" gorm:"not null;index;uniqueIndex:uidx_v3_withdrawal_ref,priority:1"`
	Amount    float64        `json:"amount" gorm:"not null;default:0"`
	FundedAt  time.Time      `json:"funded_at"`
	RefID     string         `json:"ref_id" gorm:"not null;size:200;default:'';uniqueIndex:uidx_v3_withdrawal_ref,priority:2"`
	Source    int32          `json:"source" gorm:"not null;default:0;uniqueIndex:uidx_v3_withdrawal_ref,priority:3"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `json:"-" gorm:"index"`
}

func (Withdrawal) TableName() string { return "v3_withdrawals" }

// WithdrawalLog is the withdrawal document's change history (table
// `v3_withdrawal_logs`): one row per lifecycle event (created / change_amount /
// deleted), written in the same transaction as the event itself. Not soft-deleted —
// the history of a soft-deleted withdrawal is preserved.
type WithdrawalLog struct {
	ID           uint64    `json:"id" gorm:"primaryKey;autoIncrement"`
	WithdrawalID uint64    `json:"withdrawal_id" gorm:"not null;index"`
	ShopID       uint64    `json:"shop_id" gorm:"not null;index"`
	TeamID       uint64    `json:"team_id" gorm:"not null;index"`
	ChangeType   int32     `json:"change_type" gorm:"not null;default:0"`
	Amount       float64   `json:"amount" gorm:"not null;default:0"`
	CreatedAt    time.Time `json:"created_at"`
}

func (WithdrawalLog) TableName() string { return "v3_withdrawal_logs" }
