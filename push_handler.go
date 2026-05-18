package selling_service

import (
	"context"

	"github.com/pdcgo/event_source"
	"gorm.io/gorm"
)

type SellingPushHandler event_source.PushHandler

func NewSellingPushHandler(dbx *gorm.DB, eventSender event_source.EventSender) SellingPushHandler {
	return func(ctx context.Context, msg *event_source.PushRequest) error {
		return nil
	}
}
