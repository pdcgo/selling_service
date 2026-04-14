package stat_service

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

type statServiceImpl struct {
	db *gorm.DB
}

// StatStream implements [selling_ifaceconnect.SellingStatServiceHandler].
func (s *statServiceImpl) StatStream(context.Context, *connect.Request[selling_iface.StatStreamRequest], *connect.ServerStream[selling_iface.StatStreamResponse]) error {
	panic("unimplemented")
}

func NewSellingStatService(db *gorm.DB) *statServiceImpl {
	return &statServiceImpl{db}
}
