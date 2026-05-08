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

// CrossProductList implements [selling_ifaceconnect.SellingStatServiceHandler].
func (s *statServiceImpl) CrossProductList(context.Context, *connect.Request[selling_iface.CrossProductListRequest]) (*connect.Response[selling_iface.CrossProductListResponse], error) {
	panic("unimplemented")
}

func NewSellingStatService(db *gorm.DB) *statServiceImpl {
	return &statServiceImpl{db}
}
