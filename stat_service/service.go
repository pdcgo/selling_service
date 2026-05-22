package stat_service

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/san_collection/san_caches"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

type statServiceImpl struct {
	db       *gorm.DB
	cacheMgr san_caches.CacheManager
}

// CrossProductList implements [selling_ifaceconnect.SellingStatServiceHandler].
func (s *statServiceImpl) CrossProductList(context.Context, *connect.Request[selling_iface.CrossProductListRequest]) (*connect.Response[selling_iface.CrossProductListResponse], error) {
	panic("unimplemented")
}

func NewSellingStatService(db *gorm.DB, cacheMgr san_caches.CacheManager) *statServiceImpl {
	return &statServiceImpl{db, cacheMgr}
}
