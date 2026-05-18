package shop_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

type ShopMetricBase interface {
	ProcessSort(ctx context.Context, filter *selling_iface.ShopStatMetricFilter, sort *selling_iface.ShopMetricSort) ([]uint64, error)
	FetchMetric(ctx context.Context, ids []uint64, filter *selling_iface.ShopStatMetricFilter) (*selling_iface.ShopMetric, error)
}

type CommonShopMetric struct {
	db *gorm.DB
}

func NewCommonShopMetric(db *gorm.DB) ShopMetricBase {
	return &CommonShopMetric{
		db: db,
	}
}

func (s *CommonShopMetric) ProcessSort(ctx context.Context, filter *selling_iface.ShopStatMetricFilter, sort *selling_iface.ShopMetricSort) ([]uint64, error) {
	return nil, nil
}

func (s *CommonShopMetric) FetchMetric(ctx context.Context, ids []uint64, filter *selling_iface.ShopStatMetricFilter) (*selling_iface.ShopMetric, error) {
	return nil, nil
}
