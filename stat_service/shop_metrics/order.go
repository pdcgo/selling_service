package shop_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

type shopOrderMetric struct {
	db *gorm.DB
}

// FetchMetric implements [ShopMetricBase].
func (s *shopOrderMetric) FetchMetric(ctx context.Context, ids []uint64, filter *selling_iface.ShopStatMetricFilter) (*selling_iface.ShopMetric, error) {
	panic("unimplemented")
}

// ProcessSort implements [ShopMetricBase].
func (s *shopOrderMetric) ProcessSort(ctx context.Context, filter *selling_iface.ShopStatMetricFilter, sort *selling_iface.ShopMetricSort) ([]uint64, error) {
	return nil, nil
}

func NewShopOrderMetric(db *gorm.DB) ShopMetricBase {
	return &shopOrderMetric{db}
}
