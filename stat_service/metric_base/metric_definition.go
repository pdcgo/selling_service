package metric_base

import (
	"context"
	"time"

	"github.com/pdcgo/schema/services/selling_iface/v1"
)

type ProductMetricBase interface {
	ProcessSort(ctx context.Context, pfilter *selling_iface.ProductStatMetricFilter, psort *selling_iface.ProductMetricSort) ([]uint64, error)
	FetchMetric(ctx context.Context, productIds []uint64, pfilter *selling_iface.ProductStatMetricFilter) (*selling_iface.ProductMetric, error)
}

type SortCacheManagerConfig struct {
	ExpiredDuration time.Duration
}

type SortCacheManager interface {
	GetSortCache(pfilter *selling_iface.ProductStatMetricFilter, psort *selling_iface.ProductMetricSort) ([]uint64, error)
	SetSortCache(pfilter *selling_iface.ProductStatMetricFilter, psort *selling_iface.ProductMetricSort, productIds []uint64) error
}
