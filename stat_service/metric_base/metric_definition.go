package metric_base

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
)

type ProductMetricBase interface {
	ProcessSort(ctx context.Context, pfilter *selling_iface.ProductStatMetricFilter, psort *selling_iface.ProductMetricSort) ([]uint64, error)
	FetchMetric(ctx context.Context, productIds []uint64, pfilter *selling_iface.ProductStatMetricFilter) (*selling_iface.ProductMetric, error)
}

type ProductCrossMetricBase interface {
	ProcessSort(ctx context.Context, pfilter *selling_iface.ProductCrossStatMetricFilter, psort *selling_iface.ProductCrossMetricSort) ([]uint64, error)
	FetchMetric(ctx context.Context, productIds []uint64, pfilter *selling_iface.ProductCrossStatMetricFilter) (*selling_iface.ProductCrossMetric, error)
}
