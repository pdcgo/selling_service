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
	ProcessSortQuery(
		ctx context.Context,
		pfilter *selling_iface.ProductCrossStatMetricFilter,
		psort *selling_iface.ProductCrossMetricSort,
		productIdsChan chan<- []uint64,
	) error
	FetchMetric(ctx context.Context, productIds []uint64, pfilter *selling_iface.ProductCrossStatMetricFilter) (*selling_iface.ProductCrossMetric, error)
	// WriteToRow(ctx context.Context, productIds []uint64, pfilter *selling_iface.ProductCrossStatMetricFilter) error
}

type CsvRow struct {
	mapper map[uint64][]string
}

func NewCsvRow() *CsvRow {
	return &CsvRow{
		mapper: make(map[uint64][]string),
	}
}

func (c *CsvRow) WriteToRow(id uint64, metricItem ProductCrossMetricBase) {

}

type ShopMetricBase interface {
	ProcessSort(ctx context.Context, sfilter *selling_iface.ShopStatMetricFilter, ssort *selling_iface.ShopMetricSort) ([]uint64, error)
	FetchMetric(ctx context.Context, shopIds []uint64, pfilter *selling_iface.ShopStatMetricFilter) (*selling_iface.ShopMetric, error)
}
