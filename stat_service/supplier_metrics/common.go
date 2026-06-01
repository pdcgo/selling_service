package supplier_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/supplier_metric/v1"
	"gorm.io/gorm"
)

type SupplierMetricBase interface {
	ProcessSort(ctx context.Context, sfilter *selling_iface.SupplierMetricFilter, ssort *selling_iface.SupplierMetricSort) ([]uint64, error)
	FetchMetric(ctx context.Context, productIds []uint64, pfilter *selling_iface.SupplierMetricFilter) (*selling_iface.SupplierMetric, error)
}

type commonMetric struct {
	db *gorm.DB
}

// FetchMetric implements [SupplierMetricBase].
func (c *commonMetric) FetchMetric(ctx context.Context, productIds []uint64, pfilter *selling_iface.SupplierMetricFilter) (*selling_iface.SupplierMetric, error) {
	return nil, nil
}

// ProcessSort implements [SupplierMetricBase].
func (c *commonMetric) ProcessSort(ctx context.Context, sfilter *selling_iface.SupplierMetricFilter, ssort *selling_iface.SupplierMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64

	query := c.db.
		Table("v2_suppliers vs").
		Select("vs.id")

	var sortField string
	switch ssort.GetCommonSort() {
	case supplier_metric.CommonSupplierSort_COMMON_SUPPLIER_SORT_NAME:
		sortField = "vs.name"
	case supplier_metric.CommonSupplierSort_COMMON_SUPPLIER_SORT_CODE:
		sortField = "vs.code"
	}

	switch ssort.GetSortType() {
	case selling_iface.SupplierMetricSortType_SUPPLIER_METRIC_SORT_TYPE_ASC:
		query = query.Order(sortField + " ASC")
	case selling_iface.SupplierMetricSortType_SUPPLIER_METRIC_SORT_TYPE_DESC:
		query = query.Order(sortField + " DESC")
	}

	limit, offset := getLimitOffset(sfilter.Page)
	query = query.
		Limit(limit).
		Offset(offset)

	err = query.
		Find(&ids).
		Error

	if err != nil {
		return nil, err
	}

	return ids, nil

}

func NewSupplierCommonMetric(db *gorm.DB) SupplierMetricBase {
	return &commonMetric{db}
}

func getLimitOffset(page *common.PageFilter) (int, int) {

	if page == nil {
		return 100, 0
	}
	return int(page.Limit), int((page.Page - 1) * page.Limit)
}
