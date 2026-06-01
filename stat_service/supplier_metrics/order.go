package supplier_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/supplier_metric/v1"
	"gorm.io/gorm"
)

type orderMetric struct {
	db *gorm.DB
}

// FetchMetric implements [SupplierMetricBase].
func (c *orderMetric) FetchMetric(ctx context.Context, supplierIds []uint64, pfilter *selling_iface.SupplierMetricFilter) (*selling_iface.SupplierMetric, error) {
	var err error
	result := &supplier_metric.SupplierOrderMetric{
		Data: map[uint64]*supplier_metric.SupplierOrderItem{},
	}

	resultList := []*supplier_metric.SupplierOrderItem{}

	selects := []string{
		"ol.supplier_id",
		"count(ol.order_id) as transaction_count",
		"sum(ol.count) as piece_count",
		"sum(ol.amount) as total_amount",
	}

	query := c.
		db.
		Table("supplier_order_logs ol").
		Where("ol.event_at between ? and ?", pfilter.Range.Start.AsTime(), pfilter.Range.End.AsTime()).
		Where("ol.supplier_id in ?", supplierIds)

	err = query.
		Select(selects).
		Group("ol.supplier_id").
		Find(&resultList).
		Error

	if err != nil {
		return nil, err
	}

	for _, item := range resultList {
		result.Data[item.SupplierId] = item
	}

	return &selling_iface.SupplierMetric{
		Data: &selling_iface.SupplierMetric_SupplierOrderMetric{
			SupplierOrderMetric: result,
		},
	}, nil
}

// ProcessSort implements [SupplierMetricBase].
func (c *orderMetric) ProcessSort(ctx context.Context, sfilter *selling_iface.SupplierMetricFilter, ssort *selling_iface.SupplierMetricSort) ([]uint64, error) {
	var err error
	var sortField string
	var supplierIds []uint64

	query := c.
		db.
		Table("supplier_order_logs ol").
		Where("ol.event_at between ? and ?", sfilter.Range.Start.AsTime(), sfilter.Range.End.AsTime())

	switch ssort.GetSupplierOrderMetricSort() {
	case supplier_metric.SupplierOrderMetricSort_SUPPLIER_ORDER_METRIC_SORT_TRANSACTION_COUNT:
		sortField = "count(ol.order_id) as sfield"
	case supplier_metric.SupplierOrderMetricSort_SUPPLIER_ORDER_METRIC_SORT_TOTAL_AMOUNT:
		sortField = "sum(ol.amount) as sfield"
	case supplier_metric.SupplierOrderMetricSort_SUPPLIER_ORDER_METRIC_SORT_PIECE_COUNT:
		sortField = "sum(ol.count) as sfield"
	}

	query = query.
		Select("ol.supplier_id", sortField).
		Group("ol.supplier_id")

	wrapper := c.db.
		Table("(?) w", query).
		Select("w.supplier_id")

	switch ssort.GetSortType() {
	case selling_iface.SupplierMetricSortType_SUPPLIER_METRIC_SORT_TYPE_ASC:
		wrapper = wrapper.Order("w.sfield asc nulls last")
	case selling_iface.SupplierMetricSortType_SUPPLIER_METRIC_SORT_TYPE_DESC:
		wrapper = wrapper.Order("w.sfield desc nulls last")
	}

	limit, offset := getLimitOffset(sfilter.Page)
	wrapper = wrapper.Limit(limit).Offset(offset)

	err = wrapper.Pluck("supplier_id", &supplierIds).Error

	if err != nil {
		return nil, err
	}

	return supplierIds, nil
}

func NewSupplierOrderMetric(db *gorm.DB) SupplierMetricBase {
	return &orderMetric{db}
}
