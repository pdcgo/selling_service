package product_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/product_metric/v1"
	"github.com/pdcgo/selling_service/stat_service/metric_base"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

type productShipmentTimeMetric struct {
	db *gorm.DB
}

func NewProductShipmentTimeMetric(db *gorm.DB) metric_base.ProductMetricBase {
	return &productShipmentTimeMetric{db: db}
}

func (m *productShipmentTimeMetric) FetchMetric(ctx context.Context, productIds []uint64, pfilter *selling_iface.ProductStatMetricFilter) (*selling_iface.ProductMetric, error) {
	var err error

	result := product_metric.ProductShipmentTimeMetric{
		Data: map[uint64]*product_metric.ProductShipmentTimeItem{},
	}

	trange := pfilter.Range
	query := m.
		db.
		Table("inv_transactions it").
		Joins("left join inv_tx_items iti on iti.inv_transaction_id = it.id").
		Joins("left join skus s on s.id = iti.sku_id").
		Where("it.arrived is not null").
		Where("it.arrived between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Where("s.product_id in ?", productIds).
		Where("it.type in ?", []db_models.InvTxType{db_models.InvTxRestock, db_models.InvTxReturn})

	if pfilter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", pfilter.WarehouseId)
	}

	if pfilter.TeamId != 0 {
		query = query.Where("it.team_id = ?", pfilter.TeamId)
	}

	query = query.
		Select([]string{
			"s.product_id",
			"avg(EXTRACT(EPOCH FROM (it.arrived - it.created))) filter (where it.type = 'restock') as avg_restock_shipment_time",
			"avg(EXTRACT(EPOCH FROM (it.arrived - it.created))) filter (where it.type = 'return') as avg_return_shipment_time",
		}).
		Group("s.product_id")

	resultList := []*product_metric.ProductShipmentTimeItem{}

	err = query.
		Find(&resultList).
		Error

	if err != nil {
		return nil, err
	}

	for _, item := range resultList {
		result.Data[item.ProductId] = item
	}

	return &selling_iface.ProductMetric{
		Data: &selling_iface.ProductMetric_ProductShipmentTimeMetric{
			ProductShipmentTimeMetric: &result,
		},
	}, err
}

func (m *productShipmentTimeMetric) ProcessSort(ctx context.Context, pfilter *selling_iface.ProductStatMetricFilter, psort *selling_iface.ProductMetricSort) ([]uint64, error) {
	var err error
	var productIds []uint64
	var sortField string

	trange := pfilter.Range

	query := m.
		db.
		Table("inv_transactions it").
		Joins("left join inv_tx_items iti on iti.inv_transaction_id = it.id").
		Joins("left join skus s on s.id = iti.sku_id").
		Where("it.arrived is not null").
		Where("it.arrived between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Where("it.type in ?", []db_models.InvTxType{db_models.InvTxRestock, db_models.InvTxReturn})

	if pfilter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", pfilter.WarehouseId)
	}

	if pfilter.TeamId != 0 {
		query = query.Where("it.team_id = ?", pfilter.TeamId)
	}

	sortField = "avg(it.arrived - it.created) as sfield"

	switch psort.GetProductShipmentTimeMetricSort() {
	case product_metric.ProductShipmentTimeMetricSort_PRODUCT_SHIPMENT_TIME_METRIC_SORT_AVG_RESTOCK_SHIPMENT_TIME:
		query = query.
			Where("it.type = ?", db_models.InvTxRestock)
	case product_metric.ProductShipmentTimeMetricSort_PRODUCT_SHIPMENT_TIME_METRIC_SORT_AVG_RETURN_SHIPMENT_TIME:
		query = query.
			Where("it.type = ?", db_models.InvTxReturn)

	}

	query = query.
		Select("s.product_id", sortField).
		Group("s.product_id")

	wrapquery := m.db.
		Table("(?) w", query).
		Select("product_id")

	switch psort.SortType {
	case selling_iface.ProductMetricSortType_PRODUCT_METRIC_SORT_TYPE_ASC:
		wrapquery = wrapquery.Order("w.sfield asc nulls last")
	case selling_iface.ProductMetricSortType_PRODUCT_METRIC_SORT_TYPE_DESC:
		wrapquery = wrapquery.Order("w.sfield desc nulls last")
	}

	limit, offset := getLimitOffset(pfilter.Page)
	err = wrapquery.
		Limit(limit).
		Offset(offset).
		Find(&productIds).
		Error

	return productIds, err
}
