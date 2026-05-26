package product_cross_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/product_cross_metric/v1"
	"github.com/pdcgo/selling_service/stat_service/metric_base"
	"gorm.io/gorm"
)

// just for sorting

type productCommon struct {
	db *gorm.DB
}

func NewProductCommon(db *gorm.DB) metric_base.ProductCrossMetricBase {
	return &productCommon{
		db: db,
	}
}

// ProcessSortQuery implements [metric_base.ProductCrossMetricBase].
func (p *productCommon) ProcessSortQuery(
	ctx context.Context,
	pfilter *selling_iface.ProductCrossStatMetricFilter,
	psort *selling_iface.ProductCrossMetricSort,
	productIdsChan chan<- []uint64,
) error {
	var err error

	query, err := p.sortQuery(ctx, pfilter, psort, false)
	if err != nil {
		return err
	}

	rows, err := query.Rows()
	if err != nil {
		return err
	}

	defer rows.Close()

	var ids []uint64
	for rows.Next() {
		var id uint64
		err = rows.Scan(&id)
		if err != nil {
			return err
		}

		ids = append(ids, id)
		if len(ids) >= 150 {
			productIdsChan <- ids
			ids = []uint64{}
		}
	}

	if len(ids) > 0 {
		productIdsChan <- ids
	}

	return err

}

// FetchMetric implements [metric_base.ProductMetricBase].
func (p *productCommon) FetchMetric(ctx context.Context, productIds []uint64, pfilter *selling_iface.ProductCrossStatMetricFilter) (*selling_iface.ProductCrossMetric, error) {
	return nil, nil
}

// ProcessSort implements [metric_base.ProductMetricBase].
func (p *productCommon) ProcessSort(ctx context.Context, pfilter *selling_iface.ProductCrossStatMetricFilter, psort *selling_iface.ProductCrossMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64

	query, err := p.sortQuery(ctx, pfilter, psort, true)
	if err != nil {
		return nil, err
	}

	err = query.
		Find(&ids).
		Error

	if err != nil {
		return nil, err
	}

	return ids, err

}

func (p *productCommon) sortQuery(
	_ context.Context,
	pfilter *selling_iface.ProductCrossStatMetricFilter,
	psort *selling_iface.ProductCrossMetricSort,
	useLimit bool,
) (*gorm.DB, error) {

	var sortField string

	query := p.db.
		Table("products p").
		Select("p.id")

	if useLimit {
		limit, offset := getLimitOffset(pfilter.Page)
		query = query.
			Limit(limit).
			Offset(offset)
	}

	switch psort.GetCommonSort() {
	case product_cross_metric.CommonProductCrossSort_COMMON_PRODUCT_CROSS_SORT_NAME:
		sortField = "p.name"
	case product_cross_metric.CommonProductCrossSort_COMMON_PRODUCT_CROSS_SORT_REF_ID:
		sortField = "p.ref_id"
	}

	switch psort.GetSortType() {
	case selling_iface.ProductCrossMetricSortType_PRODUCT_CROSS_METRIC_SORT_TYPE_ASC:
		query = query.Order(sortField + " asc")
	case selling_iface.ProductCrossMetricSortType_PRODUCT_CROSS_METRIC_SORT_TYPE_DESC:
		query = query.Order(sortField + " desc")
	}

	// filtering data
	if pfilter.TeamId != 0 {
		subCross := p.
			db.
			Table("team_cross_products cp").
			Where("cp.team_id = ?", pfilter.TeamId).
			Where("cp.product_id = p.id").
			Select("1")
		query = query.Where("exists (?)", subCross)
	}

	if pfilter.WarehouseId != 0 {
		skuQuery := p.db.
			Table("skus s").
			Where("s.product_id = p.id").
			Where("s.warehouse_id = ?", pfilter.WarehouseId).
			Select("1")
		query = query.Where("exists (?)", skuQuery)
	}

	if pfilter.ProductName != "" {
		search := "%" + pfilter.ProductName + "%"
		query = query.Where("p.name ilike ?", search)
	}
	return query, nil
}

func getLimitOffset(page *common.PageFilter) (int, int) {

	if page == nil {
		return 100, 0
	}
	return int(page.Limit), int((page.Page - 1) * page.Limit)
}
