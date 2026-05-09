package product_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/stat_service/metric_base"
	"gorm.io/gorm"
)

// just for sorting

type productCommon struct {
	db *gorm.DB
}

func NewProductCommon(db *gorm.DB) metric_base.ProductMetricBase {
	return &productCommon{
		db: db,
	}
}

// FetchMetric implements [metric_base.ProductMetricBase].
func (p *productCommon) FetchMetric(ctx context.Context, productIds []uint64, pfilter *selling_iface.ProductStatMetricFilter) (*selling_iface.ProductMetric, error) {
	return nil, nil
}

// ProcessSort implements [metric_base.ProductMetricBase].
func (p *productCommon) ProcessSort(ctx context.Context, pfilter *selling_iface.ProductStatMetricFilter, psort *selling_iface.ProductMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64

	var sortField string

	limit, offset := getLimitOffset(pfilter.Page)

	query := p.db.
		Table("products p").
		Select("p.id").
		Limit(limit).
		Offset(offset)

	switch psort.GetCommonSort() {
	case selling_iface.CommonProductSort_COMMON_PRODUCT_SORT_NAME:
		sortField = "p.name"
	case selling_iface.CommonProductSort_COMMON_PRODUCT_SORT_REF_ID:
		sortField = "p.ref_id"
	case selling_iface.CommonProductSort_COMMON_PRODUCT_SORT_TEAMNAME:
		query = query.
			Joins("left join teams t on t.id = p.team_id")

		sortField = "t.name"
	}

	switch psort.GetSortType() {
	case selling_iface.ProductMetricSortType_PRODUCT_METRIC_SORT_TYPE_ASC:
		query = query.Order(sortField + " asc")
	case selling_iface.ProductMetricSortType_PRODUCT_METRIC_SORT_TYPE_DESC:
		query = query.Order(sortField + " desc")
	}

	// filtering data
	if pfilter.TeamId != 0 {
		query = query.Where("p.team_id = ?", pfilter.TeamId)
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

	err = query.
		Find(&ids).
		Error

	if err != nil {
		return nil, err
	}

	return ids, err

}

func getLimitOffset(page *common.PageFilter) (int, int) {

	if page == nil {
		return 100, 0
	}
	return int(page.Limit), int((page.Page - 1) * page.Limit)
}
