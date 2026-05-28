package shop_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/shop_metric/v1"
	"gorm.io/gorm"
)

type shopAdsExpenseMetric struct {
	db *gorm.DB
}

// FetchMetric implements [ShopMetricBase].
func (s *shopAdsExpenseMetric) FetchMetric(ctx context.Context, ids []uint64, filter *selling_iface.ShopStatMetricFilter) (*selling_iface.ShopMetric, error) {
	var err error

	result := shop_metric.ShopAdsExpenseMetric{
		Data: map[uint64]*shop_metric.ShopAdsExpenseItem{},
	}
	trange := filter.Range

	resultList := []*shop_metric.ShopAdsExpenseItem{}

	selects := []string{
		"axh.marketplace_id as shop_id",
		"sum(axh.amount) as ads_amount",
	}

	query := s.db.
		Table("ads_expense_histories axh").
		Where("axh.at between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Where("axh.marketplace_id in (?)", ids).
		Select(selects)

	if filter.TeamId != 0 {
		query = query.Where("axh.team_id = ?", filter.TeamId)
	}

	err = query.
		Group("axh.marketplace_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.ShopId] = item
	}

	return &selling_iface.ShopMetric{
		Data: &selling_iface.ShopMetric_ShopAdsExpenseMetric{
			ShopAdsExpenseMetric: &result,
		},
	}, err
}

// ProcessSort implements [ShopMetricBase].
func (s *shopAdsExpenseMetric) ProcessSort(ctx context.Context, filter *selling_iface.ShopStatMetricFilter, sort *selling_iface.ShopMetricSort) ([]uint64, error) {
	var err error
	var productIds []uint64
	var sortField string

	trange := filter.Range

	query := s.db.
		Table("ads_expense_histories axh").
		Where("axh.at between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if filter.TeamId != 0 {
		query = query.Where("axh.team_id = ?", filter.TeamId)
	}

	switch sort.GetShopAdsExpenseMetricSort() {
	case shop_metric.ShopAdsExpenseMetricSort_SHOP_ADS_EXPENSE_METRIC_SORT_ADS_AMOUNT:
		sortField = "sum(axh.amount) as sfield"
	}

	query = query.
		Select("axh.marketplace_id", sortField).
		Group("axh.marketplace_id")

	wrapquery := s.db.
		Table("(?) w", query).
		Select("marketplace_id")

	switch sort.SortType {
	case selling_iface.ShopMetricSortType_SHOP_METRIC_SORT_TYPE_ASC:
		wrapquery = wrapquery.Order("w.sfield asc nulls last")
	case selling_iface.ShopMetricSortType_SHOP_METRIC_SORT_TYPE_DESC:
		wrapquery = wrapquery.Order("w.sfield desc nulls last")
	}

	limit, offset := getLimitOffset(filter.Page)
	err = wrapquery.
		Limit(limit).
		Offset(offset).
		Find(&productIds).
		Error

	return productIds, err
}

func NewShopAdsExpenseMetric(db *gorm.DB) ShopMetricBase {
	return &shopAdsExpenseMetric{db}
}
