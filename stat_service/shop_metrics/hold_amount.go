package shop_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/shop_metric/v1"
	"gorm.io/gorm"
)

type shopHoldAmountMetric struct {
	db *gorm.DB
}

// FetchMetric implements [ShopMetricBase].
func (s *shopHoldAmountMetric) FetchMetric(ctx context.Context, ids []uint64, filter *selling_iface.ShopStatMetricFilter) (*selling_iface.ShopMetric, error) {
	var err error

	result := shop_metric.ShopHoldAmountMetric{
		Data: map[uint64]*shop_metric.ShopHoldAmountItem{},
	}
	trange := filter.Range

	resultList := []*shop_metric.ShopHoldAmountItem{}

	selects := []string{
		"dsh.shop_id",
		"sum(dsh.hold_count) as transaction_count",
		"sum(dsh.hold_amount) as hold_amount",
	}

	query := s.db.
		Table("stats.daily_shop_holds dsh").
		Joins("left join marketplaces m on m.id = dsh.shop_id").
		Where("dsh.day between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Where("dsh.shop_id in (?)", ids).
		Select(selects)

	if filter.TeamId != 0 {
		query = query.Where("m.team_id = ?", filter.TeamId)
	}

	err = query.
		Group("dsh.shop_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.ShopId] = item
	}

	return &selling_iface.ShopMetric{
		Data: &selling_iface.ShopMetric_ShopHoldAmountMetric{
			ShopHoldAmountMetric: &result,
		},
	}, err
}

// ProcessSort implements [ShopMetricBase].
func (s *shopHoldAmountMetric) ProcessSort(ctx context.Context, filter *selling_iface.ShopStatMetricFilter, sort *selling_iface.ShopMetricSort) ([]uint64, error) {
	var err error
	var productIds []uint64
	var sortField string

	trange := filter.Range

	query := s.db.
		Table("stats.daily_shop_holds dsh").
		Joins("left join marketplaces m on m.id = dsh.shop_id").
		Where("dsh.day between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if filter.TeamId != 0 {
		query = query.Where("m.team_id = ?", filter.TeamId)
	}

	switch sort.GetShopHoldAmountMetricSort() {
	case shop_metric.ShopHoldAmountMetricSort_SHOP_HOLD_AMOUNT_METRIC_SORT_TRANSACTION_COUNT:
		sortField = "sum(dsh.hold_count) as sfield"
	case shop_metric.ShopHoldAmountMetricSort_SHOP_HOLD_AMOUNT_METRIC_SORT_HOLD_AMOUNT:
		sortField = "sum(dsh.hold_amount) as sfield"
	}

	query = query.
		Select("dsh.shop_id", sortField).
		Group("dsh.shop_id")

	wrapquery := s.db.
		Table("(?) w", query).
		Select("shop_id")

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

func NewShopHoldAmountMetric(db *gorm.DB) ShopMetricBase {
	return &shopHoldAmountMetric{db}
}
