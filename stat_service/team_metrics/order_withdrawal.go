package team_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/team_metric/v1"
	"gorm.io/gorm"
)

type orderWithdrawalMetric struct {
	db *gorm.DB
}

func (t *orderWithdrawalMetric) Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (query *gorm.DB, err error) {
	trange := tfilter.Range
	withdrawals := t.
		db.
		Table("order_adjustments oa").
		Joins("join orders o on o.id = oa.order_id").
		Where("oa.fund_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Select("oa.order_id, max(oa.fund_at) as last_fund_at").
		Group("oa.order_id")

	query = t.
		db.
		Table("(?) w", withdrawals).
		Joins("join orders o on o.id = w.order_id")

	return
}

// FetchMetric implements [TeamMetricBase].
func (t *orderWithdrawalMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	var err error
	result := team_metric.TeamOrderWithdrawalMetric{
		Data: map[uint64]*team_metric.TeamOrderWithdrawalItem{},
	}
	resultList := []*team_metric.TeamOrderWithdrawalItem{}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	err = query.
		Where("o.team_id in ?", teamIds).
		Select([]string{
			"o.team_id",
			"count(o.id) as transaction_count",
			"sum(o.total) as total_amount",
			"sum(o.order_mp_total) as mp_total_amount",
			"max(w.last_fund_at) as last_order_withdrawal",
		}).
		Group("o.team_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.TeamId] = item
	}

	return &selling_iface.TeamMetric{
		Data: &selling_iface.TeamMetric_TeamOrderWithdrawalMetric{
			TeamOrderWithdrawalMetric: &result,
		},
	}, err
}

// ProcessSort implements [TeamMetricBase].
func (t *orderWithdrawalMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var teamIds []uint64
	var sortField string

	switch tsort.GetTeamOrderWithdrawalMetricSort() {
	case team_metric.TeamOrderWithdrawalMetricSort_TEAM_ORDER_WITHDRAWAL_METRIC_SORT_TRANSACTION_COUNT:
		sortField = "count(o.id) as sfield"
	case team_metric.TeamOrderWithdrawalMetricSort_TEAM_ORDER_WITHDRAWAL_METRIC_SORT_TOTAL_AMOUNT:
		sortField = "sum(o.total) as sfield"
	case team_metric.TeamOrderWithdrawalMetricSort_TEAM_ORDER_WITHDRAWAL_METRIC_SORT_MP_TOTAL_AMOUNT:
		sortField = "sum(o.order_mp_total) as sfield"
	case team_metric.TeamOrderWithdrawalMetricSort_TEAM_ORDER_WITHDRAWAL_METRIC_SORT_LAST_ORDER_WITHDRAWAL:
		sortField = "max(w.last_fund_at) as sfield"
	default:
		err = errors.New("team order withdrawal metric sort invalid sort type")
		return nil, err
	}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	query = query.
		Select("o.team_id", sortField).
		Group("o.team_id")

	wrapquery := t.db.
		Table("(?) w", query).
		Select("team_id")

	switch tsort.GetSortType() {
	case selling_iface.TeamMetricSortType_TEAM_METRIC_SORT_TYPE_ASC:
		wrapquery = wrapquery.Order("w.sfield asc nulls last")
	case selling_iface.TeamMetricSortType_TEAM_METRIC_SORT_TYPE_DESC:
		wrapquery = wrapquery.Order("w.sfield desc nulls last")
	}

	limit, offset := getLimitOffset(tfilter.Page)
	err = wrapquery.
		Limit(limit).
		Offset(offset).
		Find(&teamIds).
		Error

	return teamIds, err
}

func NewOrderWithdrawalMetric(db *gorm.DB) TeamMetricBase {
	return &orderWithdrawalMetric{db}
}
