package team_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/team_metric/v1"
	"gorm.io/gorm"
)

type profitOrderWithdrawalMetric struct {
	db *gorm.DB
}

// FetchMetric implements [TeamMetricBase].
func (t *profitOrderWithdrawalMetric) Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (query *gorm.DB, err error) {
	trange := tfilter.Range
	withdrawals := t.
		db.
		Table("order_adjustments oa").
		Joins("join orders o on o.id = oa.order_id").
		Where("oa.fund_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Select("oa.order_id, sum(oa.amount) as amount").
		Group("oa.order_id")

	query = t.
		db.
		Table("(?) w", withdrawals).
		Joins("join orders o on o.id = w.order_id")

	return
}

// FetchMetric implements [TeamMetricBase].
func (t *profitOrderWithdrawalMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	var err error
	result := team_metric.TeamProfitOrderWithdrawalMetric{
		Data: map[uint64]*team_metric.TeamProfitOrderWithdrawalItem{},
	}
	resultList := []*team_metric.TeamProfitOrderWithdrawalItem{}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	err = query.
		Where("o.team_id in ?", teamIds).
		Select([]string{
			"o.team_id",
			"sum(w.amount) - sum(o.total) as profit_withdrawal_amount",
			"(((sum(w.amount) - sum(o.total)) / nullif(sum(o.total), 0)) * 100) as profit_withdrawal_percent",
			"sum(w.amount) - sum(o.order_mp_total) as profit_withdrawal_adjustment_amount",
			"(((sum(w.amount) - sum(o.order_mp_total)) / nullif(sum(o.order_mp_total), 0)) * 100) as profit_withdrawal_adjustment_percent",
		}).
		Group("o.team_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.TeamId] = item
	}

	return &selling_iface.TeamMetric{
		Data: &selling_iface.TeamMetric_TeamProfitOrderWithdrawalMetric{
			TeamProfitOrderWithdrawalMetric: &result,
		},
	}, err
}

// ProcessSort implements [TeamMetricBase].
func (t *profitOrderWithdrawalMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var teamIds []uint64
	var sortField string

	switch tsort.GetTeamProfitOrderWithdrawalMetricSort() {
	case team_metric.TeamProfitOrderWithdrawalMetricSort_TEAM_PROFIT_ORDER_WITHDRAWAL_METRIC_SORT_PROFIT_WITHDRAWAL_AMOUNT:
		sortField = "sum(w.amount - o.total) as sfield"
	case team_metric.TeamProfitOrderWithdrawalMetricSort_TEAM_PROFIT_ORDER_WITHDRAWAL_METRIC_SORT_PROFIT_WITHDRAWAL_PERCENT:
		sortField = "((sum(w.amount - o.total) / nullif(sum(o.total), 0)) * 100) as sfield"
	case team_metric.TeamProfitOrderWithdrawalMetricSort_TEAM_PROFIT_ORDER_WITHDRAWAL_METRIC_SORT_PROFIT_WITHDRAWAL_ADJUSTMENT_AMOUNT:
		sortField = "sum(w.amount - o.order_mp_total) as sfield"
	case team_metric.TeamProfitOrderWithdrawalMetricSort_TEAM_PROFIT_ORDER_WITHDRAWAL_METRIC_SORT_PROFIT_WITHDRAWAL_ADJUSTMENT_PERCENT:
		sortField = "((sum(w.amount - o.order_mp_total) / nullif(sum(o.order_mp_total), 0)) * 100) as sfield"
	default:
		err = errors.New("team profit order withdrawal metric sort invalid sort type")
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

func NewProfitOrderWithdrawalMetric(db *gorm.DB) TeamMetricBase {
	return &profitOrderWithdrawalMetric{db}
}
