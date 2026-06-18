package team_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/team_metric/v1"
	"gorm.io/gorm"
)

type withdrawalMetric struct {
	db *gorm.DB
}

// FetchMetric implements [TeamMetricBase].
func (t *withdrawalMetric) Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (query *gorm.DB, err error) {
	trange := tfilter.Range
	query = t.
		db.
		Table("order_adjustments oa").
		Joins("join orders o on oa.order_id = o.id").
		Where("oa.fund_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	return
}

// FetchMetric implements [TeamMetricBase].
func (t *withdrawalMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	var err error
	result := team_metric.TeamWithdrawalMetric{
		Data: map[uint64]*team_metric.TeamWithdrawalItem{},
	}
	resultList := []*team_metric.TeamWithdrawalItem{}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	err = query.
		Where("o.team_id in ?", teamIds).
		Select([]string{
			"o.team_id",
			"sum(oa.amount) filter (where oa.amount > 0) as withdrawal_fund_amount",
			"sum(oa.amount) filter (where oa.amount < 0) as withdrawal_adjustment_amount",
			"sum(oa.amount) as withdrawal_total_amount",
		}).
		Group("o.team_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.TeamId] = item
	}

	return &selling_iface.TeamMetric{
		Data: &selling_iface.TeamMetric_TeamWithdrawalMetric{
			TeamWithdrawalMetric: &result,
		},
	}, err
}

// ProcessSort implements [TeamMetricBase].
func (t *withdrawalMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var teamIds []uint64
	var sortField string

	switch tsort.GetTeamWithdrawalMetricSort() {
	case team_metric.TeamWithdrawalMetricSort_TEAM_WITHDRAWAL_METRIC_SORT_WITHDRAWAL_FUND_AMOUNT:
		sortField = "sum(oa.amount) filter (where oa.amount > 0) as sfield"
	case team_metric.TeamWithdrawalMetricSort_TEAM_WITHDRAWAL_METRIC_SORT_WITHDRAWAL_ADJUSTMENT_AMOUNT:
		sortField = "sum(oa.amount) filter (where oa.amount < 0) as sfield"
	case team_metric.TeamWithdrawalMetricSort_TEAM_WITHDRAWAL_METRIC_SORT_WITHDRAWAL_TOTAL_AMOUNT:
		sortField = "sum(oa.amount) as sfield"
	default:
		err = errors.New("team withdrawal metric sort invalid sort type")
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

func NewWithdrawalMetric(db *gorm.DB) TeamMetricBase {
	return &withdrawalMetric{db}
}
