package team_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/team_metric/v1"
	"gorm.io/gorm"
)

type withdrawalBreakdownMetric struct {
	db *gorm.DB
}

// FetchMetric implements [TeamMetricBase].
func (t *withdrawalBreakdownMetric) Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (query *gorm.DB, err error) {
	trange := tfilter.Range
	query = t.
		db.
		Table("order_adjustments oa").
		Joins("join orders o on oa.order_id = o.id").
		Where("oa.fund_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	return
}

// FetchMetric implements [TeamMetricBase].
func (t *withdrawalBreakdownMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	var err error
	result := team_metric.TeamWithdrawalBreakdownMetric{
		Data: map[uint64]*team_metric.TeamWithdrawalBreakdownItem{},
	}
	resultList := []*team_metric.TeamWithdrawalBreakdownItem{}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	err = query.
		Where("o.team_id in ?", teamIds).
		Select([]string{
			"o.team_id",
			"sum(oa.amount) filter (where oa.type = 'aff_commission') as aff_commission_amount",
			"sum(oa.amount) filter (where oa.type = 'premi') as premi_amount",
			"sum(oa.amount) filter (where oa.type = 'packaging') as packaging_amount",
			"sum(oa.amount) filter (where oa.type = 'return_adj') as return_adj_amount",
			"sum(oa.amount) filter (where oa.type = 'shipping_adj') as shipping_adj_amount",
			"sum(oa.amount) filter (where oa.type = 'compensation') as compensation_amount",
			"sum(oa.amount) filter (where oa.type = 'lost_compensation') as lost_compensation_amount",
			"sum(oa.amount) filter (where oa.type = 'unknown') as unknown_amount",
			"sum(oa.amount) filter (where oa.type = 'order_fund') as order_fund_amount",
			"sum(oa.amount) filter (where oa.type = 'unknown_adj') as unknown_adj_amount",
			"sum(oa.amount) as total_amount",
		}).
		Group("o.team_id").
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.TeamId] = item
	}

	return &selling_iface.TeamMetric{
		Data: &selling_iface.TeamMetric_TeamWithdrawalBreakdownMetric{
			TeamWithdrawalBreakdownMetric: &result,
		},
	}, err
}

// ProcessSort implements [TeamMetricBase].
func (t *withdrawalBreakdownMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var teamIds []uint64
	var sortField string

	switch tsort.GetTeamWithdrawalBreakdownMetricSort() {
	case team_metric.TeamWithdrawalBreakdownMetricSort_TEAM_WITHDRAWAL_BREAKDOWN_METRIC_SORT_AFF_COMMISSION_AMOUNT:
		sortField = "sum(oa.amount) filter (where oa.type = 'aff_commission') as sfield"
	case team_metric.TeamWithdrawalBreakdownMetricSort_TEAM_WITHDRAWAL_BREAKDOWN_METRIC_SORT_PREMI_AMOUNT:
		sortField = "sum(oa.amount) filter (where oa.type = 'premi') as sfield"
	case team_metric.TeamWithdrawalBreakdownMetricSort_TEAM_WITHDRAWAL_BREAKDOWN_METRIC_SORT_PACKAGING_AMOUNT:
		sortField = "sum(oa.amount) filter (where oa.type = 'packaging') as sfield"
	case team_metric.TeamWithdrawalBreakdownMetricSort_TEAM_WITHDRAWAL_BREAKDOWN_METRIC_SORT_RETURN_ADJ_AMOUNT:
		sortField = "sum(oa.amount) filter (where oa.type = 'return_adj') as sfield"
	case team_metric.TeamWithdrawalBreakdownMetricSort_TEAM_WITHDRAWAL_BREAKDOWN_METRIC_SORT_SHIPPING_ADJ_AMOUNT:
		sortField = "sum(oa.amount) filter (where oa.type = 'shipping_adj') as sfield"
	case team_metric.TeamWithdrawalBreakdownMetricSort_TEAM_WITHDRAWAL_BREAKDOWN_METRIC_SORT_COMPENSATION_AMOUNT:
		sortField = "sum(oa.amount) filter (where oa.type = 'compensation') as sfield"
	case team_metric.TeamWithdrawalBreakdownMetricSort_TEAM_WITHDRAWAL_BREAKDOWN_METRIC_SORT_LOST_COMPENSATION_AMOUNT:
		sortField = "sum(oa.amount) filter (where oa.type = 'lost_compensation') as sfield"
	case team_metric.TeamWithdrawalBreakdownMetricSort_TEAM_WITHDRAWAL_BREAKDOWN_METRIC_SORT_UNKNOWN_AMOUNT:
		sortField = "sum(oa.amount) filter (where oa.type = 'unknown') as sfield"
	case team_metric.TeamWithdrawalBreakdownMetricSort_TEAM_WITHDRAWAL_BREAKDOWN_METRIC_SORT_ORDER_FUND_AMOUNT:
		sortField = "sum(oa.amount) filter (where oa.type = 'order_fund') as sfield"
	case team_metric.TeamWithdrawalBreakdownMetricSort_TEAM_WITHDRAWAL_BREAKDOWN_METRIC_SORT_UNKNOWN_ADJ_AMOUNT:
		sortField = "sum(oa.amount) filter (where oa.type = 'unknown_adj') as sfield"
	case team_metric.TeamWithdrawalBreakdownMetricSort_TEAM_WITHDRAWAL_BREAKDOWN_METRIC_SORT_TOTAL_AMOUNT:
		sortField = "sum(oa.amount) as sfield"
	default:
		err = errors.New("team withdrawal breakdown metric sort invalid sort type")
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

func NewWithdrawalBreakdownMetric(db *gorm.DB) TeamMetricBase {
	return &withdrawalBreakdownMetric{db}
}
