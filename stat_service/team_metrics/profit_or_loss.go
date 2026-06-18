package team_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/team_metric/v1"
	"gorm.io/gorm"
)

type profitOrLossMetric struct {
	db        *gorm.DB
	metricMap MetricMap
}

// FetchMetric implements [TeamMetricBase].
func (t *profitOrLossMetric) Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (query *gorm.DB, err error) {
	ocpQuery, err := t.metricMap.GetQuery(selling_iface.TeamMetricType_TEAM_METRIC_TYPE_PROFIT_ORDER_CREATED, ctx, tfilter)
	if err != nil {
		return nil, err
	}
	ocpQuery = ocpQuery.
		Select([]string{"sum(o.order_mp_total - o.total) as profit_created_amount", "o.team_id"}).
		Group("o.team_id")

	olQuery, err := t.metricMap.GetQuery(selling_iface.TeamMetricType_TEAM_METRIC_TYPE_ORDER_LOST, ctx, tfilter)
	if err != nil {
		return nil, err
	}
	olQuery = olQuery.
		Select([]string{"sum(o.total) as lost_amount", "o.team_id"}).
		Group("o.team_id")

	wdQuery, err := t.metricMap.GetQuery(selling_iface.TeamMetricType_TEAM_METRIC_TYPE_WITHDRAWAL, ctx, tfilter)
	if err != nil {
		return nil, err
	}
	wdQuery = wdQuery.
		Select([]string{"sum(oa.amount) filter (where oa.amount < 0) as adjustment_amount", "o.team_id"}).
		Group("o.team_id")

	aeQuery, err := t.metricMap.GetQuery(selling_iface.TeamMetricType_TEAM_METRIC_TYPE_ADS_EXPENSE, ctx, tfilter)
	if err != nil {
		return nil, err
	}
	aeQuery = aeQuery.
		Select([]string{"sum(aeh.amount) as ads_amount", "aeh.team_id"}).
		Group("aeh.team_id")

	query = t.
		db.
		Table("(?) ocp", ocpQuery).
		Joins("full join (?) ol on ol.team_id = ocp.team_id", olQuery).
		Joins("full join (?) wd on wd.team_id = ocp.team_id", wdQuery).
		Joins("full join (?) ae on ae.team_id = ocp.team_id", aeQuery)

	return
}

// FetchMetric implements [TeamMetricBase].
func (t *profitOrLossMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	var err error
	result := team_metric.TeamProfitOrLossMetric{
		Data: map[uint64]*team_metric.TeamProfitOrLossItem{},
	}

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	resultList := []*team_metric.TeamProfitOrLossItem{}
	err = query.
		Where("coalesce(ocp.team_id, ol.team_id, wd.team_id, ae.team_id)  IN ?", teamIds).
		Select([]string{
			"coalesce(ocp.team_id, ol.team_id, wd.team_id, ae.team_id) as team_id",
			"coalesce(ocp.profit_created_amount, 0) - coalesce(ol.lost_amount, 0) + coalesce(wd.adjustment_amount, 0) - coalesce(ae.ads_amount, 0) as profit_or_loss_amount",
		}).
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.TeamId] = item
	}

	return &selling_iface.TeamMetric{
		Data: &selling_iface.TeamMetric_TeamProfitOrLossMetric{
			TeamProfitOrLossMetric: &result,
		},
	}, err
}

// ProcessSort implements [TeamMetricBase].
func (t *profitOrLossMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64
	var sortfield string

	query, err := t.Query(ctx, tfilter)
	if err != nil {
		return nil, err
	}

	switch tsort.GetTeamProfitOrLossMetricSort() {
	case team_metric.TeamProfitOrLossMetricSort_TEAM_PROFIT_OR_LOSS_METRIC_SORT_PROFIT_OR_LOSS_AMOUNT:
		sortfield = "coalesce(ocp.profit_created_amount, 0) - coalesce(ol.lost_amount, 0) + coalesce(wd.adjustment_amount, 0) - coalesce(ae.ads_amount, 0) as sfield"
	}

	query = query.
		Select([]string{
			"coalesce(ocp.team_id, ol.team_id, wd.team_id, ae.team_id) as team_id", sortfield,
		})

	limit, offset := getLimitOffset(tfilter.Page)
	wquery := t.
		db.
		Table("(?) w", query).
		Where("w.team_id > 0").
		Select("w.team_id")

	switch tsort.GetSortType() {
	case selling_iface.TeamMetricSortType_TEAM_METRIC_SORT_TYPE_ASC:
		wquery = wquery.Order("w.sfield asc nulls last")
	case selling_iface.TeamMetricSortType_TEAM_METRIC_SORT_TYPE_DESC:
		wquery = wquery.Order("w.sfield desc nulls last")
	}

	err = wquery.
		Limit(limit).
		Offset(offset).
		Find(&ids).
		Error

	return ids, err
}

func NewProfitOrLossMetric(db *gorm.DB, metricMap MetricMap) TeamMetricBase {
	return &profitOrLossMetric{db, metricMap}
}
