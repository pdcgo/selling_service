package team_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/team_metric/v1"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

type orderMetric struct {
	db *gorm.DB
}

// FetchMetric implements [TeamMetricBase].
func (o *orderMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	var err error
	result := team_metric.TeamOrderMetric{
		Data: map[uint64]*team_metric.TeamOrderItem{},
	}
	resultList := []*team_metric.TeamOrderItem{}

	trange := tfilter.Range

	baseQuery := o.
		db.
		Table("orders o").
		Where("o.created_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Where("o.status != ?", db_models.OrdCancel).
		Where("o.team_id in ?", teamIds)

	itemQuery := baseQuery.
		Session(&gorm.Session{}).
		Joins("left join order_items oi on oi.order_id = o.id").
		Select("o.team_id", "sum(oi.count) as piece_count").
		Group("o.team_id")

	selects := []string{
		"o.team_id",
		"count(o.id) as transaction_count",
		"sum(o.total) as total_amount",
		"sum(o.total) / count(o.id) as average_order_value",
	}

	orderQuery := baseQuery.
		Session(&gorm.Session{}).
		Select(selects).
		Group("o.team_id")

	query := o.
		db.
		Table("(?) as ot", orderQuery).
		Joins("left join (?) as oi on ot.team_id = oi.team_id", itemQuery).
		Select("*")

	err = query.
		Find(&resultList).
		Error

	for _, item := range resultList {
		result.Data[item.TeamId] = item
	}

	return &selling_iface.TeamMetric{
		Data: &selling_iface.TeamMetric_TeamOrderMetric{
			TeamOrderMetric: &result,
		},
	}, err
}

// ProcessSort implements [TeamMetricBase].
func (o *orderMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var teamIds []uint64
	var sortField string

	trange := tfilter.Range

	query := o.
		db.
		Table("orders o").
		Where("o.created_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Where("o.status != ?", db_models.OrdCancel)

	// o.team_id as team_id,
	// count(o.id) as transaction_count,
	// sum(o.total) as total_amount

	switch tsort.GetTeamOrderMetricSort() {
	case team_metric.TeamOrderMetricSort_TEAM_ORDER_METRIC_SORT_AVERAGE_ORDER_VALUE:
		sortField = "sum(o.total) / count(o.id) as sfield"
	case team_metric.TeamOrderMetricSort_TEAM_ORDER_METRIC_SORT_TRANSACTION_COUNT:
		sortField = "count(o.id) as sfield"
	case team_metric.TeamOrderMetricSort_TEAM_ORDER_METRIC_SORT_PIECE_COUNT:
		query = query.
			Joins("left join order_items oi on oi.order_id = o.id")
		sortField = "sum(oi.count) as sfield"
	case team_metric.TeamOrderMetricSort_TEAM_ORDER_METRIC_SORT_TOTAL_AMOUNT:
		sortField = "sum(o.total) as sfield"
	default:
		err = errors.New("team order metric sort invalid sort type")
		return nil, err
	}

	query = query.
		Select("o.team_id", sortField).
		Group("o.team_id")

	wrapquery := o.db.
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

func NewOrderMetric(db *gorm.DB) TeamMetricBase {
	return &orderMetric{db}
}
