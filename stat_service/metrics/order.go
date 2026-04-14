package metrics

import (
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

func NewOrderActiveMetric(
	db *gorm.DB,
	filter *selling_iface.StatFilter,
) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.OrderActiveMetric{
		Type: selling_iface.MetricType_METRIC_TYPE_ORDER_ACTIVE,
	}

	orderQuery := db.
		Table("orders o").
		Where("o.team_id = ?", filter.TeamId).
		Where("o.status NOT IN (?)", []db_models.OrdStatus{db_models.OrdCancel, db_models.OrdReturnCompleted, db_models.OrdCompleted, db_models.OrdReturnProblem}).
		Where("o.is_partial IS NULL OR o.is_partial = ?", false).
		Where("o.is_order_fake IS NULL OR o.is_order_fake = ?", false)

	selects := []string{"count(o.id) AS order_count"}
	err = orderQuery.
		Select(selects).
		Find(&result).
		Error

	if err != nil {
		return nil, err
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_OrderActive{
			OrderActive: &result,
		},
	}, err
}

func NewHistoryOrderMetric(
	db *gorm.DB,
	filter *selling_iface.StatFilter,
	trange *selling_iface.TimeRange,
) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.HistoryOrderMetric{
		Type:     selling_iface.MetricType_METRIC_TYPE_HISTORY_ORDER,
		TimeType: trange.Type,
		Datas:    []*selling_iface.HistoryOrderItem{},
	}

	oquery := db.
		Table("public.orders o").
		Where("o.status != ?", db_models.OrdCancel).
		Where("o.is_partial != ?", true).
		Where("o.is_order_fake != ?", true)

	selects := []string{
		"count(o.id) AS total_count",
		"sum(o.total) AS total_amount",
	}

	switch trange.Type {
	case selling_iface.TimeType_TIME_TYPE_DAY:
		selects = append(selects, "DATE_TRUNC('day', o.created_at) AS t")
	case selling_iface.TimeType_TIME_TYPE_WEEK:
		selects = append(selects, "DATE_TRUNC('week', o.created_at) AS t")
	case selling_iface.TimeType_TIME_TYPE_MONTH:
		selects = append(selects, "DATE_TRUNC('month', o.created_at) AS t")
	case selling_iface.TimeType_TIME_TYPE_YEAR:
		selects = append(selects, "DATE_TRUNC('year', o.created_at) AS t")
	}

	if trange.Start != nil && trange.End != nil {
		oquery = oquery.Where("o.created_at BETWEEN ? AND ?", trange.Start.AsTime(), trange.End.AsTime())
	}

	if filter.TeamId != 0 {
		oquery = oquery.Where("o.team_id = ?", filter.TeamId)
	}

	err = oquery.
		Select(selects).
		Group("t").
		Find(&result.Datas).
		Error

	if err != nil {
		return nil, err
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_HistoryOrder{
			HistoryOrder: &result,
		},
	}, err
}
