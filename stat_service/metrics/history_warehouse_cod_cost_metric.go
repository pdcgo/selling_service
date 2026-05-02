package metrics

import (
	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

func NewHistoryWarehouseCodCostMetric(db *gorm.DB, filter *selling_iface.StatFilter, trange *common.StatTimeRange) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.HistoryWarehouseCodCostMetric{
		TimeType: trange.Type,
		Items:    []*selling_iface.HistoryWarehouseCodCostItem{},
	}

	var selects []string = []string{
		"sum(rc.cod_fee) as cost_amount",
		"count(it.id) as transaction_count",
	}

	switch trange.Type {
	case common.StatTimeType_STAT_TIME_TYPE_DAY:
		selects = append(selects,
			"date_trunc('day', it.arrived) as t",
		)
	case common.StatTimeType_STAT_TIME_TYPE_WEEK:
		selects = append(selects,
			"date_trunc('week', it.arrived) as t",
		)
	case common.StatTimeType_STAT_TIME_TYPE_MONTH:
		selects = append(selects,
			"date_trunc('month', it.arrived) as t",
		)
	case common.StatTimeType_STAT_TIME_TYPE_YEAR:
		selects = append(selects,
			"date_trunc('year', it.arrived) as t",
		)
	}

	query := db.
		Debug().
		Table("restock_costs rc").
		Joins("join inv_transactions it on it.id = rc.inv_transaction_id").
		Where("rc.cod_fee > 0").
		Where("it.arrived between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if filter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", filter.WarehouseId)
	}

	if filter.TeamId != 0 {
		query = query.Where("it.team_id = ?", filter.TeamId)
	}

	err = query.
		Select(selects).
		Group("t").
		Order("t asc").
		Find(&result.Items).
		Error

	if err != nil {
		return nil, err
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_HistoryWarehouseCodCost{
			HistoryWarehouseCodCost: &result,
		},
	}, nil

}
