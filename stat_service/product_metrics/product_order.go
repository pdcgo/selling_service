package product_metrics

import (
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

func NewProductOrderMetric(db *gorm.DB, filter *selling_iface.ProductStatMetricFilter, trange *selling_iface.ProductStatTimeRange) (*selling_iface.ProductMetric, error) {
	var err error
	result := selling_iface.ProductOrderMetric{
		Items: []*selling_iface.ProductOrderMetricItem{},
	}

	selects := []string{
		"s.product_id as product_id",
		"count(iti.inv_transaction_id) as transaction_count",
		"sum(iti.count) as piece_count",
		"sum(iti.total) as piece_amount",

		"count(iti.inv_transaction_id) filter (where it.team_id = s.team_id) as own_transaction_count",
		"sum(iti.count) filter (where it.team_id = s.team_id) as own_piece_count",
		"sum(iti.total) filter (where it.team_id = s.team_id) as own_piece_amount",

		"count(iti.inv_transaction_id) filter (where it.team_id != s.team_id) as cross_transaction_count",
		"sum(iti.count) filter (where it.team_id != s.team_id) as cross_piece_count",
		"sum(iti.total) filter (where it.team_id != s.team_id) as cross_piece_amount",
	}

	query := db.
		Table("inv_tx_items iti").
		Joins("left join inv_transactions it on it.id = iti.inv_transaction_id").
		Joins("left join skus s on s.id = iti.sku_id").
		Where("it.status != 'cancel'").
		Where("it.type = 'order'").
		Where("it.created between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if filter.WarehouseId != 0 {
		query = query.Where("it.warehouse_id = ?", filter.WarehouseId)
	}

	if filter.TeamId != 0 {
		query = query.Where("s.team_id = ?", filter.TeamId)
	}

	limit := filter.Page.Limit
	offset := (filter.Page.Page - 1) * filter.Page.Limit

	err = query.
		Select(selects).
		Group("product_id").
		Limit(int(limit)).
		Offset(int(offset)).
		Find(&result.Items).
		Error

	return &selling_iface.ProductMetric{
		Data: &selling_iface.ProductMetric_OrderMetric{
			OrderMetric: &result,
		},
	}, err

}
