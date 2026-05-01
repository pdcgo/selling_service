package metrics

import (
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

func NewReadyStockMetric(db *gorm.DB, filter *selling_iface.StatFilter) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.ReadyStockMetric{
		Type: selling_iface.MetricType_METRIC_TYPE_READY_STOCK,
	}

	// select
	// 	sum(-1 * ih.count) as total_count,
	// 	count(ih.sku_id) as total_sku_count,
	// 	sum((-1 * ih.count) * (ih.price + coalesce(ih.ext_price, 0))) as total_amount
	// from public.invertory_histories ih
	// where
	// 	ih.tx_id is null

	readyQ := db.
		Table("public.invertory_histories ih").
		Where("ih.tx_id is null")

	if filter.TeamId != 0 {
		readyQ = readyQ.Where("ih.team_id = ?", filter.TeamId)
	}

	if filter.ProductFilter != nil {
		productFilter := filter.ProductFilter
		skuQuery := db.
			Table("skus s").
			Where("s.product_id = ?", productFilter.ProductId).
			Where("s.id = ih.sku_id").
			Select("1")
		readyQ = readyQ.Where("exists (?)", skuQuery)
	}

	selects := []string{
		"sum(-1 * ih.count) as total_count",
		"count(ih.sku_id) as total_sku_count",
		"sum((-1 * ih.count) * (ih.price + coalesce(ih.ext_price, 0))) as total_amount",
	}

	err = readyQ.
		Session(&gorm.Session{}).
		Select(selects).
		Find(&result).Error
	if err != nil {
		return nil, err
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_ReadyStock{
			ReadyStock: &result,
		},
	}, nil
}
