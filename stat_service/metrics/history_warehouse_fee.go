package metrics

import (
	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

func NewHistoryWarehouseFeeMetric(db *gorm.DB, filter *selling_iface.StatFilter, trange *common.StatTimeRange) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.HistoryWarehouseFeeMetric{
		TimeType: trange.Type,
		Items:    []*selling_iface.HistoryWarehouseFeeItem{},
	}

	selects := []string{
		"sum(o.warehouse_fee) as fee_amount",
		"count(o.id) as transaction_count",
	}

	switch trange.Type {
	case common.StatTimeType_STAT_TIME_TYPE_DAY:
		selects = append(selects, "date_trunc('day', o.created_at) as t")
	case common.StatTimeType_STAT_TIME_TYPE_WEEK:
		selects = append(selects, "date_trunc('week', o.created_at) as t")
	case common.StatTimeType_STAT_TIME_TYPE_MONTH:
		selects = append(selects, "date_trunc('month', o.created_at) as t")
	case common.StatTimeType_STAT_TIME_TYPE_YEAR:
		selects = append(selects, "date_trunc('year', o.created_at) as t")
	}

	query := db.
		Table("orders o").
		Where("o.created_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime()).
		Where("o.status != ?", db_models.OrdCancel)

	if filter.TeamId != 0 {
		query = query.Where("o.team_id = ?", filter.TeamId)
	}

	if filter.WarehouseId != 0 {
		query = query.Where("o.warehouse_id = ?", filter.WarehouseId)
	}

	if filter.MarketplaceType != selling_iface.MarketplaceType_MARKETPLACE_TYPE_UNSPECIFIED {
		query = query.
			Joins("left join marketplaces m on m.id = o.order_mp_id")

		switch filter.MarketplaceType {
		case selling_iface.MarketplaceType_MARKETPLACE_TYPE_CUSTOM:
			query = query.Where("m.mp_type = ?", "custom")
		case selling_iface.MarketplaceType_MARKETPLACE_TYPE_SHOPEE:
			query = query.Where("m.mp_type = ?", "shopee")
		case selling_iface.MarketplaceType_MARKETPLACE_TYPE_LAZADA:
			query = query.Where("m.mp_type = ?", "lazada")
		case selling_iface.MarketplaceType_MARKETPLACE_TYPE_TIKTOK:
			query = query.Where("m.mp_type = ?", "tiktok")
		case selling_iface.MarketplaceType_MARKETPLACE_TYPE_TOKOPEDIA:
			query = query.Where("m.mp_type = ?", "tokopedia")
		case selling_iface.MarketplaceType_MARKETPLACE_TYPE_MENGATAR:
			query = query.Where("m.mp_type = ?", "mengantar")
		}

	} else {
		if filter.ShopId != 0 {
			query.Where("o.order_mp_id = ?", filter.ShopId)
		}
	}

	err = query.
		Select(selects).
		Group("t").
		Order("t desc").
		Find(&result.Items).
		Error

	if err != nil {
		return nil, err
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_HistoryWarehouseFee{
			HistoryWarehouseFee: &result,
		},
	}, nil
}
