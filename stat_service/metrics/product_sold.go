package metrics

import (
	"fmt"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

func NewProductSoldMetric(
	db *gorm.DB,
	filter *selling_iface.StatFilter,
	trange *selling_iface.TimeRange,
) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.ProductSoldMetric{
		Type: selling_iface.MetricType_METRIC_TYPE_PRODUCT_SOLD,
	}

	selects := []string{
		"sum(oi.count) as piece_count",
		"count(oi.order_id) as order_count",
		"sum(oi.total) as total_amount",
	}

	oquery := db.
		Table("order_items oi").
		Joins("JOIN orders o ON o.id = oi.order_id").
		Joins("JOIN products p ON p.id = oi.product_id").
		Where("o.status != 'cancel'")

	if filter.TeamId != 0 {
		oquery = oquery.Where("p.team_id = ?", filter.TeamId)
		selects = append(selects,
			fmt.Sprintf("sum(oi.count) filter (where o.team_id = %d) as own_piece_count", filter.TeamId),
			fmt.Sprintf("count(oi.order_id) filter (where o.team_id = %d) as own_order_count", filter.TeamId),
			fmt.Sprintf("sum(oi.total) filter (where o.team_id = %d) as own_total_amount", filter.TeamId),

			fmt.Sprintf("sum(oi.count) filter (where o.team_id != %d) as cross_piece_count", filter.TeamId),
			fmt.Sprintf("count(oi.order_id) filter (where o.team_id != %d) as cross_order_count", filter.TeamId),
			fmt.Sprintf("sum(oi.total) filter (where o.team_id != %d) as cross_total_amount", filter.TeamId),
		)

	}

	if trange.End.IsValid() {
		oquery = oquery.Where("o.created_at <= ?", trange.End.AsTime())
	}

	if trange.Start.IsValid() {
		oquery = oquery.Where("o.created_at >= ?", trange.Start.AsTime())
	}

	err = oquery.
		Select(selects).
		Find(&result).
		Error

	if err != nil {
		return nil, err
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_ProductSold{
			ProductSold: &result,
		},
	}, err
}

func NewHistoryProductSoldMetric(
	db *gorm.DB,
	filter *selling_iface.StatFilter,
	trange *selling_iface.TimeRange,
) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.HistoryProductSoldMetric{
		Type:     selling_iface.MetricType_METRIC_TYPE_HISTORY_PRODUCT_SOLD,
		TimeType: trange.Type,
		Datas:    []*selling_iface.HistoryProductSoldItem{},
	}

	selects := []string{}

	switch trange.Type {
	case selling_iface.TimeType_TIME_TYPE_DAY:
		selects = append(selects,
			"date_trunc('day', o.created_at) as t",
		)
	case selling_iface.TimeType_TIME_TYPE_WEEK:
		selects = append(selects,
			"date_trunc('week', o.created_at) as t",
		)
	case selling_iface.TimeType_TIME_TYPE_MONTH:
		selects = append(selects,
			"date_trunc('month', o.created_at) as t",
		)
	case selling_iface.TimeType_TIME_TYPE_YEAR:
		selects = append(selects,
			"date_trunc('year', o.created_at) as t",
		)
	}

	selects = append(selects,
		"sum(oi.count) as piece_count",
		"count(oi.order_id) as order_count",
		"sum(oi.total) as total_amount",
	)

	oquery := db.
		Table("order_items oi").
		Joins("JOIN orders o ON o.id = oi.order_id").
		Joins("JOIN products p ON p.id = oi.product_id").
		Where("o.status != 'cancel'")

	if filter.TeamId != 0 {
		oquery = oquery.Where("p.team_id = ?", filter.TeamId)
		selects = append(selects,
			fmt.Sprintf("sum(oi.count) filter (where o.team_id = %d) as own_piece_count", filter.TeamId),
			fmt.Sprintf("count(oi.order_id) filter (where o.team_id = %d) as own_order_count", filter.TeamId),
			fmt.Sprintf("sum(oi.total) filter (where o.team_id = %d) as own_total_amount", filter.TeamId),
			fmt.Sprintf("sum(oi.count) filter (where o.team_id != %d) as cross_piece_count", filter.TeamId),
			fmt.Sprintf("count(oi.order_id) filter (where o.team_id != %d) as cross_order_count", filter.TeamId),
			fmt.Sprintf("sum(oi.total) filter (where o.team_id != %d) as cross_total_amount", filter.TeamId),
		)

	}

	if trange.End.IsValid() {
		oquery = oquery.Where("o.created_at <= ?", trange.End.AsTime())
	}

	if trange.Start.IsValid() {
		oquery = oquery.Where("o.created_at >= ?", trange.Start.AsTime())
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
		Data: &selling_iface.Metric_HistoryProductSold{
			HistoryProductSold: &result,
		},
	}, err
}

func NewTopProductSoldMetric(
	db *gorm.DB,
	filter *selling_iface.StatFilter,
	trange *selling_iface.TimeRange,
) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.TopProductSoldMetric{
		Type:  selling_iface.MetricType_METRIC_TYPE_TOP_PRODUCT_SOLD,
		Items: []*selling_iface.SoldProductItem{},
	}

	// with d as (
	// 	select
	// 		oi.product_id,
	// 		count(oi.order_id) as order_count
	// 	from public.order_items oi
	// 	group by
	// 		oi.product_id
	// 	order by order_count desc
	// 	limit 10
	// )

	// select
	// 	d.*,
	// 	p.name,
	// 	p.ref_id
	// from d
	// left join public.products p on p.id = d.product_id

	oquery := db.
		Table("public.order_items oi").
		Joins("left join public.orders o on o.id = oi.order_id").
		Joins("left join public.products p on p.id = oi.product_id").
		Where("o.created_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if filter.TeamId != 0 {
		oquery = oquery.Where("p.team_id = ?", filter.TeamId)
	}

	oquery = oquery.
		Select([]string{
			"oi.product_id",
			"count(oi.order_id) as order_count",
			"sum(oi.count) as piece_count",
		}).
		Group("oi.product_id").
		Order("piece_count desc")

	if filter.TopN > 0 {
		oquery = oquery.Limit(int(filter.TopN))
	} else {
		oquery = oquery.Limit(10)
	}

	dquery := db.
		Table("(?) d", oquery).
		Joins("left join public.products p on p.id = d.product_id").
		Select([]string{
			"p.name",
			"p.image::json ->> 0 as image",
			"p.ref_id",
			"d.*",
		})

	err = dquery.Find(&result.Items).Error
	if err != nil {
		return nil, err
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_TopProductSold{
			TopProductSold: &result,
		},
	}, err
}

func NewTopProductUnsoldMetric(
	db *gorm.DB,
	filter *selling_iface.StatFilter,
	trange *selling_iface.TimeRange,
) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.TopProductUnsoldMetric{
		Type:  selling_iface.MetricType_METRIC_TYPE_TOP_PRODUCT_UNSOLD,
		Items: []*selling_iface.UnsoldProductItem{},
	}

	// with d as (
	// 	select
	// 		oi.product_id,
	// 		count(oi.order_id) as order_count
	// 	from public.order_items oi
	// 	group by
	// 		oi.product_id
	// 	order by order_count desc
	// 	limit 10
	// )

	// select
	// 	d.*,
	// 	p.name,
	// 	p.ref_id
	// from d
	// left join public.products p on p.id = d.product_id

	oquery := db.
		Table("public.order_items oi").
		Joins("left join public.orders o on o.id = oi.order_id").
		Joins("left join public.products p on p.id = oi.product_id").
		Where("o.created_at between ? and ?", trange.Start.AsTime(), trange.End.AsTime())

	if filter.TeamId != 0 {
		oquery = oquery.Where("p.team_id = ?", filter.TeamId)
	}

	oquery = oquery.
		Select([]string{
			"oi.product_id",
			"count(oi.order_id) as order_count",
		}).
		Group("oi.product_id").
		Order("order_count desc")

	if filter.TopN > 0 {
		oquery = oquery.Limit(int(filter.TopN))
	} else {
		oquery = oquery.Limit(10)
	}

	dquery := db.
		Table("(?) d", oquery).
		Joins("left join public.products p on p.id = d.product_id").
		Select([]string{
			"p.name",
			"p.image::json ->> 0 as image",
			"p.ref_id",
			"d.product_id",
			"d.order_count",
		})

	err = dquery.Find(&result.Items).Error
	if err != nil {
		return nil, err
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_TopProductUnsold{
			TopProductUnsold: &result,
		},
	}, err
}
