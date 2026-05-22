package shop_metrics

import (
	"context"

	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

type ShopMetricBase interface {
	ProcessSort(ctx context.Context, filter *selling_iface.ShopStatMetricFilter, sort *selling_iface.ShopMetricSort) ([]uint64, error)
	FetchMetric(ctx context.Context, ids []uint64, filter *selling_iface.ShopStatMetricFilter) (*selling_iface.ShopMetric, error)
}

type CommonShopMetric struct {
	db *gorm.DB
}

func NewCommonShopMetric(db *gorm.DB) ShopMetricBase {
	return &CommonShopMetric{
		db: db,
	}
}

func (s *CommonShopMetric) ProcessSort(ctx context.Context, filter *selling_iface.ShopStatMetricFilter, sort *selling_iface.ShopMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64

	var sortField string

	limit, offset := getLimitOffset(filter.Page)

	query := s.db.
		Table("marketplaces m").
		Select("m.id").
		Limit(limit).
		Offset(offset)

	switch sort.GetCommonSort() {
	case selling_iface.CommonShopSort_COMMON_SHOP_SORT_NAME:
		sortField = "m.mp_name"
	case selling_iface.CommonShopSort_COMMON_SHOP_SORT_USERNAME:
		sortField = "m.mp_username"
	}

	switch sort.GetSortType() {
	case selling_iface.ShopMetricSortType_SHOP_METRIC_SORT_TYPE_ASC:
		query = query.Order(sortField + " asc")
	case selling_iface.ShopMetricSortType_SHOP_METRIC_SORT_TYPE_DESC:
		query = query.Order(sortField + " desc")
	}

	// filtering data
	if filter.TeamId != 0 {
		query = query.Where("m.team_id = ?", filter.TeamId)
	}

	if filter.WarehouseId != 0 {
		skuQuery := s.db.
			Table("shop_warehouses sw").
			Where("sw.shop_id = m.id").
			Where("sw.warehouse_id = ?", filter.WarehouseId).
			Select("1")
		query = query.Where("exists (?)", skuQuery)
	}

	err = query.
		Find(&ids).
		Error

	if err != nil {
		return nil, err
	}

	return ids, err
}

func (s *CommonShopMetric) FetchMetric(ctx context.Context, ids []uint64, filter *selling_iface.ShopStatMetricFilter) (*selling_iface.ShopMetric, error) {
	return nil, nil
}

func getLimitOffset(page *common.PageFilter) (int, int) {

	if page == nil {
		return 100, 0
	}
	return int(page.Limit), int((page.Page - 1) * page.Limit)
}
