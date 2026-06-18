package team_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

type TeamMetricBase interface {
	Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (*gorm.DB, error)
	ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error)
	FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error)
}

type MetricMap map[selling_iface.TeamMetricType]TeamMetricBase

func (mm MetricMap) GetMetric(mtype selling_iface.TeamMetricType) (metric TeamMetricBase, err error) {
	metric = mm[mtype]
	if mm[mtype] == nil {
		err = errors.New("metric:required " + mtype.String())
	}
	return
}

func (mm MetricMap) GetQuery(mtype selling_iface.TeamMetricType, ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (query *gorm.DB, err error) {
	metric, err := mm.GetMetric(mtype)
	if mm[mtype] == nil {
		return
	}
	query, err = metric.Query(ctx, tfilter)
	return
}

type commonMetric struct {
	db *gorm.DB
}

// FetchMetric implements [TeamMetricBase].
func (u *commonMetric) Query(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter) (*gorm.DB, error) {
	panic("unimplemented")
}

// FetchMetric implements [TeamMetricBase].
func (c *commonMetric) FetchMetric(ctx context.Context, teamIds []uint64, tfilter *selling_iface.TeamStatMetricFilter) (*selling_iface.TeamMetric, error) {
	return nil, nil
}

// ProcessSort implements [TeamMetricBase].
func (c *commonMetric) ProcessSort(ctx context.Context, tfilter *selling_iface.TeamStatMetricFilter, tsort *selling_iface.TeamMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64

	var sortField string

	query := c.db.
		Table("teams t").
		Select("t.id")

	switch tsort.GetCommonSort() {
	case selling_iface.CommonTeamSort_COMMON_TEAM_SORT_NAME:
		sortField = "t.name"

	default:
		err = errors.New("team common sort invalid sort type")
		return nil, err

	}

	switch tsort.GetSortType() {
	case selling_iface.TeamMetricSortType_TEAM_METRIC_SORT_TYPE_ASC:
		query = query.Order(sortField + " asc nulls last")
	case selling_iface.TeamMetricSortType_TEAM_METRIC_SORT_TYPE_DESC:
		query = query.Order(sortField + " desc nulls last")
	}

	limit, offset := getLimitOffset(tfilter.Page)
	err = query.
		Limit(limit).
		Offset(offset).
		Pluck("team_id", &ids).
		Error

	return ids, err
}

func NewCommon(db *gorm.DB) TeamMetricBase {
	return &commonMetric{db}
}

func getLimitOffset(page *common.PageFilter) (int, int) {

	if page == nil {
		return 100, 0
	}
	return int(page.Limit), int((page.Page - 1) * page.Limit)
}
