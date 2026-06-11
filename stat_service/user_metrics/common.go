package user_metrics

import (
	"context"
	"errors"

	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"gorm.io/gorm"
)

type UserMetricBase interface {
	Query(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (*gorm.DB, error)
	ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error)
	FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error)
}

type MetricMap map[selling_iface.UserMetricType]UserMetricBase

func (mm MetricMap) GetMetric(mtype selling_iface.UserMetricType) (metric UserMetricBase, err error) {
	metric = mm[mtype]
	if mm[mtype] == nil {
		err = errors.New("metric:required " + mtype.String())
	}
	return
}

func (mm MetricMap) GetQuery(mtype selling_iface.UserMetricType, ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (query *gorm.DB, err error) {
	metric, err := mm.GetMetric(mtype)
	if mm[mtype] == nil {
		return
	}
	query, err = metric.Query(ctx, ufilter)
	return
}

type userCommon struct {
	db *gorm.DB
}

// FetchMetric implements [UserMetricBase].
func (u *userCommon) Query(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter) (*gorm.DB, error) {
	panic("unimplemented")
}

// FetchMetric implements [UserMetricBase].
func (u *userCommon) FetchMetric(ctx context.Context, userIds []uint64, ufilter *selling_iface.UserStatMetricFilter) (*selling_iface.UserMetric, error) {
	panic("unimplemented")
}

// ProcessSort implements [UserMetricBase].
func (u *userCommon) ProcessSort(ctx context.Context, ufilter *selling_iface.UserStatMetricFilter, usort *selling_iface.UserMetricSort) ([]uint64, error) {
	var err error
	var ids []uint64

	var sortField string

	query := u.
		db.
		Table("users u").
		Select("u.id")

	switch usort.GetCommonSort() {
	case selling_iface.CommonUserSort_COMMON_USER_SORT_NAME:
		sortField = "u.name"
	case selling_iface.CommonUserSort_COMMON_USER_SORT_USERNAME:
		sortField = "u.username"
	}

	switch usort.GetSortType() {
	case selling_iface.UserMetricSortType_USER_METRIC_SORT_TYPE_DESC:
		query = query.Order(sortField + " desc")
	case selling_iface.UserMetricSortType_USER_METRIC_SORT_TYPE_ASC:
		query = query.Order(sortField + " asc")
	}

	if ufilter.TeamId != 0 {
		teamQuery := u.
			db.
			Table("user_teams ut").
			Where("ut.team_id = ?", ufilter.TeamId).
			Where("u.id = ut.user_id").
			Select("1")

		query = query.Where("EXISTS (?)", teamQuery)
	}

	limit, offset := getLimitOffset(ufilter.Page)
	query = query.
		Limit(limit).
		Offset(offset)

	err = query.
		Find(&ids).
		Error

	if err != nil {
		return nil, err
	}

	return ids, nil
}

func NewUserCommon(db *gorm.DB) UserMetricBase {
	return &userCommon{db: db}
}

func getLimitOffset(page *common.PageFilter) (int, int) {

	if page == nil {
		return 100, 0
	}
	return int(page.Limit), int((page.Page - 1) * page.Limit)
}
