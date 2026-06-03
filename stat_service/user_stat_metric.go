package stat_service

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/stat_service/user_metrics"
	"google.golang.org/protobuf/proto"
)

// UserStatMetric implements [selling_ifaceconnect.SellingStatServiceHandler].
func (s *statServiceImpl) UserStatMetric(
	ctx context.Context,
	req *connect.Request[selling_iface.UserStatMetricRequest],
) (*connect.Response[selling_iface.UserStatMetricResponse], error) {
	var err error
	result := &selling_iface.UserStatMetricResponse{
		Ids:     []uint64{},
		Metrics: []*selling_iface.UserMetric{},
	}

	var defaultExpiration time.Duration = time.Minute

	db := s.db.WithContext(ctx)

	var sortbase user_metrics.UserMetricBase

	var sortFieldName string
	// processing sort
	switch sortField := req.Msg.Sort.S.(type) {
	case *selling_iface.UserMetricSort_CommonSort:
		sortFieldName = sortField.CommonSort.String()
		sortbase = user_metrics.NewUserCommon(db)

	case *selling_iface.UserMetricSort_UserOrderMetricSort:
		sortFieldName = sortField.UserOrderMetricSort.String()
		sortbase = user_metrics.NewUserOrderMetric(db)

	default:
		err = errors.New("invalid sort type")
		return nil, err
	}

	var resultIds resultKey

	sortCacheKey := &userSortKey{
		Filter:        req.Msg.Filter,
		SortFieldName: sortFieldName,
		SortType:      req.Msg.Sort.SortType.String(),
	}
	err = s.cacheMgr.Get(ctx, sortCacheKey, &resultIds)

	if err != nil {
		key, _ := sortCacheKey.GetKey()
		slog.Info("getting fresh sort", "sort_key", key, "err", err)
		resultIds, err = sortbase.ProcessSort(ctx, req.Msg.Filter, req.Msg.Sort)

		if err != nil {
			return nil, err
		}

		err = s.cacheMgr.Set(ctx, sortCacheKey, &resultIds, defaultExpiration)

		if err != nil {
			return nil, err
		}
	}
	result.Ids = resultIds

	for _, metType := range req.Msg.MetricTypes {
		var metric *selling_iface.UserMetric = &selling_iface.UserMetric{}
		var metricbase user_metrics.UserMetricBase

		switch metType {
		case selling_iface.UserMetricType_USER_METRIC_TYPE_ORDER:
			metricbase = user_metrics.NewUserOrderMetric(db)
		default:
			err = errors.New("invalid metric type")
		}

		err = s.cacheMgr.Get(ctx, &listKey{
			Ids:        result.Ids,
			MetricName: metType.String(),
		}, metric)

		if err == nil {
			result.Metrics = append(result.Metrics, metric)
			continue
		}

		slog.Info("getting fresh metric", "metricType", metType, "error", err)

		// jika cache tidak ada
		metric, err = metricbase.FetchMetric(ctx, result.Ids, req.Msg.Filter)
		if err != nil {
			return nil, err
		}

		err = s.cacheMgr.Set(ctx, &listKey{
			Ids:        result.Ids,
			MetricName: metType.String(),
		}, metric, defaultExpiration)

		if err != nil {
			return nil, err
		}

		result.Metrics = append(result.Metrics, metric)
	}

	return connect.NewResponse(result), err
}

type userSortKey struct {
	Filter        *selling_iface.UserStatMetricFilter
	SortType      string
	SortFieldName string
}

func (k *userSortKey) GetKey() (string, error) {

	bytes, err := proto.MarshalOptions{
		Deterministic: true,
	}.Marshal(k.Filter)

	if err != nil {
		return "", err
	}
	hashedIds := md5.Sum(bytes)
	return fmt.Sprintf("metric_sort:%s:%s:%x", k.SortFieldName, k.SortType, hashedIds), nil
}
