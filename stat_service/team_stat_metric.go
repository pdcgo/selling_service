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
	"github.com/pdcgo/selling_service/stat_service/team_metrics"
	"google.golang.org/protobuf/proto"
)

// TeamStatMetric implements [selling_ifaceconnect.SellingStatServiceHandler].
func (s *statServiceImpl) TeamStatMetric(
	ctx context.Context,
	req *connect.Request[selling_iface.TeamStatMetricRequest],
) (*connect.Response[selling_iface.TeamStatMetricResponse], error) {
	var err error
	result := &selling_iface.TeamStatMetricResponse{
		Ids:     []uint64{},
		Metrics: []*selling_iface.TeamMetric{}, //error
	}

	var defaultExpiration time.Duration = time.Minute

	db := s.db.WithContext(ctx)

	var sortbase team_metrics.TeamMetricBase

	var sortFieldName string
	// processing sort
	switch sortField := req.Msg.Sort.S.(type) {
	case *selling_iface.TeamMetricSort_CommonSort:
		sortFieldName = sortField.CommonSort.String()
		sortbase = team_metrics.NewCommon(db)

	case *selling_iface.TeamMetricSort_TeamOrderMetricSort:
		sortFieldName = sortField.TeamOrderMetricSort.String()
		sortbase = team_metrics.NewOrderMetric(db)

	default:
		err = errors.New("invalid sort type")
		return nil, err
	}

	var resultIds resultKey

	sortCacheKey := &teamCrossSortKey{
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
		var metric *selling_iface.TeamMetric = &selling_iface.TeamMetric{}
		var metricbase team_metrics.TeamMetricBase

		switch metType {
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_ORDER:
			metricbase = team_metrics.NewOrderMetric(db)
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

type teamCrossSortKey struct {
	Filter        *selling_iface.TeamStatMetricFilter
	SortType      string
	SortFieldName string
}

func (k *teamCrossSortKey) GetKey() (string, error) {

	bytes, err := proto.MarshalOptions{
		Deterministic: true,
	}.Marshal(k.Filter)

	if err != nil {
		return "", err
	}
	hashedIds := md5.Sum(bytes)
	return fmt.Sprintf("metric_sort:%s:%s:%x", k.SortFieldName, k.SortType, hashedIds), nil
}
