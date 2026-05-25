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
	"github.com/pdcgo/selling_service/stat_service/metric_base"
	"github.com/pdcgo/selling_service/stat_service/product_cross_metrics"
	"google.golang.org/protobuf/proto"
)

// ProductCrossStatMetric implements [selling_ifaceconnect.SellingStatServiceHandler].
func (s *statServiceImpl) ProductCrossStatMetric(
	ctx context.Context,
	req *connect.Request[selling_iface.ProductCrossStatMetricRequest],
) (*connect.Response[selling_iface.ProductCrossStatMetricResponse], error) {
	var err error
	result := &selling_iface.ProductCrossStatMetricResponse{
		Ids:     []uint64{},
		Metrics: []*selling_iface.ProductCrossMetric{},
	}

	var defaultExpiration time.Duration = time.Minute

	db := s.db.WithContext(ctx)

	var sortbase metric_base.ProductCrossMetricBase

	var sortFieldName string
	// processing sort
	switch sortField := req.Msg.Sort.S.(type) {
	case *selling_iface.ProductCrossMetricSort_CommonSort:
		sortFieldName = sortField.CommonSort.String()
		sortbase = product_cross_metrics.NewProductCommon(db)

	case *selling_iface.ProductCrossMetricSort_CostProductMetricSort:
		sortFieldName = sortField.CostProductMetricSort.String()
		sortbase = product_cross_metrics.NewCostProductMetric(db)

	default:
		err = errors.New("invalid sort type")
		return nil, err
	}

	var resultIds resultKey

	sortCacheKey := &productCrossSortKey{
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
		var metric *selling_iface.ProductCrossMetric = &selling_iface.ProductCrossMetric{}
		var metricbase metric_base.ProductCrossMetricBase

		switch metType {
		case selling_iface.ProductCrossMetricType_PRODUCT_CROSS_METRIC_TYPE_PRODUCT_COST:
			metricbase = product_cross_metrics.NewCostProductMetric(db)
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

type productCrossSortKey struct {
	Filter        *selling_iface.ProductCrossStatMetricFilter
	SortType      string
	SortFieldName string
}

func (k *productCrossSortKey) GetKey() (string, error) {

	bytes, err := proto.MarshalOptions{
		Deterministic: true,
	}.Marshal(k.Filter)

	if err != nil {
		return "", err
	}
	hashedIds := md5.Sum(bytes)
	return fmt.Sprintf("metric_sort:%s:%s:%x", k.SortFieldName, k.SortType, hashedIds), nil
}
