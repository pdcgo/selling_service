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
	"github.com/pdcgo/selling_service/stat_service/supplier_metrics"
	"google.golang.org/protobuf/proto"
)

// SupplierStatMetric implements [selling_ifaceconnect.SellingStatServiceHandler].
func (s *statServiceImpl) SupplierStatMetric(
	ctx context.Context,
	req *connect.Request[selling_iface.SupplierStatMetricRequest],
) (*connect.Response[selling_iface.SupplierStatMetricResponse], error) {

	var err error
	result := &selling_iface.SupplierStatMetricResponse{
		Metrics: []*selling_iface.SupplierMetric{},
		Ids:     []uint64{},
	}

	var defaultExpiration time.Duration = time.Minute * 5

	db := s.db.WithContext(ctx)

	var sortbase supplier_metrics.SupplierMetricBase
	var sortFieldName string

	// processing sort
	switch sortField := req.Msg.Sort.S.(type) {
	case *selling_iface.SupplierMetricSort_CommonSort:
		sortFieldName = sortField.CommonSort.String()
		sortbase = supplier_metrics.NewSupplierCommonMetric(db)

	case *selling_iface.SupplierMetricSort_SupplierOrderMetricSort:
		sortFieldName = sortField.SupplierOrderMetricSort.String()
		sortbase = supplier_metrics.NewSupplierOrderMetric(db)

	default:
		err = errors.New("invalid sort type")
		return nil, err
	}

	var resultIds resultKey

	sortCacheKey := &supplierSortKey{
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
		var metric *selling_iface.SupplierMetric = &selling_iface.SupplierMetric{}
		var metricbase supplier_metrics.SupplierMetricBase

		switch metType {
		case selling_iface.SupplierMetricType_SUPPLIER_METRIC_TYPE_COMMON:
			metricbase = supplier_metrics.NewSupplierCommonMetric(db)
		case selling_iface.SupplierMetricType_SUPPLIER_METRIC_TYPE_SUPPLIER_ORDER:
			metricbase = supplier_metrics.NewSupplierOrderMetric(db)
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

type supplierSortKey struct {
	Filter        *selling_iface.SupplierMetricFilter
	SortType      string
	SortFieldName string
}

func (k *supplierSortKey) GetKey() (string, error) {

	bytes, err := proto.MarshalOptions{
		Deterministic: true,
	}.Marshal(k.Filter)

	if err != nil {
		return "", err
	}
	hashedIds := md5.Sum(bytes)
	return fmt.Sprintf("metric_sort:%s:%s:%x", k.SortFieldName, k.SortType, hashedIds), nil
}
