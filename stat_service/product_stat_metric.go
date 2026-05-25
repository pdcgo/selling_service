package stat_service

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/stat_service/metric_base"
	"github.com/pdcgo/selling_service/stat_service/product_metrics"
	"google.golang.org/protobuf/proto"
)

// ProductStatMetric implements [selling_ifaceconnect.SellingStatServiceHandler].
func (s *statServiceImpl) ProductStatMetric(
	ctx context.Context,
	req *connect.Request[selling_iface.ProductStatMetricRequest],
) (*connect.Response[selling_iface.ProductStatMetricResponse], error) {
	var err error
	result := selling_iface.ProductStatMetricResponse{
		Metrics: []*selling_iface.ProductMetric{},
		Ids:     []uint64{},
	}

	var defaultExpiration time.Duration = time.Minute

	db := s.db.WithContext(ctx)

	var sortbase metric_base.ProductMetricBase

	var sortFieldName string
	// processing sort
	switch sortField := req.Msg.Sort.S.(type) {
	case *selling_iface.ProductMetricSort_CommonSort:
		sortFieldName = sortField.CommonSort.String()
		sortbase = product_metrics.NewProductCommon(db)
	case *selling_iface.ProductMetricSort_ProductOrderMetricSort:
		sortFieldName = sortField.ProductOrderMetricSort.String()
		sortbase = product_metrics.NewProductOrderMetric(db)
	case *selling_iface.ProductMetricSort_RestockAcceptedMetricSort:
		sortFieldName = sortField.RestockAcceptedMetricSort.String()
		sortbase = product_metrics.NewRestockAcceptedMetric(db)
	case *selling_iface.ProductMetricSort_RestockCreatedMetricSort:
		sortFieldName = sortField.RestockCreatedMetricSort.String()
		sortbase = product_metrics.NewRestockCreatedMetric(db)
	case *selling_iface.ProductMetricSort_RestockCancelledMetricSort:
		sortFieldName = sortField.RestockCancelledMetricSort.String()
		sortbase = product_metrics.NewRestockCancelledMetric(db)
	case *selling_iface.ProductMetricSort_ReturnCreatedMetricSort:
		sortFieldName = sortField.ReturnCreatedMetricSort.String()
		sortbase = product_metrics.NewReturnCreatedMetric(db)
	case *selling_iface.ProductMetricSort_ReturnArrivedMetricSort:
		sortFieldName = sortField.ReturnArrivedMetricSort.String()
		sortbase = product_metrics.NewReturnArrivedMetric(db)
	case *selling_iface.ProductMetricSort_ReturnCancelledMetricSort:
		sortFieldName = sortField.ReturnCancelledMetricSort.String()
		sortbase = product_metrics.NewReturnCancelledMetric(db)
	case *selling_iface.ProductMetricSort_StockReadyMetricSort:
		sortFieldName = sortField.StockReadyMetricSort.String()
		sortbase = product_metrics.NewStockReadyMetric(db)
	case *selling_iface.ProductMetricSort_StockOngoingMetricSort:
		sortFieldName = sortField.StockOngoingMetricSort.String()
		sortbase = product_metrics.NewStockOngoingMetric(db)
	case *selling_iface.ProductMetricSort_ProductShipmentTimeMetricSort:
		sortFieldName = sortField.ProductShipmentTimeMetricSort.String()
		sortbase = product_metrics.NewProductShipmentTimeMetric(db)
	default:
		err = errors.New("invalid sort type")
		return nil, err
	}

	var resultIds resultKey

	sortCacheKey := &sortKey{
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
		var metric *selling_iface.ProductMetric = &selling_iface.ProductMetric{}
		var metricbase metric_base.ProductMetricBase

		switch metType {
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_ORDER:
			metricbase = product_metrics.NewProductOrderMetric(db)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_RESTOCK_ACCEPTED:
			metricbase = product_metrics.NewRestockAcceptedMetric(db)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_RESTOCK:
			metricbase = product_metrics.NewRestockCreatedMetric(db)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_RESTOCK_CANCEL:
			metricbase = product_metrics.NewRestockCancelledMetric(db)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_RETURN_ACCEPTED:
			metricbase = product_metrics.NewReturnArrivedMetric(db)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_RETURN:
			metricbase = product_metrics.NewReturnCreatedMetric(db)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_RETURN_CANCEL:
			metricbase = product_metrics.NewReturnCancelledMetric(db)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_STOCK_READY:
			metricbase = product_metrics.NewStockReadyMetric(db)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_STOCK_ONGOING:
			metricbase = product_metrics.NewStockOngoingMetric(db)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_SHIPMENT_TIME:
			metricbase = product_metrics.NewProductShipmentTimeMetric(db)
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

	return connect.NewResponse(&result), nil
}

type listKey struct {
	Ids        []uint64
	MetricName string
}

func (k *listKey) GetKey() (string, error) {
	hashedIds := md5.Sum([]byte(fmt.Sprintf("%v", k.Ids)))
	return fmt.Sprintf("metric:%s:%x", k.MetricName, hashedIds), nil
}

type sortKey struct {
	Filter        *selling_iface.ProductStatMetricFilter
	SortType      string
	SortFieldName string
}

func (k *sortKey) GetKey() (string, error) {

	bytes, err := proto.MarshalOptions{
		Deterministic: true,
	}.Marshal(k.Filter)

	if err != nil {
		return "", err
	}
	hashedIds := md5.Sum(bytes)
	return fmt.Sprintf("metric_sort:%s:%s:%x", k.SortFieldName, k.SortType, hashedIds), nil
}

type resultKey []uint64

// UnmarshalBinary implements [encoding.BinaryUnmarshaler].
func (r *resultKey) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, r)
}

// MarshalBinary implements [encoding.BinaryMarshaler].
func (r *resultKey) MarshalBinary() (data []byte, err error) {
	return json.Marshal(r)
}
