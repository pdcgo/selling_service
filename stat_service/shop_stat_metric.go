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
	"github.com/pdcgo/selling_service/stat_service/shop_metrics"
	"google.golang.org/protobuf/proto"
)

// ShopStatMetric implements [selling_ifaceconnect.SellingStatServiceHandler].
func (s *statServiceImpl) ShopStatMetric(ctx context.Context, req *connect.Request[selling_iface.ShopStatMetricRequest]) (*connect.Response[selling_iface.ShopStatMetricResponse], error) {
	var err error
	result := selling_iface.ShopStatMetricResponse{
		Metrics: []*selling_iface.ShopMetric{},
		Ids:     []uint64{},
	}

	db := s.db.WithContext(ctx)

	var defaultExpiration time.Duration = time.Minute
	var sortbase metric_base.ShopMetricBase
	var sortFieldName string

	switch sortField := req.Msg.Sort.S.(type) {
	case *selling_iface.ShopMetricSort_CommonSort:
		sortFieldName = sortField.CommonSort.String()
		sortbase = shop_metrics.NewCommonShopMetric(db)
	case *selling_iface.ShopMetricSort_ShopOrderMetricSort:
		sortFieldName = sortField.ShopOrderMetricSort.String()
		sortbase = shop_metrics.NewShopOrderMetric(db)
	case *selling_iface.ShopMetricSort_ShopOrderCompletedMetricSort:
		sortFieldName = sortField.ShopOrderCompletedMetricSort.String()
		sortbase = shop_metrics.NewShopOrderCompletedMetric(db)
	case *selling_iface.ShopMetricSort_ShopOrderCancelledMetricSort:
		sortFieldName = sortField.ShopOrderCancelledMetricSort.String()
		sortbase = shop_metrics.NewShopOrderCancelledMetric(db)
	case *selling_iface.ShopMetricSort_ShopReturnMetricSort:
		sortFieldName = sortField.ShopReturnMetricSort.String()
		sortbase = shop_metrics.NewShopReturnCreatedMetric(db)
	case *selling_iface.ShopMetricSort_ShopReturnArrivedMetricSort:
		sortFieldName = sortField.ShopReturnArrivedMetricSort.String()
		sortbase = shop_metrics.NewShopReturnArrivedMetric(db)
	case *selling_iface.ShopMetricSort_ShopReturnCancelledMetricSort:
		sortFieldName = sortField.ShopReturnCancelledMetricSort.String()
		sortbase = shop_metrics.NewShopReturnCancelledMetric(db)
	case *selling_iface.ShopMetricSort_ShopProductCostMetricSort:
		sortFieldName = sortField.ShopProductCostMetricSort.String()
		sortbase = shop_metrics.NewShopProductCostMetric(db)
	case *selling_iface.ShopMetricSort_ShopAdsExpenseMetricSort:
		sortFieldName = sortField.ShopAdsExpenseMetricSort.String()
		sortbase = shop_metrics.NewShopAdsExpenseMetric(db)
	case *selling_iface.ShopMetricSort_ShopHoldAmountMetricSort:
		sortFieldName = sortField.ShopHoldAmountMetricSort.String()
		sortbase = shop_metrics.NewShopHoldAmountMetric(db)
	default:
		err = errors.New("invalid sort type")
		return nil, err
	}

	var resultIds resultKey
	sortCacheKey := &shopSortKey{
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
		var metric *selling_iface.ShopMetric = &selling_iface.ShopMetric{}
		var metricbase metric_base.ShopMetricBase

		switch metType {
		case selling_iface.ShopMetricType_SHOP_METRIC_TYPE_ORDER:
			metricbase = shop_metrics.NewShopOrderMetric(db)
		case selling_iface.ShopMetricType_SHOP_METRIC_TYPE_ORDER_COMPLETED:
			metricbase = shop_metrics.NewShopOrderCompletedMetric(db)
		case selling_iface.ShopMetricType_SHOP_METRIC_TYPE_ORDER_CANCELLED:
			metricbase = shop_metrics.NewShopOrderCancelledMetric(db)
		case selling_iface.ShopMetricType_SHOP_METRIC_TYPE_RETURN_CREATED:
			metricbase = shop_metrics.NewShopReturnCreatedMetric(db)
		case selling_iface.ShopMetricType_SHOP_METRIC_TYPE_RETURN_ARRIVED:
			metricbase = shop_metrics.NewShopReturnArrivedMetric(db)
		case selling_iface.ShopMetricType_SHOP_METRIC_TYPE_RETURN_CANCELLED:
			metricbase = shop_metrics.NewShopReturnCancelledMetric(db)
		case selling_iface.ShopMetricType_SHOP_METRIC_TYPE_PRODUCT_COST:
			metricbase = shop_metrics.NewShopProductCostMetric(db)
		case selling_iface.ShopMetricType_SHOP_METRIC_TYPE_ADS_EXPENSE:
			metricbase = shop_metrics.NewShopAdsExpenseMetric(db)
		case selling_iface.ShopMetricType_SHOP_METRIC_TYPE_HOLD_AMOUNT:
			metricbase = shop_metrics.NewShopHoldAmountMetric(db)
		}

		err = s.cacheMgr.Get(ctx, &listKey{
			Ids:        result.Ids,
			MetricName: metType.String(),
		}, metric)
		if err == nil {
			result.Metrics = append(result.Metrics, metric)
			continue
		}

		// jika cache tidak ada
		slog.Info("getting fresh metric", "metricType", metType, "error", err)
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

type shopSortKey struct {
	Filter        *selling_iface.ShopStatMetricFilter
	SortType      string
	SortFieldName string
}

func (k *shopSortKey) GetKey() (string, error) {

	bytes, err := proto.MarshalOptions{
		Deterministic: true,
	}.Marshal(k.Filter)

	if err != nil {
		return "", err
	}
	hashedIds := md5.Sum(bytes)
	return fmt.Sprintf("metric_sort:%s:%s:%x", k.SortFieldName, k.SortType, hashedIds), nil
}
