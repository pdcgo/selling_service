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

	// order
	case *selling_iface.UserMetricSort_UserOrderMetricSort:
		sortFieldName = sortField.UserOrderMetricSort.String()
		sortbase = user_metrics.NewUserOrderMetric(db)
	case *selling_iface.UserMetricSort_UserOrderWithdrawalMetricSort:
		sortFieldName = sortField.UserOrderWithdrawalMetricSort.String()
		sortbase = user_metrics.NewUserOrderWithdrawalMetric(db)
	case *selling_iface.UserMetricSort_UserOrderCancelledMetricSort:
		sortFieldName = sortField.UserOrderCancelledMetricSort.String()
		sortbase = user_metrics.NewUserOrderCancelledMetric(db)
	case *selling_iface.UserMetricSort_UserOrderLostMetricSort:
		sortFieldName = sortField.UserOrderLostMetricSort.String()
		sortbase = user_metrics.NewUserOrderLostMetric(db)

	// stock order
	case *selling_iface.UserMetricSort_UserStockOrderMetricSort:
		sortFieldName = sortField.UserStockOrderMetricSort.String()
		sortbase = user_metrics.NewUserStockOrderMetric(db)
	case *selling_iface.UserMetricSort_UserStockOrderWithdrawalMetricSort:
		sortFieldName = sortField.UserStockOrderWithdrawalMetricSort.String()
		sortbase = user_metrics.NewUserStockOrderWithdrawalMetric(db)
	case *selling_iface.UserMetricSort_UserStockOrderCancelledMetricSort:
		sortFieldName = sortField.UserStockOrderCancelledMetricSort.String()
		sortbase = user_metrics.NewUserStockOrderCancelledMetric(db)
	case *selling_iface.UserMetricSort_UserStockOrderLostMetricSort:
		sortFieldName = sortField.UserStockOrderLostMetricSort.String()
		sortbase = user_metrics.NewUserStockOrderLostMetric(db)

	// avg order
	case *selling_iface.UserMetricSort_UserAvgOrderMetricSort:
		sortFieldName = sortField.UserAvgOrderMetricSort.String()
		sortbase = user_metrics.NewUserAvgOrderMetric(db)
	case *selling_iface.UserMetricSort_UserAvgOrderWithdrawalMetricSort:
		sortFieldName = sortField.UserAvgOrderWithdrawalMetricSort.String()
		sortbase = user_metrics.NewUserAvgOrderWithdrawalMetric(db)
	case *selling_iface.UserMetricSort_UserAvgOrderCancelledMetricSort:
		sortFieldName = sortField.UserAvgOrderCancelledMetricSort.String()
		sortbase = user_metrics.NewUserAvgOrderCancelledMetric(db)
	case *selling_iface.UserMetricSort_UserAvgOrderLostMetricSort:
		sortFieldName = sortField.UserAvgOrderLostMetricSort.String()
		sortbase = user_metrics.NewUserAvgOrderLostMetric(db)

	// cost order
	case *selling_iface.UserMetricSort_UserCostOrderMetricSort:
		sortFieldName = sortField.UserCostOrderMetricSort.String()
		sortbase = user_metrics.NewUserCostOrderMetric(db)
	case *selling_iface.UserMetricSort_UserCostOrderWithdrawalMetricSort:
		sortFieldName = sortField.UserCostOrderWithdrawalMetricSort.String()
		sortbase = user_metrics.NewUserCostOrderWithdrawalMetric(db)
	case *selling_iface.UserMetricSort_UserCostOrderCancelledMetricSort:
		sortFieldName = sortField.UserCostOrderCancelledMetricSort.String()
		sortbase = user_metrics.NewUserCostOrderCancelledMetric(db)
	case *selling_iface.UserMetricSort_UserCostOrderLostMetricSort:
		sortFieldName = sortField.UserCostOrderLostMetricSort.String()
		sortbase = user_metrics.NewUserCostOrderLostMetric(db)

	// profit order
	case *selling_iface.UserMetricSort_UserProfitOrderCreatedMetricSort:
		sortFieldName = sortField.UserProfitOrderCreatedMetricSort.String()
		sortbase = user_metrics.NewUserProfitOrderCreatedMetric(db)
	case *selling_iface.UserMetricSort_UserProfitOrderWithdrawalMetricSort:
		sortFieldName = sortField.UserProfitOrderWithdrawalMetricSort.String()
		sortbase = user_metrics.NewUserProfitOrderWithdrawalMetric(db)
	case *selling_iface.UserMetricSort_UserLostProfitOrderMetricSort:
		sortFieldName = sortField.UserLostProfitOrderMetricSort.String()
		sortbase = user_metrics.NewUserLostProfitOrderMetric(db)

	case *selling_iface.UserMetricSort_UserWithdrawalMetricSort:
		sortFieldName = sortField.UserWithdrawalMetricSort.String()
		sortbase = user_metrics.NewUserWithdrawalMetric(db)

	case *selling_iface.UserMetricSort_UserAdsExpenseMetricSort:
		sortFieldName = sortField.UserAdsExpenseMetricSort.String()
		sortbase = user_metrics.NewUserAdsExpenseMetric(db)

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
		// order
		case selling_iface.UserMetricType_USER_METRIC_TYPE_ORDER:
			metricbase = user_metrics.NewUserOrderMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_ORDER_WITHDRAWAL:
			metricbase = user_metrics.NewUserOrderWithdrawalMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_ORDER_CANCELLED:
			metricbase = user_metrics.NewUserOrderCancelledMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_ORDER_LOST:
			metricbase = user_metrics.NewUserOrderLostMetric(db)

		// stock order
		case selling_iface.UserMetricType_USER_METRIC_TYPE_STOCK_ORDER:
			metricbase = user_metrics.NewUserStockOrderMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_STOCK_ORDER_WITHDRAWAL:
			metricbase = user_metrics.NewUserStockOrderWithdrawalMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_STOCK_ORDER_CANCELLED:
			metricbase = user_metrics.NewUserStockOrderCancelledMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_STOCK_ORDER_LOST:
			metricbase = user_metrics.NewUserStockOrderLostMetric(db)

		// avg order
		case selling_iface.UserMetricType_USER_METRIC_TYPE_AVG_ORDER:
			metricbase = user_metrics.NewUserAvgOrderMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_AVG_ORDER_WITHDRAWAL:
			metricbase = user_metrics.NewUserAvgOrderWithdrawalMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_AVG_ORDER_CANCELLED:
			metricbase = user_metrics.NewUserAvgOrderCancelledMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_AVG_ORDER_LOST:
			metricbase = user_metrics.NewUserAvgOrderLostMetric(db)

		// cost order
		case selling_iface.UserMetricType_USER_METRIC_TYPE_COST_ORDER:
			metricbase = user_metrics.NewUserCostOrderMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_COST_ORDER_WITHDRAWAL:
			metricbase = user_metrics.NewUserCostOrderWithdrawalMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_COST_ORDER_CANCELLED:
			metricbase = user_metrics.NewUserCostOrderCancelledMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_COST_ORDER_LOST:
			metricbase = user_metrics.NewUserCostOrderLostMetric(db)

		// profit order
		case selling_iface.UserMetricType_USER_METRIC_TYPE_PROFIT_ORDER_CREATED:
			metricbase = user_metrics.NewUserProfitOrderCreatedMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_PROFIT_ORDER_WITHDRAWAL:
			metricbase = user_metrics.NewUserProfitOrderWithdrawalMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_LOST_PROFIT_ORDER:
			metricbase = user_metrics.NewUserLostProfitOrderMetric(db)

		case selling_iface.UserMetricType_USER_METRIC_TYPE_WITHDRAWAL:
			metricbase = user_metrics.NewUserWithdrawalMetric(db)

		case selling_iface.UserMetricType_USER_METRIC_TYPE_ADS_EXPENSE:
			metricbase = user_metrics.NewUserAdsExpenseMetric(db)

		default:
			err = errors.New("invalid metric type")
			return nil, err
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
