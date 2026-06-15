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

	var metricMap = user_metrics.MetricMap{}
	for _, metType := range req.Msg.MetricTypes {
		switch metType {
		// order
		case selling_iface.UserMetricType_USER_METRIC_TYPE_ORDER:
			metricMap[metType] = user_metrics.NewUserOrderMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_ORDER_WITHDRAWAL:
			metricMap[metType] = user_metrics.NewUserOrderWithdrawalMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_ORDER_CANCELLED:
			metricMap[metType] = user_metrics.NewUserOrderCancelledMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_ORDER_LOST:
			metricMap[metType] = user_metrics.NewUserOrderLostMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_ORDER_COMPLETED:
			metricMap[metType] = user_metrics.NewUserOrderCompletedMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_ORDER_RETURN:
			metricMap[metType] = user_metrics.NewUserOrderReturnMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_ORDER_RETURN_COMPLETED:
			metricMap[metType] = user_metrics.NewUserOrderReturnCompletedMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_ORDER_ONGOING:
			metricMap[metType] = user_metrics.NewUserOrderOngoingMetric(db)

		// stock order
		case selling_iface.UserMetricType_USER_METRIC_TYPE_STOCK_ORDER:
			metricMap[metType] = user_metrics.NewUserStockOrderMetric(db)

		// avg order
		case selling_iface.UserMetricType_USER_METRIC_TYPE_AVG_ORDER:
			metricMap[metType] = user_metrics.NewUserAvgOrderMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_AVG_ORDER_WITHDRAWAL:
			metricMap[metType] = user_metrics.NewUserAvgOrderWithdrawalMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_AVG_ORDER_CANCELLED:
			metricMap[metType] = user_metrics.NewUserAvgOrderCancelledMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_AVG_ORDER_LOST:
			metricMap[metType] = user_metrics.NewUserAvgOrderLostMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_AVG_ORDER_COMPLETED:
			metricMap[metType] = user_metrics.NewUserAvgOrderCompletedMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_AVG_ORDER_RETURN:
			metricMap[metType] = user_metrics.NewUserAvgOrderReturnMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_AVG_ORDER_RETURN_COMPLETED:
			metricMap[metType] = user_metrics.NewUserAvgOrderReturnCompletedMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_AVG_ORDER_ONGOING:
			metricMap[metType] = user_metrics.NewUserAvgOrderOngoingMetric(db)

		// cost order
		case selling_iface.UserMetricType_USER_METRIC_TYPE_COST_WAREHOUSE:
			metricMap[metType] = user_metrics.NewUserCostWarehouseMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_COST_PRODUCT_OWN:
			metricMap[metType] = user_metrics.NewUserCostProductOwnMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_COST_PRODUCT_CROSS:
			metricMap[metType] = user_metrics.NewUserCostProductCrossMetric(db)

		// profit order
		case selling_iface.UserMetricType_USER_METRIC_TYPE_PROFIT_ORDER_CREATED:
			metricMap[metType] = user_metrics.NewUserProfitOrderCreatedMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_PROFIT_ORDER_WITHDRAWAL:
			metricMap[metType] = user_metrics.NewUserProfitOrderWithdrawalMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_LOST_PROFIT_ORDER:
			metricMap[metType] = user_metrics.NewUserLostProfitOrderMetric(db)

		case selling_iface.UserMetricType_USER_METRIC_TYPE_WITHDRAWAL:
			metricMap[metType] = user_metrics.NewUserWithdrawalMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_WITHDRAWAL_BREAKDOWN:
			metricMap[metType] = user_metrics.NewUserWithdrawalBreakdownMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_REVENUE_ORDER:
			metricMap[metType] = user_metrics.NewUserRevenueOrderMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_REVENUE_WAREHOUSE_ADJUSTMENT:
			metricMap[metType] = user_metrics.NewUserRevenueWarehouseAdjustmentMetric(db)
		case selling_iface.UserMetricType_USER_METRIC_TYPE_REVENUE_PRODUCT_ADJUSTMENT:
			metricMap[metType] = user_metrics.NewUserRevenueProductAdjustmentMetric(db)

		case selling_iface.UserMetricType_USER_METRIC_TYPE_ADS_EXPENSE:
			metricMap[metType] = user_metrics.NewUserAdsExpenseMetric(db)

		case selling_iface.UserMetricType_USER_METRIC_TYPE_PROFIT_OR_LOSS:
			metricMap[metType] = user_metrics.NewUserProfitOrLossMetric(db, metricMap)

		default:
			err = errors.New("invalid metric type")
			return nil, err
		}
	}

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
	case *selling_iface.UserMetricSort_UserOrderCompletedMetricSort:
		sortFieldName = sortField.UserOrderCompletedMetricSort.String()
		sortbase = user_metrics.NewUserOrderCompletedMetric(db)
	case *selling_iface.UserMetricSort_UserOrderReturnMetricSort:
		sortFieldName = sortField.UserOrderReturnMetricSort.String()
		sortbase = user_metrics.NewUserOrderReturnMetric(db)
	case *selling_iface.UserMetricSort_UserOrderReturnCompletedMetricSort:
		sortFieldName = sortField.UserOrderReturnCompletedMetricSort.String()
		sortbase = user_metrics.NewUserOrderReturnCompletedMetric(db)
	case *selling_iface.UserMetricSort_UserOrderOngoingMetricSort:
		sortFieldName = sortField.UserOrderOngoingMetricSort.String()
		sortbase = user_metrics.NewUserOrderOngoingMetric(db)

	// stock order
	case *selling_iface.UserMetricSort_UserStockOrderMetricSort:
		sortFieldName = sortField.UserStockOrderMetricSort.String()
		sortbase = user_metrics.NewUserStockOrderMetric(db)

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
	case *selling_iface.UserMetricSort_UserAvgOrderCompletedMetricSort:
		sortFieldName = sortField.UserAvgOrderCompletedMetricSort.String()
		sortbase = user_metrics.NewUserAvgOrderCompletedMetric(db)
	case *selling_iface.UserMetricSort_UserAvgOrderReturnMetricSort:
		sortFieldName = sortField.UserAvgOrderReturnMetricSort.String()
		sortbase = user_metrics.NewUserAvgOrderReturnMetric(db)
	case *selling_iface.UserMetricSort_UserAvgOrderReturnCompletedMetricSort:
		sortFieldName = sortField.UserAvgOrderReturnCompletedMetricSort.String()
		sortbase = user_metrics.NewUserAvgOrderReturnCompletedMetric(db)
	case *selling_iface.UserMetricSort_UserAvgOrderOngoingMetricSort:
		sortFieldName = sortField.UserAvgOrderOngoingMetricSort.String()
		sortbase = user_metrics.NewUserAvgOrderOngoingMetric(db)

	// cost order
	case *selling_iface.UserMetricSort_UserCostWarehouseMetricSort:
		sortFieldName = sortField.UserCostWarehouseMetricSort.String()
		sortbase = user_metrics.NewUserCostWarehouseMetric(db)
	case *selling_iface.UserMetricSort_UserCostProductOwnMetricSort:
		sortFieldName = sortField.UserCostProductOwnMetricSort.String()
		sortbase = user_metrics.NewUserCostProductOwnMetric(db)
	case *selling_iface.UserMetricSort_UserCostProductCrossMetricSort:
		sortFieldName = sortField.UserCostProductCrossMetricSort.String()
		sortbase = user_metrics.NewUserCostProductCrossMetric(db)

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
	case *selling_iface.UserMetricSort_UserWithdrawalBreakdownMetricSort:
		sortFieldName = sortField.UserWithdrawalBreakdownMetricSort.String()
		sortbase = user_metrics.NewUserWithdrawalBreakdownMetric(db)
	case *selling_iface.UserMetricSort_UserRevenueOrderMetricSort:
		sortFieldName = sortField.UserRevenueOrderMetricSort.String()
		sortbase = user_metrics.NewUserRevenueOrderMetric(db)
	case *selling_iface.UserMetricSort_UserRevenueWarehouseAdjustmentMetricSort:
		sortFieldName = sortField.UserRevenueWarehouseAdjustmentMetricSort.String()
		sortbase = user_metrics.NewUserRevenueWarehouseAdjustmentMetric(db)
	case *selling_iface.UserMetricSort_UserRevenueProductAdjustmentMetricSort:
		sortFieldName = sortField.UserRevenueProductAdjustmentMetricSort.String()
		sortbase = user_metrics.NewUserRevenueProductAdjustmentMetric(db)

	case *selling_iface.UserMetricSort_UserAdsExpenseMetricSort:
		sortFieldName = sortField.UserAdsExpenseMetricSort.String()
		sortbase = user_metrics.NewUserAdsExpenseMetric(db)

	case *selling_iface.UserMetricSort_UserProfitOrLossMetricSort:
		sortFieldName = sortField.UserProfitOrLossMetricSort.String()
		sortbase = user_metrics.NewUserProfitOrLossMetric(db, metricMap)

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
		var metricbase = metricMap[metType]

		if metricbase == nil {
			err = errors.New("invalid sort type")
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
