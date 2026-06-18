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

	var metricMap = team_metrics.MetricMap{}
	for _, metType := range req.Msg.MetricTypes {
		switch metType {
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_ORDER:
			metricMap[metType] = team_metrics.NewOrderMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_ORDER_WITHDRAWAL:
			metricMap[metType] = team_metrics.NewOrderWithdrawalMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_ORDER_CANCELLED:
			metricMap[metType] = team_metrics.NewOrderCancelledMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_ORDER_LOST:
			metricMap[metType] = team_metrics.NewOrderLostMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_ORDER_COMPLETED:
			metricMap[metType] = team_metrics.NewOrderCompletedMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_ORDER_RETURN:
			metricMap[metType] = team_metrics.NewOrderReturnMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_ORDER_RETURN_COMPLETED:
			metricMap[metType] = team_metrics.NewOrderReturnCompletedMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_ORDER_ONGOING:
			metricMap[metType] = team_metrics.NewOrderOngoingMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_AVG_ORDER:
			metricMap[metType] = team_metrics.NewAvgOrderMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_AVG_ORDER_WITHDRAWAL:
			metricMap[metType] = team_metrics.NewAvgOrderWithdrawalMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_AVG_ORDER_CANCELLED:
			metricMap[metType] = team_metrics.NewAvgOrderCancelledMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_AVG_ORDER_LOST:
			metricMap[metType] = team_metrics.NewAvgOrderLostMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_AVG_ORDER_COMPLETED:
			metricMap[metType] = team_metrics.NewAvgOrderCompletedMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_AVG_ORDER_RETURN:
			metricMap[metType] = team_metrics.NewAvgOrderReturnMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_AVG_ORDER_RETURN_COMPLETED:
			metricMap[metType] = team_metrics.NewAvgOrderReturnCompletedMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_AVG_ORDER_ONGOING:
			metricMap[metType] = team_metrics.NewAvgOrderOngoingMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_STOCK_ORDER:
			metricMap[metType] = team_metrics.NewStockOrderMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_COST_WAREHOUSE:
			metricMap[metType] = team_metrics.NewCostWarehouseMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_COST_PRODUCT_OWN:
			metricMap[metType] = team_metrics.NewCostProductOwnMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_COST_PRODUCT_CROSS:
			metricMap[metType] = team_metrics.NewCostProductCrossMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_ADS_EXPENSE:
			metricMap[metType] = team_metrics.NewAdsExpenseMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_WITHDRAWAL:
			metricMap[metType] = team_metrics.NewWithdrawalMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_WITHDRAWAL_BREAKDOWN:
			metricMap[metType] = team_metrics.NewWithdrawalBreakdownMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_REVENUE_ORDER:
			metricMap[metType] = team_metrics.NewRevenueOrderMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_REVENUE_WAREHOUSE_ADJUSTMENT:
			metricMap[metType] = team_metrics.NewRevenueWarehouseAdjustmentMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_REVENUE_PRODUCT_ADJUSTMENT:
			metricMap[metType] = team_metrics.NewRevenueProductAdjustmentMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_PROFIT_ORDER_CREATED:
			metricMap[metType] = team_metrics.NewProfitOrderCreatedMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_PROFIT_ORDER_WITHDRAWAL:
			metricMap[metType] = team_metrics.NewProfitOrderWithdrawalMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_LOST_PROFIT_ORDER:
			metricMap[metType] = team_metrics.NewLostProfitOrderMetric(db)
		case selling_iface.TeamMetricType_TEAM_METRIC_TYPE_PROFIT_OR_LOSS:
			metricMap[metType] = team_metrics.NewProfitOrLossMetric(db, metricMap)
		default:
			err = errors.New("invalid metric type")
		}
	}

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
	case *selling_iface.TeamMetricSort_TeamOrderWithdrawalMetricSort:
		sortFieldName = sortField.TeamOrderWithdrawalMetricSort.String()
		sortbase = team_metrics.NewOrderWithdrawalMetric(db)
	case *selling_iface.TeamMetricSort_TeamOrderCancelledMetricSort:
		sortFieldName = sortField.TeamOrderCancelledMetricSort.String()
		sortbase = team_metrics.NewOrderCancelledMetric(db)
	case *selling_iface.TeamMetricSort_TeamOrderLostMetricSort:
		sortFieldName = sortField.TeamOrderLostMetricSort.String()
		sortbase = team_metrics.NewOrderLostMetric(db)
	case *selling_iface.TeamMetricSort_TeamOrderCompletedMetricSort:
		sortFieldName = sortField.TeamOrderCompletedMetricSort.String()
		sortbase = team_metrics.NewOrderCompletedMetric(db)
	case *selling_iface.TeamMetricSort_TeamOrderReturnMetricSort:
		sortFieldName = sortField.TeamOrderReturnMetricSort.String()
		sortbase = team_metrics.NewOrderReturnMetric(db)
	case *selling_iface.TeamMetricSort_TeamOrderReturnCompletedMetricSort:
		sortFieldName = sortField.TeamOrderReturnCompletedMetricSort.String()
		sortbase = team_metrics.NewOrderReturnCompletedMetric(db)
	case *selling_iface.TeamMetricSort_TeamOrderOngoingMetricSort:
		sortFieldName = sortField.TeamOrderOngoingMetricSort.String()
		sortbase = team_metrics.NewOrderOngoingMetric(db)

	case *selling_iface.TeamMetricSort_TeamAvgOrderMetricSort:
		sortFieldName = sortField.TeamAvgOrderMetricSort.String()
		sortbase = team_metrics.NewAvgOrderMetric(db)
	case *selling_iface.TeamMetricSort_TeamAvgOrderWithdrawalMetricSort:
		sortFieldName = sortField.TeamAvgOrderWithdrawalMetricSort.String()
		sortbase = team_metrics.NewAvgOrderWithdrawalMetric(db)
	case *selling_iface.TeamMetricSort_TeamAvgOrderCancelledMetricSort:
		sortFieldName = sortField.TeamAvgOrderCancelledMetricSort.String()
		sortbase = team_metrics.NewAvgOrderCancelledMetric(db)
	case *selling_iface.TeamMetricSort_TeamAvgOrderLostMetricSort:
		sortFieldName = sortField.TeamAvgOrderLostMetricSort.String()
		sortbase = team_metrics.NewAvgOrderLostMetric(db)
	case *selling_iface.TeamMetricSort_TeamAvgOrderCompletedMetricSort:
		sortFieldName = sortField.TeamAvgOrderCompletedMetricSort.String()
		sortbase = team_metrics.NewAvgOrderCompletedMetric(db)
	case *selling_iface.TeamMetricSort_TeamAvgOrderReturnMetricSort:
		sortFieldName = sortField.TeamAvgOrderReturnMetricSort.String()
		sortbase = team_metrics.NewAvgOrderReturnMetric(db)
	case *selling_iface.TeamMetricSort_TeamAvgOrderReturnCompletedMetricSort:
		sortFieldName = sortField.TeamAvgOrderReturnCompletedMetricSort.String()
		sortbase = team_metrics.NewAvgOrderReturnCompletedMetric(db)
	case *selling_iface.TeamMetricSort_TeamAvgOrderOngoingMetricSort:
		sortFieldName = sortField.TeamAvgOrderOngoingMetricSort.String()
		sortbase = team_metrics.NewAvgOrderOngoingMetric(db)

	case *selling_iface.TeamMetricSort_TeamStockOrderMetricSort:
		sortFieldName = sortField.TeamStockOrderMetricSort.String()
		sortbase = team_metrics.NewStockOrderMetric(db)

	case *selling_iface.TeamMetricSort_TeamCostWarehouseMetricSort:
		sortFieldName = sortField.TeamCostWarehouseMetricSort.String()
		sortbase = team_metrics.NewCostWarehouseMetric(db)
	case *selling_iface.TeamMetricSort_TeamCostProductOwnMetricSort:
		sortFieldName = sortField.TeamCostProductOwnMetricSort.String()
		sortbase = team_metrics.NewCostProductOwnMetric(db)
	case *selling_iface.TeamMetricSort_TeamCostProductCrossMetricSort:
		sortFieldName = sortField.TeamCostProductCrossMetricSort.String()
		sortbase = team_metrics.NewCostProductCrossMetric(db)

	case *selling_iface.TeamMetricSort_TeamAdsExpenseMetricSort:
		sortFieldName = sortField.TeamAdsExpenseMetricSort.String()
		sortbase = team_metrics.NewAdsExpenseMetric(db)

	case *selling_iface.TeamMetricSort_TeamWithdrawalMetricSort:
		sortFieldName = sortField.TeamWithdrawalMetricSort.String()
		sortbase = team_metrics.NewWithdrawalMetric(db)
	case *selling_iface.TeamMetricSort_TeamWithdrawalBreakdownMetricSort:
		sortFieldName = sortField.TeamWithdrawalBreakdownMetricSort.String()
		sortbase = team_metrics.NewWithdrawalBreakdownMetric(db)
	case *selling_iface.TeamMetricSort_TeamRevenueOrderMetricSort:
		sortFieldName = sortField.TeamRevenueOrderMetricSort.String()
		sortbase = team_metrics.NewRevenueOrderMetric(db)
	case *selling_iface.TeamMetricSort_TeamRevenueWarehouseAdjustmentMetricSort:
		sortFieldName = sortField.TeamRevenueWarehouseAdjustmentMetricSort.String()
		sortbase = team_metrics.NewRevenueWarehouseAdjustmentMetric(db)
	case *selling_iface.TeamMetricSort_TeamRevenueProductAdjustmentMetricSort:
		sortFieldName = sortField.TeamRevenueProductAdjustmentMetricSort.String()
		sortbase = team_metrics.NewRevenueProductAdjustmentMetric(db)

	case *selling_iface.TeamMetricSort_TeamProfitOrderCreatedMetricSort:
		sortFieldName = sortField.TeamProfitOrderCreatedMetricSort.String()
		sortbase = team_metrics.NewProfitOrderCreatedMetric(db)
	case *selling_iface.TeamMetricSort_TeamProfitOrderWithdrawalMetricSort:
		sortFieldName = sortField.TeamProfitOrderWithdrawalMetricSort.String()
		sortbase = team_metrics.NewProfitOrderWithdrawalMetric(db)
	case *selling_iface.TeamMetricSort_TeamLostProfitOrderMetricSort:
		sortFieldName = sortField.TeamLostProfitOrderMetricSort.String()
		sortbase = team_metrics.NewLostProfitOrderMetric(db)
	case *selling_iface.TeamMetricSort_TeamProfitOrLossMetricSort:
		sortFieldName = sortField.TeamProfitOrLossMetricSort.String()
		sortbase = team_metrics.NewProfitOrLossMetric(db, metricMap)

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
