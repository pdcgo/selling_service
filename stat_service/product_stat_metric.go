package stat_service

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/stat_service/product_metrics"
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

	db := s.db.WithContext(ctx)

	// processing sort
	switch req.Msg.Sort.S.(type) {
	case *selling_iface.ProductMetricSort_CommonSort:
		result.Ids, err = product_metrics.NewProductCommon(db).ProcessSort(ctx, req.Msg.Filter, req.Msg.Sort)
	case *selling_iface.ProductMetricSort_ProductOrderMetricSort:
		result.Ids, err = product_metrics.NewProductOrderMetric(db).ProcessSort(ctx, req.Msg.Filter, req.Msg.Sort)
	case *selling_iface.ProductMetricSort_RestockAcceptedMetricSort:
		result.Ids, err = product_metrics.NewRestockAcceptedMetric(db).ProcessSort(ctx, req.Msg.Filter, req.Msg.Sort)
	case *selling_iface.ProductMetricSort_RestockCreatedMetricSort:
		result.Ids, err = product_metrics.NewRestockCreatedMetric(db).ProcessSort(ctx, req.Msg.Filter, req.Msg.Sort)
	case *selling_iface.ProductMetricSort_RestockCancelledMetricSort:
		result.Ids, err = product_metrics.NewRestockCancelledMetric(db).ProcessSort(ctx, req.Msg.Filter, req.Msg.Sort)
	case *selling_iface.ProductMetricSort_ReturnCreatedMetricSort:
		result.Ids, err = product_metrics.NewReturnCreatedMetric(db).ProcessSort(ctx, req.Msg.Filter, req.Msg.Sort)
	case *selling_iface.ProductMetricSort_ReturnArrivedMetricSort:
		result.Ids, err = product_metrics.NewReturnArrivedMetric(db).ProcessSort(ctx, req.Msg.Filter, req.Msg.Sort)
	case *selling_iface.ProductMetricSort_ReturnCancelledMetricSort:
		result.Ids, err = product_metrics.NewReturnCancelledMetric(db).ProcessSort(ctx, req.Msg.Filter, req.Msg.Sort)
	case *selling_iface.ProductMetricSort_StockReadyMetricSort:
		result.Ids, err = product_metrics.NewStockReadyMetric(db).ProcessSort(ctx, req.Msg.Filter, req.Msg.Sort)
	case *selling_iface.ProductMetricSort_StockOngoingMetricSort:
		result.Ids, err = product_metrics.NewStockOngoingMetric(db).ProcessSort(ctx, req.Msg.Filter, req.Msg.Sort)
	case *selling_iface.ProductMetricSort_ProductShipmentTimeMetricSort:
		result.Ids, err = product_metrics.NewProductShipmentTimeMetric(db).ProcessSort(ctx, req.Msg.Filter, req.Msg.Sort)

	default:
		err = errors.New("invalid sort type")
	}

	if err != nil {
		return nil, err
	}

	for _, metType := range req.Msg.MetricTypes {
		var metric *selling_iface.ProductMetric
		switch metType {
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_ORDER:
			metric, err = product_metrics.NewProductOrderMetric(db).FetchMetric(ctx, result.Ids, req.Msg.Filter)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_RESTOCK_ACCEPTED:
			metric, err = product_metrics.NewRestockAcceptedMetric(db).FetchMetric(ctx, result.Ids, req.Msg.Filter)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_RESTOCK:
			metric, err = product_metrics.NewRestockCreatedMetric(db).FetchMetric(ctx, result.Ids, req.Msg.Filter)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_RESTOCK_CANCEL:
			metric, err = product_metrics.NewRestockCancelledMetric(db).FetchMetric(ctx, result.Ids, req.Msg.Filter)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_RETURN_ACCEPTED:
			metric, err = product_metrics.NewReturnArrivedMetric(db).FetchMetric(ctx, result.Ids, req.Msg.Filter)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_RETURN:
			metric, err = product_metrics.NewReturnCreatedMetric(db).FetchMetric(ctx, result.Ids, req.Msg.Filter)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_RETURN_CANCEL:
			metric, err = product_metrics.NewReturnCancelledMetric(db).FetchMetric(ctx, result.Ids, req.Msg.Filter)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_STOCK_READY:
			metric, err = product_metrics.NewStockReadyMetric(db).FetchMetric(ctx, result.Ids, req.Msg.Filter)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_STOCK_ONGOING:
			metric, err = product_metrics.NewStockOngoingMetric(db).FetchMetric(ctx, result.Ids, req.Msg.Filter)
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_SHIPMENT_TIME:
			metric, err = product_metrics.NewProductShipmentTimeMetric(db).FetchMetric(ctx, result.Ids, req.Msg.Filter)
		}

		if err != nil {
			return nil, err
		}

		result.Metrics = append(result.Metrics, metric)
	}

	return connect.NewResponse(&result), nil
}
