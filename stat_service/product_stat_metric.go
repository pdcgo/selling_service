package stat_service

import (
	"context"

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
	var ids []uint64
	result := selling_iface.ProductStatMetricResponse{}

	db := s.db.WithContext(ctx)

	// processing sort
	switch req.Msg.Sort.S.(type) {
	case *selling_iface.ProductMetricSort_CommonSort:
		ids, err = product_metrics.NewProductCommon(db).ProcessSort(ctx, req.Msg.Filter, req.Msg.Sort)
	case *selling_iface.ProductMetricSort_ProductOrderMetricSort:
		ids, err = product_metrics.NewProductOrderMetric(db).ProcessSort(ctx, req.Msg.Filter, req.Msg.Sort)
	}

	if err != nil {
		return nil, err
	}

	for _, metType := range req.Msg.MetricTypes {
		var metric *selling_iface.ProductMetric
		switch metType {
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_ORDER:
			metric, err = product_metrics.NewProductOrderMetric(db).FetchMetric(ctx, ids, req.Msg.Filter)
		}

		if err != nil {
			return nil, err
		}

		result.Metrics = append(result.Metrics, metric)
	}

	return connect.NewResponse(&result), nil
}
