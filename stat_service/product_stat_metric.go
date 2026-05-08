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
	result := selling_iface.ProductStatMetricResponse{}

	db := s.db.WithContext(ctx)

	for _, metType := range req.Msg.MetricTypes {
		var metric *selling_iface.ProductMetric
		switch metType {
		case selling_iface.ProductMetricType_PRODUCT_METRIC_TYPE_ORDER:
			metric, err = product_metrics.NewProductOrderMetric(db, req.Msg.Filter, req.Msg.Range)
			if err != nil {
				return nil, err
			}
		}

		result.Metrics = append(result.Metrics, metric)
	}

	return connect.NewResponse(&result), nil
}
