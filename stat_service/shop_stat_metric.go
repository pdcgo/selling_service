package stat_service

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/stat_service/shop_metrics"
)

// ShopStatMetric implements [selling_ifaceconnect.SellingStatServiceHandler].
func (s *statServiceImpl) ShopStatMetric(ctx context.Context, req *connect.Request[selling_iface.ShopStatMetricRequest]) (*connect.Response[selling_iface.ShopStatMetricResponse], error) {
	var err error
	result := selling_iface.ShopStatMetricResponse{
		Metrics: []*selling_iface.ShopMetric{},
	}

	db := s.db.WithContext(ctx)

	switch req.Msg.Sort.S.(type) {
	case *selling_iface.ShopMetricSort_CommonSort:
		result.Ids, err = shop_metrics.NewCommonShopMetric(db).ProcessSort(ctx, req.Msg.Filter, req.Msg.Sort)
	default:
		err = errors.New("invalid sort type")
	}

	if err != nil {
		return nil, err
	}

	for _, metType := range req.Msg.MetricTypes {
		var metric *selling_iface.ShopMetric
		switch metType {
		case selling_iface.ShopMetricType_SHOP_METRIC_TYPE_ORDER:
			metric, err = shop_metrics.NewShopOrderMetric(db).FetchMetric(ctx, result.Ids, req.Msg.Filter)
		}

		if err != nil {
			return nil, err
		}

		result.Metrics = append(result.Metrics, metric)
	}

	return connect.NewResponse(&result), nil
}
