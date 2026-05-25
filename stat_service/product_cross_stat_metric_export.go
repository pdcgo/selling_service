package stat_service

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
)

// ProductCrossStatMetricExport implements [selling_ifaceconnect.SellingStatServiceHandler].
func (s *statServiceImpl) ProductCrossStatMetricExport(
	ctx context.Context,
	req *connect.Request[selling_iface.ProductCrossStatMetricExportRequest],
	stream *connect.ServerStream[selling_iface.ProductCrossStatMetricExportResponse],

) error {
	var err error
	// var defaultExpiration time.Duration = time.Minute

	// db := s.db.WithContext(ctx)
	// var sortbase metric_base.ProductCrossMetricBase

	return err
}
