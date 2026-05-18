package stat_service

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
)

// UserStatMetric implements [selling_ifaceconnect.SellingStatServiceHandler].
func (s *statServiceImpl) UserStatMetric(context.Context, *connect.Request[selling_iface.UserStatMetricRequest]) (*connect.Response[selling_iface.UserStatMetricResponse], error) {
	panic("unimplemented")
}
