package stat_service

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
)

// Statements implements [selling_ifaceconnect.SellingStatServiceHandler].
func (s *statServiceImpl) Statements(
	ctx context.Context,
	req *connect.Request[selling_iface.StatementsRequest],
	stream *connect.ServerStream[selling_iface.StatementsResponse],
) error {
	panic("unimplemented")
}
