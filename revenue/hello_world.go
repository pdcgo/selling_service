package revenue

import (
	"context"

	"connectrpc.com/connect"
	selling_iface "github.com/pdcgo/schema/services/selling_iface/v1"
)

// HelloWorld implements [selling_ifaceconnect.RevenueServiceHandler]. Skeleton RPC
// proving the service wiring + v2 roling; replaced when the real revenue RPCs land.
func (s *revenueServiceImpl) HelloWorld(
	ctx context.Context,
	req *connect.Request[selling_iface.HelloWorldRequest],
) (*connect.Response[selling_iface.HelloWorldResponse], error) {
	name := req.Msg.GetName()
	if name == "" {
		name = "world"
	}

	return connect.NewResponse(&selling_iface.HelloWorldResponse{
		Message: "hello " + name,
	}), nil
}
