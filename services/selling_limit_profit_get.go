package services

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
)

// SellingLimitProfitGet implements selling_ifaceconnect.ConfigurationLimitServiceHandler.
func (c *configurationLimitServiceImpl) SellingLimitProfitGet(
	ctx context.Context,
	req *connect.Request[selling_iface.SellingLimitProfitGetRequest],
) (*connect.Response[selling_iface.SellingLimitProfitGetResponse], error) {
	var err error

	identity := c.
		auth.
		AuthIdentityFromHeader(req.Header())

	err = identity.Err()
	if err != nil {
		return nil, err
	}

	ref := "selling/limit_profit/default"
	doc := c.client.Collection("configuration").Doc(ref)
	snap, err := doc.Get(ctx)
	if err != nil {
		return nil, err
	}
	result := selling_iface.SellingLimitProfitGetResponse{
		Data: &selling_iface.SellingLimitProfitConfig{},
	}

	err = snap.DataTo(result.Data)
	return connect.NewResponse(&result), err

}
