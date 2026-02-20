package services

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/access_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/authorization"
	"github.com/pdcgo/shared/custom_connect"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/interfaces/authorization_iface"
)

// SellingLimitProfitEdit implements selling_ifaceconnect.ConfigurationLimitServiceHandler.
func (c *configurationLimitServiceImpl) SellingLimitProfitEdit(
	ctx context.Context,
	req *connect.Request[selling_iface.SellingLimitProfitEditRequest],
) (*connect.Response[selling_iface.SellingLimitProfitEditResponse], error) {
	var err error

	source, err := custom_connect.GetRequestSource(ctx)
	if err != nil {
		return nil, err
	}

	identity := c.
		auth.
		AuthIdentityFromHeader(req.Header())

	var domainID uint
	switch source.RequestFrom {
	case access_iface.RequestFrom_REQUEST_FROM_ADMIN:
		domainID = authorization.RootDomain
	case access_iface.RequestFrom_REQUEST_FROM_SELLING,
		access_iface.RequestFrom_REQUEST_FROM_WAREHOUSE:
		domainID = uint(source.TeamId)
	default:
		domainID = authorization.RootDomain
	}

	identity.
		HasPermission(
			authorization_iface.CheckPermissionGroup{
				&db_models.OweLimitConfiguration{}: &authorization_iface.CheckPermission{
					DomainID: domainID,
					Actions:  []authorization_iface.Action{authorization_iface.Update},
				},
			},
		)

	err = identity.Err()
	if err != nil {
		return nil, err
	}

	pay := req.Msg
	cfgData := pay.Data

	ref := "selling/limit_profit/default"
	doc := c.client.Collection("configuration").Doc(ref)
	_, err = doc.Set(ctx, cfgData)
	if err != nil {
		return nil, err
	}

	return &connect.Response[selling_iface.SellingLimitProfitEditResponse]{}, nil
}
