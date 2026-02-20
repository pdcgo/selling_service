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

// OweLimitCustomDelete implements selling_ifaceconnect.ConfigurationLimitServiceHandler.
func (c *configurationLimitServiceImpl) OweLimitCustomDelete(
	ctx context.Context,
	req *connect.Request[selling_iface.OweLimitCustomDeleteRequest]) (*connect.Response[selling_iface.OweLimitCustomDeleteResponse], error) {
	var err error
	pay := req.Msg

	source, err := custom_connect.GetRequestSource(ctx)
	if err != nil {
		return &connect.Response[selling_iface.OweLimitCustomDeleteResponse]{}, err
	}

	identity := c.
		auth.
		AuthIdentityFromHeader(req.Header())

	var domainID uint
	switch source.RequestFrom {
	case access_iface.RequestFrom_REQUEST_FROM_ADMIN:
		domainID = authorization.RootDomain
	default:
		domainID = uint(pay.TeamId)
	}

	identity.
		HasPermission(
			authorization_iface.CheckPermissionGroup{
				&db_models.OweLimitConfiguration{}: &authorization_iface.CheckPermission{
					DomainID: domainID,
					Actions:  []authorization_iface.Action{authorization_iface.Delete},
				},
			},
		)

	err = identity.Err()
	if err != nil {
		return &connect.Response[selling_iface.OweLimitCustomDeleteResponse]{}, err
	}
	db := c.db.WithContext(ctx)

	err = db.
		Model(&db_models.OweLimitConfiguration{}).
		Where("team_id = ?", pay.TeamId).
		Where("for_team_id = ?", pay.ForTeamId).
		Delete(&db_models.OweLimitConfiguration{}).
		Error

	return connect.NewResponse(&selling_iface.OweLimitCustomDeleteResponse{}), err

}
