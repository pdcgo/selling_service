package services

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_models"
)

// OweDefaultLimitGet implements selling_ifaceconnect.ConfigurationLimitServiceHandler.
func (c *configurationLimitServiceImpl) OweDefaultLimitGet(
	ctx context.Context,
	req *connect.Request[selling_iface.OweDefaultLimitGetRequest]) (*connect.Response[selling_iface.OweDefaultLimitGetResponse], error) {
	var err error
	db := c.db.WithContext(ctx)

	result := selling_iface.OweDefaultLimitGetResponse{}
	err = db.
		Model(&db_models.OweLimitConfiguration{}).
		Where("is_default = true and team_id = ?", req.Msg.TeamId).
		Select([]string{
			"id",
			"team_id",
			"is_default",
			"threshold",
		}).
		Find(&result).
		Error

	return connect.NewResponse(&result), err
}
