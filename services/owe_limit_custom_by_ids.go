package services

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
)

// OweLimitCustomByIDs implements selling_ifaceconnect.ConfigurationLimitServiceHandler.
func (c *configurationLimitServiceImpl) OweLimitCustomByIDs(
	ctx context.Context,
	req *connect.Request[selling_iface.OweLimitCustomByIDsRequest]) (
	*connect.Response[selling_iface.OweLimitCustomByIDsResponse],
	error) {

	var err error
	db := c.db.WithContext(ctx)
	pay := req.Msg

	result := selling_iface.OweLimitCustomByIDsResponse{
		Data: []*selling_iface.OweLimitDetailItem{},
	}

	for _, item := range pay.Items {
		cfgs := []*selling_iface.OweLimitItem{}
		err = db.
			Table("owe_limit_configurations l").
			Where(`
			l.team_id = ?
			and (
				l.for_team_id = ?
				or (
					l.for_team_id is null
					and l.is_default = true
				)
			)
			`,
				item.CfgTeamId, item.TeamId,
			).
			Select([]string{
				"l.team_id as team_id",
				"l.is_default as is_default",
				"l.for_team_id as for_team_id",
				"l.threshold as threshold",
			}).
			Find(&cfgs).
			Error

		if err != nil {
			return &connect.Response[selling_iface.OweLimitCustomByIDsResponse]{}, err
		}

		var data *selling_iface.OweLimitItem
		for _, cfg := range cfgs {
			if cfg.IsDefault {
				data = cfg
				data.ForTeamId = item.TeamId
			} else {
				data = cfg
				break
			}

		}

		var amount float64
		if pay.IncludeActive {
			amount, err = c.getInvoiceAmount(db, item.TeamId, item.CfgTeamId)
			if err != nil {
				return &connect.Response[selling_iface.OweLimitCustomByIDsResponse]{}, err
			}
		}

		result.Data = append(result.Data, &selling_iface.OweLimitDetailItem{
			Limit:        data,
			ActiveAmount: amount,
		})

	}

	return connect.NewResponse(&result), nil

}
