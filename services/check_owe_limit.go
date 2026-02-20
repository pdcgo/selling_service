package services

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

// CheckOweLimit implements selling_ifaceconnect.ConfigurationLimitServiceHandler.
func (c *configurationLimitServiceImpl) CheckOweLimit(
	ctx context.Context,
	req *connect.Request[selling_iface.CheckOweLimitRequest],
) (*connect.Response[selling_iface.CheckOweLimitResponse], error) {
	var err error

	pay := req.Msg

	result := selling_iface.CheckOweLimitResponse{
		CanOwe: map[uint64]*selling_iface.OweLimitAllow{},
	}

	for _, cfgTeamID := range pay.CfgTeamIds {
		result.CanOwe[cfgTeamID] = &selling_iface.OweLimitAllow{
			Allow: false,
		}

		cfg, err := c.getTeamLimit(c.db, pay.TeamId, cfgTeamID)
		if err != nil {
			return &connect.Response[selling_iface.CheckOweLimitResponse]{}, err
		}

		if cfg == nil {
			result.CanOwe[cfgTeamID].Allow = true
			continue
		} else {
			result.CanOwe[cfgTeamID].Threshold = cfg.Threshold
		}

		amount, err := c.getInvoiceAmount(c.db, pay.TeamId, cfgTeamID)
		if err != nil {
			return &connect.Response[selling_iface.CheckOweLimitResponse]{}, err
		}
		result.CanOwe[cfgTeamID].ActiveAmount = amount

		if cfg.Threshold == 0 {
			result.CanOwe[cfgTeamID].Allow = true
			continue
		}

		if amount < cfg.Threshold {
			result.CanOwe[cfgTeamID].Allow = true
		} else {
			result.CanOwe[cfgTeamID].Allow = false
			return connect.NewResponse(&result), errors.New("limit hutang tercapay")
		}

	}

	return connect.NewResponse(&result), err
}

func (c *configurationLimitServiceImpl) getInvoiceAmount(db *gorm.DB, teamID, cfgTeamID uint64) (float64, error) {
	var unpaidAmount float64
	err := db.
		Raw(`
			select 
				(case when sum(amount) is null then 0 else sum(amount) end) as amount 
			from invoices i 
			where 
				i.status = 'not_paid'
				and i.from_team_id = ?
				and i.to_team_id = ?
		`, teamID, cfgTeamID).
		Find(&unpaidAmount).
		Error

	return unpaidAmount, err
}

func (c *configurationLimitServiceImpl) getTeamLimit(db *gorm.DB, teamID, cfgTeamID uint64) (*db_models.OweLimitConfiguration, error) {
	var err error

	result := []*db_models.OweLimitConfiguration{}

	err = db.
		Model(&db_models.OweLimitConfiguration{}).
		Where("team_id = ?", cfgTeamID).
		Where("(for_team_id = ? or is_default = true)", teamID).
		Find(&result).
		Error

	if err != nil {
		return nil, err
	}

	var defaultcfg, customcfg, cfg *db_models.OweLimitConfiguration
	for _, item := range result {
		if item.IsDefault {
			defaultcfg = item
		} else {
			customcfg = item
		}
	}

	if customcfg != nil {
		cfg = customcfg
	} else if defaultcfg != nil {
		cfg = defaultcfg
	}

	return cfg, nil
}
