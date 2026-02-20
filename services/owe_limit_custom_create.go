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
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// OweLimitCustomCreate implements selling_ifaceconnect.ConfigurationLimitServiceHandler.
func (c *configurationLimitServiceImpl) OweLimitCustomCreate(
	ctx context.Context,
	req *connect.Request[selling_iface.OweLimitCustomCreateRequest],
) (*connect.Response[selling_iface.OweLimitCustomCreateResponse], error) {
	var err error

	source, err := custom_connect.GetRequestSource(ctx)
	if err != nil {
		return &connect.Response[selling_iface.OweLimitCustomCreateResponse]{}, err
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
		return &connect.Response[selling_iface.OweLimitCustomCreateResponse]{}, err
	}

	db := c.db.WithContext(ctx)
	pay := req.Msg

	result := selling_iface.OweLimitCustomCreateResponse{}
	err = db.Transaction(func(tx *gorm.DB) error {
		conf := db_models.OweLimitConfiguration{
			TeamID:    pay.TeamId,
			Threshold: pay.Threshold,
			ForTeamID: &pay.ForTeamId,
			IsDefault: false,
		}
		err = tx.
			Clauses(clause.Locking{
				Strength: "UPDATE",
			}).
			Where("is_default = false").
			Where("team_id = ?", pay.TeamId).
			Where("for_team_id = ?", pay.ForTeamId).
			Find(&conf).
			Error

		if err != nil {
			return err
		}

		if conf.ID == 0 {
			err = tx.Save(&conf).Error
			if err != nil {
				return err
			}
			return nil
		}

		conf.Threshold = pay.Threshold
		err = tx.Save(&conf).Error
		if err != nil {
			return err
		}

		return nil
	})

	return connect.NewResponse(&result), nil
}
