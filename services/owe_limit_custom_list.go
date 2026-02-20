package services

import (
	"context"
	"strings"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/access_iface/v1"
	"github.com/pdcgo/schema/services/common/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/authorization"
	"github.com/pdcgo/shared/custom_connect"
	"github.com/pdcgo/shared/db_connect"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/interfaces/authorization_iface"
	"gorm.io/gorm"
)

// OweLimitCustomList implements selling_ifaceconnect.ConfigurationLimitServiceHandler.
func (c *configurationLimitServiceImpl) OweLimitCustomList(
	ctx context.Context,
	req *connect.Request[selling_iface.OweLimitCustomListRequest],
) (*connect.Response[selling_iface.OweLimitCustomListResponse], error) {
	var err error

	source, err := custom_connect.GetRequestSource(ctx)
	if err != nil {
		return &connect.Response[selling_iface.OweLimitCustomListResponse]{}, err
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
		return &connect.Response[selling_iface.OweLimitCustomListResponse]{}, err
	}

	pay := req.Msg
	db := c.db.WithContext(ctx)

	result := selling_iface.OweLimitCustomListResponse{
		Data:     []*selling_iface.OweLimitItem{},
		PageInfo: &common.PageInfo{},
	}

	var paginated *gorm.DB
	paginated, result.PageInfo, err = db_connect.SetPaginationQuery(db, func() (*gorm.DB, error) {
		query := db.
			Table("teams t")

		if pay.Q != "" {
			q := "%" + strings.ToLower(pay.Q) + "%"
			query = query.
				Where("t.name ilike ?", q)
		}

		// joining custom
		query = query.
			Joins(`
			left join owe_limit_configurations l on 
				l.for_team_id = t.id
				and l.team_id = ?
			left join owe_limit_configurations l2 on 
				l2.team_id = ?
				and l2.is_default = true
				and l.for_team_id is null
			`,
				pay.TeamId,
				pay.TeamId,
			).
			Where("t.id != ?", pay.TeamId)

		switch pay.Type {
		case common.TeamType_TEAM_TYPE_SELLING:
			query = query.
				Where("t.type = ?", "selling")
		case common.TeamType_TEAM_TYPE_WAREHOUSE:
			query = query.
				Where("t.type = ?", "warehouse")
		case common.TeamType_TEAM_TYPE_ADMIN:
			query = query.
				Where("t.type = ?", "admin")

		}

		query = query.
			Select([]string{
				"COALESCE(l.team_id, l2.team_id) as team_id",

				"t.name as team_name",
				"COALESCE(l.id, l2.id) as id",
				"COALESCE(l2.is_default, l.is_default) as is_default",
				"COALESCE(l.for_team_id, t.id) as for_team_id",
				"COALESCE(l.threshold, l2.threshold) as threshold",
			})

		return query, nil

	}, pay.Page)

	if err != nil {
		return &connect.Response[selling_iface.OweLimitCustomListResponse]{}, err
	}

	err = paginated.
		Order("t.name asc").
		Find(&result.Data).
		Error

	return connect.NewResponse(&result), err

}
