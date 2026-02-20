package services

import (
	"context"

	"cloud.google.com/go/firestore"
	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/interfaces/authorization_iface"
	"github.com/pdcgo/shared/pkg/ware_cache"
	"gorm.io/gorm"
)

type configurationLimitServiceImpl struct {
	db     *gorm.DB
	client *firestore.Client
	auth   authorization_iface.Authorization
	cache  ware_cache.Cache
}

// PublicTeamList implements selling_ifaceconnect.ConfigurationLimitServiceHandler.
func (c *configurationLimitServiceImpl) LimitInvoice(
	ctx context.Context,
	req *connect.Request[selling_iface.LimitInvoiceRequest],
) (*connect.Response[selling_iface.LimitInvoiceResponse], error) {

	var err error
	err = c.
		auth.
		AuthIdentityFromHeader(req.Header()).
		Err()

	if err != nil {
		return &connect.Response[selling_iface.LimitInvoiceResponse]{}, err
	}

	pay := req.Msg
	db := c.db.WithContext(ctx)
	result := selling_iface.LimitInvoiceResponse{
		Data: []*selling_iface.LimitInvoiceItem{},
	}

	var fromTeamIds []uint64
	var toTeamIds []uint64
	for _, reqItem := range pay.Limit {
		fromTeamIds = append(fromTeamIds, reqItem.FromTeamId)
		toTeamIds = append(toTeamIds, reqItem.ToTeamId)
	}

	var configs []*db_models.InvoiceLimitConfiguration
	err = db.Model(&db_models.InvoiceLimitConfiguration{}).
		Where("team_id IN (?) AND for_team_id IN (?)", fromTeamIds, toTeamIds).
		Find(&configs).
		Error

	if err != nil {
		return &connect.Response[selling_iface.LimitInvoiceResponse]{}, err
	}

	for _, config := range configs {
		if config == nil {
			continue
		}

		result.Data = append(result.Data, &selling_iface.LimitInvoiceItem{
			Id:         uint64(config.ID),
			FromTeamId: uint64(config.TeamID),
			ToTeamId:   uint64(*config.ForTeamID),
			Threshold:  config.Threshold,
		})
	}

	return connect.NewResponse(&result), nil
}

func NewConfigurationService(
	db *gorm.DB,
	client *firestore.Client,
	auth authorization_iface.Authorization,
	cache ware_cache.Cache) *configurationLimitServiceImpl {
	return &configurationLimitServiceImpl{
		db,
		client,
		auth,
		cache,
	}
}
