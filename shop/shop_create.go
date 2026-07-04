package shop

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_models"
)

// ShopCreate implements [selling_ifaceconnect.ShopServiceHandler].
func (s *shopServiceImpl) ShopCreate(
	ctx context.Context,
	req *connect.Request[selling_iface.ShopCreateRequest],
) (*connect.Response[selling_iface.ShopCreateResponse], error) {
	pay := req.Msg

	shop := &db_models.V2Shop{
		TeamID:          pay.TeamId,
		ShopName:        pay.ShopName,
		ShopUsername:    pay.ShopUsername,
		MarketplaceType: int32(pay.MarketplaceType),
		URI:             pay.Uri,
	}

	db := s.db.WithContext(ctx)
	err := db.Create(shop).Error
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&selling_iface.ShopCreateResponse{Id: shop.ID}), nil
}
