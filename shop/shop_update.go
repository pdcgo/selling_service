package shop

import (
	"context"
	"time"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/selling_models"
)

// ShopUpdate implements [selling_ifaceconnect.ShopServiceHandler].
func (s *shopServiceImpl) ShopUpdate(
	ctx context.Context,
	req *connect.Request[selling_iface.ShopUpdateRequest],
) (*connect.Response[selling_iface.ShopUpdateResponse], error) {
	pay := req.Msg

	db := s.db.WithContext(ctx)
	err := db.
		Model(&selling_models.Shop{}).
		Where("id = ? AND team_id = ?", pay.Id, pay.TeamId).
		Updates(map[string]interface{}{
			"shop_name":        pay.ShopName,
			"shop_username":    pay.ShopUsername,
			"marketplace_type": int32(pay.MarketplaceType),
			"uri":              pay.Uri,
			"updated_at":       time.Now(),
		}).
		Error
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&selling_iface.ShopUpdateResponse{}), nil
}
