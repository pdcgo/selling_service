package shop

import (
	"context"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/selling_models"
)

// ShopUserList implements [selling_ifaceconnect.ShopServiceHandler]. Lists the users
// who can access a shop. Display names are resolved by the caller (frontend) via the
// user service, keeping selling_service DB-local.
func (s *shopServiceImpl) ShopUserList(
	ctx context.Context,
	req *connect.Request[selling_iface.ShopUserListRequest],
) (*connect.Response[selling_iface.ShopUserListResponse], error) {
	pay := req.Msg

	db := s.db.WithContext(ctx)

	err := shopInTeam(db, pay.ShopId, pay.TeamId)
	if err != nil {
		return nil, err
	}

	result := &selling_iface.ShopUserListResponse{
		Data: []*selling_iface.ShopUser{},
	}

	var rows []*selling_models.ShopUser
	err = db.
		Where("shop_id = ?", pay.ShopId).
		Order("id ASC").
		Find(&rows).
		Error
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		if row == nil {
			continue
		}
		result.Data = append(result.Data, &selling_iface.ShopUser{
			Id:     row.ID,
			ShopId: row.ShopID,
			UserId: row.UserID,
		})
	}

	return connect.NewResponse(result), nil
}
