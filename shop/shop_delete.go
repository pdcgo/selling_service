package shop

import (
	"context"
	"strconv"
	"time"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

// ShopDelete implements [selling_ifaceconnect.ShopServiceHandler]. Soft delete: the
// username is renamed (`[username]_deleted_[unix]`) so a same-named shop can be
// re-created without colliding on the (team_id, shop_username) unique index.
func (s *shopServiceImpl) ShopDelete(
	ctx context.Context,
	req *connect.Request[selling_iface.ShopDeleteRequest],
) (*connect.Response[selling_iface.ShopDeleteResponse], error) {
	pay := req.Msg

	now := time.Now()
	ts := strconv.FormatInt(now.UnixMilli(), 10)

	db := s.db.WithContext(ctx)
	err := db.
		Model(db_models.V2Shop{}).
		Where("id = ? AND team_id = ?", pay.Id, pay.TeamId).
		Updates(map[string]interface{}{
			"shop_username": gorm.Expr("shop_username || ?", "_deleted_"+ts),
			"deleted_at":    now,
		}).
		Error
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&selling_iface.ShopDeleteResponse{}), nil
}
