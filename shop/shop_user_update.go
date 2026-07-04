package shop

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

// shopInTeam verifies a shop exists and belongs to the given team.
func shopInTeam(db *gorm.DB, shopID, teamID uint64) error {
	var shop db_models.V2Shop
	err := db.
		Select("id").
		Where("id = ? AND team_id = ?", shopID, teamID).
		Limit(1).
		Find(&shop).
		Error
	if err != nil {
		return err
	}
	if shop.ID == 0 {
		return connect.NewError(connect.CodeNotFound, errors.New("shop not found in team"))
	}
	return nil
}

// ShopUserUpdate implements [selling_ifaceconnect.ShopServiceHandler]. Adds and removes
// users from a shop's access list; removal is a soft delete.
func (s *shopServiceImpl) ShopUserUpdate(
	ctx context.Context,
	req *connect.Request[selling_iface.ShopUserUpdateRequest],
) (*connect.Response[selling_iface.ShopUserUpdateResponse], error) {
	pay := req.Msg

	db := s.db.WithContext(ctx)

	err := shopInTeam(db, pay.ShopId, pay.TeamId)
	if err != nil {
		return nil, err
	}

	err = db.Transaction(func(tx *gorm.DB) error {
		for _, uid := range pay.AddedUserIds {
			var existing db_models.V2ShopUser
			e := tx.
				Unscoped().
				Where("shop_id = ? AND user_id = ?", pay.ShopId, uid).
				Limit(1).
				Find(&existing).
				Error
			if e != nil {
				return e
			}

			if existing.ID == 0 {
				e = tx.
					Create(&db_models.V2ShopUser{
						ShopID: pay.ShopId,
						TeamID: pay.TeamId,
						UserID: uid,
					}).
					Error
				if e != nil {
					return e
				}
				continue
			}

			// Reactivate a previously removed access row.
			e = tx.
				Unscoped().
				Model(&db_models.V2ShopUser{}).
				Where("id = ?", existing.ID).
				Update("deleted_at", nil).
				Error
			if e != nil {
				return e
			}
		}

		for _, uid := range pay.RemovedUserIds {
			e := tx.
				Where("shop_id = ? AND user_id = ?", pay.ShopId, uid).
				Delete(&db_models.V2ShopUser{}).
				Error
			if e != nil {
				return e
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&selling_iface.ShopUserUpdateResponse{}), nil
}
