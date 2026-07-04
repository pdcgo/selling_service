package shop

import "gorm.io/gorm"

// shopServiceImpl serves ShopService. Authorization is enforced by the access
// interceptor (registered in selling_service/register.go) from the
// (role_base.v1.request_policy) / (role_base.v1.use_scope) options on the proto,
// so handlers carry no imperative auth — they only scope queries by team.
type shopServiceImpl struct {
	db *gorm.DB
}

func NewShopService(db *gorm.DB) *shopServiceImpl {
	return &shopServiceImpl{
		db: db,
	}
}
