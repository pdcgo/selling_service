package revenue

import "gorm.io/gorm"

// revenueServiceImpl serves RevenueService. Authorization is enforced by the access
// interceptor (registered in selling_service/register.go) from the
// (role_base.v1.request_policy) options on the proto, so handlers carry no
// imperative auth. Skeleton — real revenue RPCs land here later.
type revenueServiceImpl struct {
	db *gorm.DB
}

func NewRevenueService(db *gorm.DB) *revenueServiceImpl {
	return &revenueServiceImpl{
		db: db,
	}
}
