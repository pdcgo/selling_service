package metric_base

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

type redisSortCacheManager struct {
	config *SortCacheManagerConfig
	client *redis.Client
}

func NewRedisSortCacheManager(config *SortCacheManagerConfig, client *redis.Client) SortCacheManager {
	return &redisSortCacheManager{
		config: config,
		client: client,
	}
}

// cacheKey builds a deterministic Redis key by hashing the proto-encoded filter and sort messages.
func (m *redisSortCacheManager) cacheKey(pfilter *selling_iface.ProductStatMetricFilter, psort *selling_iface.ProductMetricSort) string {
	h := sha256.New()

	if pfilter != nil {
		b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(pfilter)
		h.Write(b)
	}

	h.Write([]byte("|")) // separator

	if psort != nil {
		b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(psort)
		h.Write(b)
	}

	return fmt.Sprintf("product_metric:sort_cache:%x", h.Sum(nil))
}

func (m *redisSortCacheManager) GetSortCache(pfilter *selling_iface.ProductStatMetricFilter, psort *selling_iface.ProductMetricSort) ([]uint64, error) {
	ctx := context.Background()
	key := m.cacheKey(pfilter, psort)

	data, err := m.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // cache miss — not an error
		}
		return nil, err
	}

	var productIds []uint64
	if err := json.Unmarshal(data, &productIds); err != nil {
		return nil, err
	}

	return productIds, nil
}

func (m *redisSortCacheManager) SetSortCache(pfilter *selling_iface.ProductStatMetricFilter, psort *selling_iface.ProductMetricSort, productIds []uint64) error {
	ctx := context.Background()
	key := m.cacheKey(pfilter, psort)

	data, err := json.Marshal(productIds)
	if err != nil {
		return err
	}

	return m.client.Set(ctx, key, data, m.config.ExpiredDuration).Err()
}
