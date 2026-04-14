package stat_service

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/stat_service/metrics"
	"github.com/pdcgo/shared/db_models"
	"gorm.io/gorm"
)

func (s *statServiceImpl) Stat(
	ctx context.Context,
	req *connect.Request[selling_iface.StatRequest]) (*connect.Response[selling_iface.StatResponse], error) {
	var err error
	result := selling_iface.StatResponse{
		Metrics: []*selling_iface.Metric{},
	}

	for _, metType := range req.Msg.MetricTypes {
		var metric *selling_iface.Metric

		switch metType {
		case selling_iface.MetricType_METRIC_TYPE_ORDER_ACTIVE:
			metric, err = metrics.NewOrderActiveMetric(s.db, req.Msg.Filter)
			if err != nil {
				return nil, err
			}

		case selling_iface.MetricType_METRIC_TYPE_HISTORY_ORDER:
			metric, err = metrics.NewHistoryOrderMetric(s.db, req.Msg.Filter, req.Msg.Range)
			if err != nil {
				return nil, err
			}

		case selling_iface.MetricType_METRIC_TYPE_PRODUCT_SOLD:
			metric, err = metrics.NewProductSoldMetric(s.db, req.Msg.Filter, req.Msg.Range)
			if err != nil {
				return nil, err
			}

		case selling_iface.MetricType_METRIC_TYPE_HISTORY_PRODUCT_SOLD:
			metric, err = metrics.NewHistoryProductSoldMetric(s.db, req.Msg.Filter, req.Msg.Range)
			if err != nil {
				return nil, err
			}

		case selling_iface.MetricType_METRIC_TYPE_ONGOING_STOCK:
			metric, err = NewOngoingStockMetric(s.db, req.Msg.Filter)
			if err != nil {
				return nil, err
			}
		case selling_iface.MetricType_METRIC_TYPE_READY_STOCK:
			metric, err = NewReadyStockMetric(s.db, req.Msg.Filter)
			if err != nil {
				return nil, err
			}
		case selling_iface.MetricType_METRIC_TYPE_TOTAL_STOCK:
			metric = &selling_iface.Metric{
				Data: &selling_iface.Metric_TotalStock{},
			}

			ongoMetric, err := NewOngoingStockMetric(s.db, req.Msg.Filter)
			if err != nil {
				return nil, err
			}
			readyMetric, err := NewReadyStockMetric(s.db, req.Msg.Filter)
			if err != nil {
				return nil, err
			}
			metric.Data = &selling_iface.Metric_TotalStock{
				TotalStock: &selling_iface.TotalStockMetric{
					Type:          selling_iface.MetricType_METRIC_TYPE_TOTAL_STOCK,
					TotalCount:    ongoMetric.Data.(*selling_iface.Metric_OngoingStock).OngoingStock.TotalCount + readyMetric.Data.(*selling_iface.Metric_ReadyStock).ReadyStock.TotalCount,
					TotalSkuCount: ongoMetric.Data.(*selling_iface.Metric_OngoingStock).OngoingStock.TotalSkuCount + readyMetric.Data.(*selling_iface.Metric_ReadyStock).ReadyStock.TotalSkuCount,
					TotalAmount:   ongoMetric.Data.(*selling_iface.Metric_OngoingStock).OngoingStock.TotalAmount + readyMetric.Data.(*selling_iface.Metric_ReadyStock).ReadyStock.TotalAmount,
				},
			}
		case selling_iface.MetricType_METRIC_TYPE_PAYABLE:
			metric, err = NewPayableMetric(s.db, req.Msg.Filter)
			if err != nil {
				return nil, err
			}
		case selling_iface.MetricType_METRIC_TYPE_RECEIVABLE:
			metric, err = NewReceivableMetric(s.db, req.Msg.Filter)
			if err != nil {
				return nil, err
			}
		}

		result.Metrics = append(result.Metrics, metric)

	}

	return connect.NewResponse(&result), nil
}

func NewReceivableMetric(db *gorm.DB, filter *selling_iface.StatFilter) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.ReceivableMetric{
		Type: selling_iface.MetricType_METRIC_TYPE_RECEIVABLE,
	}

	if filter.TeamId == 0 {
		return nil, errors.New("metric receivable must set team id")
	}

	recvQ := db.Model(&db_models.Invoice{}).
		Where("invoices.status = ?", db_models.InvoiceNotPaid).
		Where("invoices.to_team_id = ?", filter.TeamId)

	selects := []string{
		"count(invoices.id) As invoice_count",
		"sum(invoices.amount) AS amount",
	}
	err = recvQ.Session(&gorm.Session{}).
		Select(selects).
		Find(&result).
		Error
	if err != nil {
		return nil, err
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_Receivable{
			Receivable: &result,
		},
	}, nil
}

func NewPayableMetric(db *gorm.DB, filter *selling_iface.StatFilter) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.PayableMetric{
		Type: selling_iface.MetricType_METRIC_TYPE_PAYABLE,
	}

	if filter.TeamId == 0 {
		return nil, errors.New("metric payable must set team id")
	}

	payQ := db.Model(&db_models.Invoice{}).
		Where("invoices.from_team_id = ?", filter.TeamId).
		Where("invoices.status = ?", db_models.InvoiceNotPaid).
		Where("invoices.amount > ?", 0)

	selects := []string{
		"count(invoices.id) AS invoice_count",
		"sum(invoices.amount) AS amount",
	}
	err = payQ.Session(&gorm.Session{}).
		Select(selects).
		Find(&result).
		Error

	if err != nil {
		return nil, err
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_Payable{
			Payable: &result,
		},
	}, nil
}

func NewReadyStockMetric(db *gorm.DB, filter *selling_iface.StatFilter) (*selling_iface.Metric, error) {
	var err error
	result := selling_iface.ReadyStockMetric{
		Type: selling_iface.MetricType_METRIC_TYPE_READY_STOCK,
	}

	// select
	// 	sum(-1 * ih.count) as total_count,
	// 	count(ih.sku_id) as total_sku_count,
	// 	sum((-1 * ih.count) * (ih.price + coalesce(ih.ext_price, 0))) as total_amount
	// from public.invertory_histories ih
	// where
	// 	ih.tx_id is null

	readyQ := db.
		Table("public.invertory_histories ih").
		Where("ih.tx_id is null")

	if filter.TeamId != 0 {
		readyQ = readyQ.Where("ih.team_id = ?", filter.TeamId)
	}

	selects := []string{
		"sum(-1 * ih.count) as total_count",
		"count(ih.sku_id) as total_sku_count",
		"sum((-1 * ih.count) * (ih.price + coalesce(ih.ext_price, 0))) as total_amount",
	}

	err = readyQ.
		Session(&gorm.Session{}).
		Select(selects).
		Find(&result).Error
	if err != nil {
		return nil, err
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_ReadyStock{
			ReadyStock: &result,
		},
	}, nil
}

func NewOngoingStockMetric(db *gorm.DB, filter *selling_iface.StatFilter) (*selling_iface.Metric, error) {

	var err error
	result := selling_iface.OngoingStockMetric{
		Type: selling_iface.MetricType_METRIC_TYPE_ONGOING_STOCK,
	}

	ongoingType := []db_models.InvTxType{
		db_models.InvTxRestock,
		db_models.InvTxReturn,
		db_models.InvTxAdjRestock,
		db_models.InvTxTransferIn,
	}

	selects := []string{
		"sum(it.total) as total_amount",
	}

	for _, tipe := range ongoingType {
		selects = append(
			selects,
			fmt.Sprintf("sum(it.total) filter (where it.type = '%s') as %s_amount", tipe, tipe),
		)
	}

	amountQ := db.
		Table("public.inv_transactions it").
		Where("it.type in ?", ongoingType).
		Where("it.status", db_models.InvTxOngoing)

	if filter.TeamId != 0 {
		amountQ = amountQ.
			Where("it.team_id = ?", filter.TeamId)
	}

	err = amountQ.
		Session(&gorm.Session{}).
		Select(selects).
		Find(&result).Error
	if err != nil {
		return nil, err
	}

	countQ := amountQ.
		Joins("join public.inv_tx_items iti on iti.inv_transaction_id = it.id")

	selects = []string{
		"count(distinct iti.sku_id) as total_sku_count",
		"sum(iti.count) as total_count",
	}

	for _, tipe := range ongoingType {
		selects = append(
			selects,
			fmt.Sprintf("count(distinct iti.sku_id) filter (where it.type = '%s') as %s_sku_count", tipe, tipe),
			fmt.Sprintf("sum(iti.count) filter (where it.type = '%s') as %s_count", tipe, tipe),
		)
	}

	err = countQ.
		Session(&gorm.Session{}).
		Select(selects).
		Find(&result).Error
	if err != nil {
		return nil, err
	}

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_OngoingStock{
			OngoingStock: &result,
		},
	}, nil
}

func NewTotalStockMetric(db *gorm.DB) (*selling_iface.Metric, error) {

	return &selling_iface.Metric{
		Data: &selling_iface.Metric_TotalStock{},
	}, nil
}
