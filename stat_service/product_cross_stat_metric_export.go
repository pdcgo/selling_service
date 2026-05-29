package stat_service

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"log/slog"
	"os"
	"time"

	"connectrpc.com/connect"
	"github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/metric_opts/v1"
	"github.com/pdcgo/schema/services/selling_iface/v1/product_cross_metric/v1"
	"github.com/pdcgo/selling_service/stat_service/metric_base"
	"github.com/pdcgo/selling_service/stat_service/product_cross_metrics"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// ProductCrossStatMetricExport implements [selling_ifaceconnect.SellingStatServiceHandler].
func (s *statServiceImpl) ProductCrossStatMetricExport(
	ctx context.Context,
	streamreq *connect.Request[selling_iface.ProductCrossStatMetricExportRequest],
	stream *connect.ServerStream[selling_iface.ProductCrossStatMetricExportResponse],

) error {
	var err error

	w := io.MultiWriter(os.Stdout, ProductCrossWritter{stream})
	logger := slog.New(slog.NewTextHandler(w, nil))

	req := streamreq.Msg.Req

	db := s.db.WithContext(ctx)
	var sortbase metric_base.ProductCrossMetricBase

	// processing sort
	switch req.Sort.S.(type) {
	case *selling_iface.ProductCrossMetricSort_CommonSort:
		sortbase = product_cross_metrics.NewProductCommon(db)

	case *selling_iface.ProductCrossMetricSort_CostProductMetricSort:
		sortbase = product_cross_metrics.NewCostProductMetric(db)

	default:
		err = errors.New("invalid sort type")
		return err
	}

	productIdsChan := make(chan []uint64, 10)

	go func() {
		defer close(productIdsChan)
		err = sortbase.ProcessSortQuery(ctx, req.Filter, req.Sort, productIdsChan)
		if err != nil {
			logger.Error("error process sort", "error", err.Error())
		}
	}()

	csvWriter := csv.NewWriter(&ProductCrossCSVWritter{s: stream})
	// writing header
	headers, err := productCrossStatMetricExportHeader(req.MetricTypes)
	if err != nil {
		return err
	}

	csvWriter.Write(headers)
	csvWriter.Flush()

	// writing data
	for ids := range productIdsChan {
		for _, metType := range req.MetricTypes {
			metric, err := s.getMetric(ctx, ids, metType, req.Filter)
			if err != nil {
				return err
			}
			logger.Info("metric", "metric", metric)
			// writingMetric(metric)
		}
		csvWriter.Flush()

	}

	return err
}

func (s *statServiceImpl) getMetric(
	ctx context.Context,
	ids []uint64,
	metType selling_iface.ProductCrossMetricType,
	pfilter *selling_iface.ProductCrossStatMetricFilter,
) (*selling_iface.ProductCrossMetric, error) {
	var err error
	defaultExpiration := time.Minute
	var metric *selling_iface.ProductCrossMetric = &selling_iface.ProductCrossMetric{}
	var metricbase metric_base.ProductCrossMetricBase

	switch metType {
	case selling_iface.ProductCrossMetricType_PRODUCT_CROSS_METRIC_TYPE_PRODUCT_COST:
		metricbase = product_cross_metrics.NewCostProductMetric(s.db.WithContext(ctx))
	default:
		err = errors.New("invalid metric type")
	}

	err = s.cacheMgr.Get(ctx, &listKey{
		Ids:        ids,
		MetricName: metType.String(),
	}, metric)

	if err == nil {
		return metric, nil
	}

	slog.Info("getting fresh metric", "metricType", metType, "error", err)

	// jika cache tidak ada
	metric, err = metricbase.FetchMetric(ctx, ids, pfilter)
	if err != nil {
		return metric, err
	}

	err = s.cacheMgr.Set(ctx, &listKey{
		Ids:        ids,
		MetricName: metType.String(),
	}, metric, defaultExpiration)

	if err != nil {
		return metric, err
	}

	return metric, nil
}

func productCrossStatMetricExportHeader(metricType []selling_iface.ProductCrossMetricType) ([]string, error) {
	fieldNames := []string{}
	for _, mt := range metricType {
		var metric proto.Message
		switch mt {
		case selling_iface.ProductCrossMetricType_PRODUCT_CROSS_METRIC_TYPE_PRODUCT_COST:
			metric = &product_cross_metric.CostProductItem{}
		case selling_iface.ProductCrossMetricType_PRODUCT_CROSS_METRIC_TYPE_COMMON:
			metric = &product_cross_metric.CommonProductCrossItem{}

		default:
			return nil, errors.New("invalid metric type")
		}

		pbDesc := metric.ProtoReflect().Descriptor()

		fields := pbDesc.Fields()

		for i := 0; i < fields.Len(); i++ {
			fd := fields.Get(i)
			fOpts := fd.Options().(*descriptorpb.FieldOptions)

			label := proto.GetExtension(fOpts, metric_opts.E_CsvName).(string)
			fieldNames = append(fieldNames, label)
		}
	}

	if len(fieldNames) == 0 {
		return nil, errors.New("no metric types csv header found")
	}
	return fieldNames, nil
}

type ProductCrossWritter struct {
	s *connect.ServerStream[selling_iface.ProductCrossStatMetricExportResponse]
}

// Write implements [io.Writer].
func (s ProductCrossWritter) Write(p []byte) (n int, err error) {
	s.s.Send(&selling_iface.ProductCrossStatMetricExportResponse{
		Message: string(p),
	})
	return len(p), nil
}

type ProductCrossCSVWritter struct {
	s *connect.ServerStream[selling_iface.ProductCrossStatMetricExportResponse]
}

// Write implements [io.Writer].
func (s *ProductCrossCSVWritter) Write(p []byte) (n int, err error) {
	err = s.s.Send(&selling_iface.ProductCrossStatMetricExportResponse{
		Data: p,
	})
	return len(p), nil
}
