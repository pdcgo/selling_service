package main

import (
	"context"
	"log/slog"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/pdcgo/san_collection/san_pubsub"
	"github.com/pdcgo/shared/custom_connect"
	"github.com/urfave/cli/v3"
	"go.opentelemetry.io/otel"
)

type StatementConfig struct {
	SellingSubname string
}

type StatementFunc cli.ActionFunc

func NewStatementFunc(cfg *StatementConfig) StatementFunc {
	return func(ctx context.Context, c *cli.Command) error {
		canceltrace, err := custom_connect.InitTracer("selling-service")
		if err != nil {
			return err
		}

		defer canceltrace(ctx)

		client, err := san_pubsub.NewPubSubClientWithContext(ctx)
		if err != nil {
			panic(err)
		}

		subscribe := client.Subscriber(cfg.SellingSubname)
		slog.Info("listening", "subname", cfg.SellingSubname)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		iddleDuration := time.Minute
		timeout := time.AfterFunc(iddleDuration, cancel)

		subscribe.ReceiveSettings.MaxOutstandingMessages = 10
		subscribe.ReceiveSettings.NumGoroutines = 3

		err = subscribe.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			timeout.Reset(iddleDuration)

			ctx, span := otel.Tracer("").Start(ctx, "event/statement/receive")
			defer span.End()

			// err := batchHandler(ctx, m)
			// if err != nil {
			// 	slog.Error("error processing event", "err", err.Error())
			// 	span.RecordError(err)
			// 	span.SetStatus(codes.Error, err.Error())
			// 	m.Nack()
			// 	return
			// }
			m.Ack()
		})

		if err != nil {
			return err
		}

		slog.Info("stopped", "subname", cfg.SellingSubname)

		return err
	}
}
