package revenue_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	selling_iface "github.com/pdcgo/schema/services/selling_iface/v1"
	"github.com/pdcgo/selling_service/revenue"
	"github.com/stretchr/testify/assert"
)

func TestHelloWorld(t *testing.T) {
	svc := revenue.NewRevenueService(nil)

	res, err := svc.HelloWorld(context.Background(), connect.NewRequest(&selling_iface.HelloWorldRequest{Name: "pdc"}))
	assert.NoError(t, err)
	assert.Equal(t, "hello pdc", res.Msg.Message)

	res, err = svc.HelloWorld(context.Background(), connect.NewRequest(&selling_iface.HelloWorldRequest{}))
	assert.NoError(t, err)
	assert.Equal(t, "hello world", res.Msg.Message)
}
