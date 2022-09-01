package source

import (
	"context"
	"time"

	"github.com/handlename/mets"
)

type MetricsSourceMock struct {
	LabePrefix string
}

func (ms MetricsSourceMock) String() string {
	return "MetricsSourceMock"
}

func (ms MetricsSourceMock) FetchMetrics(ctx context.Context) ([]*mets.MetricValue, error) {
	return []*mets.MetricValue{
		{
			Label: "dummy.A",
			Time:  time.Now().Unix(),
			Value: 1.1111,
		},
		{
			Label: "dummy.B",
			Time:  time.Now().Unix(),
			Value: 2.2222,
		},
		{
			Label: "dummy.C",
			Time:  time.Now().Unix(),
			Value: 3.3333,
		},
	}, nil
}
