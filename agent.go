package mets

import (
	"context"
	"fmt"
	"log"

	"github.com/mackerelio/mackerel-client-go"
)

type MetricsSource interface {
	String() string
	FetchMetrics(context.Context) ([]*MetricValue, error)
}

type MetricValue struct {
	Label string
	Time  int64
	Value interface{}
}

type Agent struct {
	dryrun  bool
	sources []MetricsSource

	mkrlClient      *mackerel.Client
	mkrService      string
	mkrMetricPrefix string
}

type AgentConfig struct {
	Dryrun bool

	MackerelAPIKey       string
	MackerelService      string
	MackerelMetricPrefix string
}

func NewAgent(config AgentConfig) Agent {
	return Agent{
		dryrun: config.Dryrun,

		mkrlClient:      mackerel.NewClient(config.MackerelAPIKey),
		mkrService:      config.MackerelService,
		mkrMetricPrefix: config.MackerelMetricPrefix,
	}
}

func (app *Agent) RegisterMetricSource(target MetricsSource) error {
	app.sources = append(app.sources, target)
	return nil
}

func (app Agent) Run(ctx context.Context) error {
	if app.dryrun {
		log.Printf("[INFO] running as dryrun mode")
	}

	for _, target := range app.sources {
		log.Printf("[DEBUG] processing %s", target)

		values, err := target.FetchMetrics(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch metrics for %s: %w", target, err)
		}

		if err := app.ThrowMetricValues(ctx, values); err != nil {
			return fmt.Errorf("failed to throw metric values: %w", err)
		}
	}

	return nil
}

func (app Agent) ThrowMetricValues(ctx context.Context, values []*MetricValue) error {
	mkrValues := []*mackerel.MetricValue{}

	for _, v := range values {
		mkrValues = append(mkrValues, &mackerel.MetricValue{
			Name:  fmt.Sprintf("%s.%s", app.mkrMetricPrefix, v.Label),
			Time:  v.Time,
			Value: v.Value,
		})
	}

	log.Printf("[INFO] will throw metrics to service '%s':", app.mkrService)
	for _, v := range mkrValues {
		log.Printf("[INFO] %s", MkrMetricValueString(v))
	}

	if app.dryrun {
		log.Printf("[INFO] not throwed (dryrun)")
		return nil
	}

	if err := app.mkrlClient.PostServiceMetricValues(app.mkrService, mkrValues); err != nil {
		return fmt.Errorf("failed to post metrics to mackerel: %w", err)
	}

	if !app.dryrun {
		log.Printf("[INFO] metrics throwed")
	}

	return nil
}

func MkrMetricValueString(v *mackerel.MetricValue) string {
	if v == nil {
		return "[nil]"
	}

	return fmt.Sprintf("[name: %s time: %d value: %s]", v.Name, v.Time, v.Value)
}
