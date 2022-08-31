package mets

import (
	"context"
	"fmt"
	"log"

	"github.com/mackerelio/mackerel-client-go"
	"golang.org/x/sync/errgroup"
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

type App struct {
	dryrun  bool
	targets []MetricsSource

	mkrlClient      *mackerel.Client
	mkrService      string
	mkrMetricPrefix string
}

type AppConfig struct {
	Dryrun bool

	MackerelAPIKey       string
	MackerelService      string
	MackerelMetricPrefix string
}

func New(config AppConfig) App {
	return App{
		dryrun: config.Dryrun,

		mkrlClient:      mackerel.NewClient(config.MackerelAPIKey),
		mkrService:      config.MackerelService,
		mkrMetricPrefix: config.MackerelMetricPrefix,
	}
}

func (app *App) RegisterTarget(target MetricsSource) error {
	app.targets = append(app.targets, target)
	return nil
}

func (app App) Run(ctx context.Context) error {
	if app.dryrun {
		log.Printf("[INFO] running as dryrun mode")
	}

	eg := new(errgroup.Group)

	for _, target := range app.targets {
		target := target // for goroutine

		eg.Go(func() error {
			log.Printf("[DEBUG] processing %s", target)

			values, err := target.FetchMetrics(ctx)
			if err != nil {
				return fmt.Errorf("failed to fetch metrics for %s: %w", target, err)
			}

			return app.ThrowMetricValues(ctx, values)
		})
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("failed to throw metrics: %w", err)
	}

	return nil
}

func (app App) ThrowMetricValues(ctx context.Context, values []*MetricValue) error {
	mkrValues := []*mackerel.MetricValue{}

	for _, v := range values {
		mkrValues = append(mkrValues, &mackerel.MetricValue{
			Name:  app.mkrMetricPrefix + "." + v.Label,
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
