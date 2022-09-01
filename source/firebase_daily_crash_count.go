package source

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/handlename/mets"
	"google.golang.org/api/iterator"
)

type MetricsSourceFirebaseDailyCrashCount struct {
	projectId   string
	tableName   string
	labelPrefix string
}

type MetricsSourceFirebaseDailyCrashCountConfig struct {
	ProjectId   string
	TableName   string
	LabelPrefix string
}

func NewMetricsSourceDailyCrashCount(config MetricsSourceFirebaseDailyCrashCountConfig) *MetricsSourceFirebaseDailyCrashCount {
	return &MetricsSourceFirebaseDailyCrashCount{
		projectId:   config.ProjectId,
		tableName:   config.TableName,
		labelPrefix: config.LabelPrefix,
	}
}

func (ms MetricsSourceFirebaseDailyCrashCount) String() string {
	return fmt.Sprintf("MetricsSourceDailyCrashCount{projectId:%s tableName:%s}", ms.projectId, ms.tableName)
}

func (ms MetricsSourceFirebaseDailyCrashCount) FetchMetrics(ctx context.Context) ([]*mets.MetricValue, error) {
	log.Printf("[DEBUG] start to fetch metrics by %s", ms)

	var err error

	client, err := bigquery.NewClient(ctx, ms.projectId)
	if err != nil {
		return nil, fmt.Errorf("failed to init bigquery client: %w", err)
	}

	// exported reports from Crashlytics to Bigquery has delay over 24 hours
	ts := time.Now().Truncate(24 * time.Hour)
	queryTs := ts.Add(-2 * 24 * time.Hour)
	rawQuery, err := ms.buildQuery(queryTs)

	log.Printf("[DEBUG] query:%s", rawQuery)

	query := client.Query(rawQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	it, err := query.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query daily crash count: %w", err)
	}

	values := []*mets.MetricValue{}

	for {
		var vs []bigquery.Value
		err := it.Next(&vs)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate daily crash count query result: %w", err)
		}

		errorType := vs[1].(string)
		count := vs[2].(int64)
		countUnique := vs[3].(int64)

		values = append(
			values,
			&mets.MetricValue{
				Label: fmt.Sprintf("%s.%s.count", ms.labelPrefix, errorType),
				Time:  ts.Unix(),
				Value: float64(count),
			},
			&mets.MetricValue{
				Label: fmt.Sprintf("%s.%s.count_unique", ms.labelPrefix, errorType),
				Time:  ts.Unix(),
				Value: float64(countUnique),
			},
		)
	}

	log.Printf("[DEBUG] done to fetch metrics by %s", ms)

	return values, nil
}

var metricsSourceDailyCrashCountTmpl = template.Must(template.New("query").Parse(`
SELECT
  DATE(event_timestamp) AS day,
  error_type,
  COUNT(*) AS num,
  COUNT(DISTINCT(installation_uuid)) AS num_uniq
FROM
` + "`{{ .tableName }}`" + `
WHERE
  DATE(event_timestamp) = "{{ .date }}"
GROUP BY
  day,
  error_type
ORDER BY
  day,
  error_type DESC
LIMIT
  1000;
`))

func (ms MetricsSourceFirebaseDailyCrashCount) buildQuery(ts time.Time) (string, error) {
	input := map[string]string{
		"tableName": ms.tableName,
		"date":      ts.Format("2006-01-02"),
	}

	var buf bytes.Buffer
	if err := metricsSourceDailyCrashCountTmpl.Execute(&buf, input); err != nil {
		return "", fmt.Errorf("failed to execute query template: %w", err)
	}

	return buf.String(), nil
}

func convertBigqueryValueToFloat64(value bigquery.Value) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int64:
		return float64(v)
	default:
		log.Printf("[WARN] unknown type: %+v", v)
		return 0
	}
}
