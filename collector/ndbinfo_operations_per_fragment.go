package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoOperationsPerFragmentQuery = `
	SELECT 
		SUM(tot_key_reads)    AS total_key_reads,
		SUM(tot_key_inserts)  AS total_key_writes,
		SUM(tot_key_updates)  AS total_key_updates,
		SUM(tot_key_deletes)  AS total_key_deletes,
		type
	FROM ndbinfo.operations_per_fragment
	GROUP BY type;
`

var (
	ndbinfoTotalKeyReadsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_key_reads"),
		"Total number of key read operations",
		[]string{"type"}, nil,
	)
	ndbinfoTotalKeyWritesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_key_writes"),
		"Total number of key write operations",
		[]string{"type"}, nil,
	)
	ndbinfoTotalKeyUpdatesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_key_updates"),
		"Total number of key update operations",
		[]string{"type"}, nil,
	)
	ndbinfoTotalKeyDeletesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_key_deletes"),
		"Total number of key delete operations",
		[]string{"type"}, nil,
	)
)

// ScrapeNdbinfoOperationsPerFragment collects metrics from `ndbinfo.operations_per_fragment`.
type ScrapeNdbinfoOperationsPerFragment struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNdbinfoOperationsPerFragment) Name() string {
	return "ndbinfo.operations_per_fragment"
}

// Help describes the role of the Scraper.
func (ScrapeNdbinfoOperationsPerFragment) Help() string {
	return "Collect metrics from ndbinfo.operations_per_fragment"
}

// Version of MySQL from which scraper is available.
func (ScrapeNdbinfoOperationsPerFragment) Version() float64 {
	return 5.7
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNdbinfoOperationsPerFragment) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoOperationsPerFragmentQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		totalKeyReads, totalKeyWrites, totalKeyUpdates, totalKeyDeletes float64
		operationType                                                   string
	)

	logger.Debug("Scraping ndbinfo.operations_per_fragment", "query", ndbinfoOperationsPerFragmentQuery)

	for rows.Next() {
		if err := rows.Scan(&totalKeyReads, &totalKeyWrites, &totalKeyUpdates, &totalKeyDeletes, &operationType); err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalKeyReadsDesc, prometheus.GaugeValue, totalKeyReads, operationType,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalKeyWritesDesc, prometheus.GaugeValue, totalKeyWrites, operationType,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalKeyUpdatesDesc, prometheus.GaugeValue, totalKeyUpdates, operationType,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalKeyDeletesDesc, prometheus.GaugeValue, totalKeyDeletes, operationType,
		)
	}

	return rows.Err()
}
