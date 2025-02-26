// Copyright 2019 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Scrape `ndbinfo.memoryusage`

package collector

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoMemoryusageQuery = `
	SELECT node_id, memory_type, used, used_pages, total, total_pages 
	FROM ndbinfo.memoryusage;
	`

var (
	ndbinfoMemoryusageUsedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "memory_used"),
		"Memory used for each node and memory type in bytes",
		[]string{"nodeID", "memoryType"}, nil,
	)

	ndbinfoMemoryusageTotalDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "memory_total"),
		"Total memory configured for each node and memory type in bytes",
		[]string{"nodeID", "memoryType"}, nil,
	)

	ndbinfoMemoryusagePagesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "memory_pages"),
		"Number of pages used for each node and memory type",
		[]string{"nodeID", "memoryType"}, nil,
	)

	ndbinfoMemoryusageTotalPagesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "memory_total_pages"),
		"Total number of pages available for each node and memory type",
		[]string{"nodeID", "memoryType"}, nil,
	)
)

// ScrapeNdbinfoMemoryusage collects for `ndbinfo.memoryusage`
type ScrapeNdbinfoMemoryusage struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNdbinfoMemoryusage) Name() string {
	return "ndbinfo.memoryusage"
}

// Help describes the role of the Scraper
func (ScrapeNdbinfoMemoryusage) Help() string {
	return "Collect metrics from ndbinfo.memoryusage"
}

// Version of MySQL from which scraper is available
func (ScrapeNdbinfoMemoryusage) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric
func (ScrapeNdbinfoMemoryusage) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	ndbinfoMemoryusageRows, err := db.QueryContext(ctx, ndbinfoMemoryusageQuery)
	if err != nil {
		logger.Error("Error querying ndbinfo.memoryusage", "err", err)
		return err
	}
	defer ndbinfoMemoryusageRows.Close()

	var (
		nodeID, used, total, usedPages, totalPages uint64
		memoryType                                 string
	)

	// Iterate over the memory settings
	for ndbinfoMemoryusageRows.Next() {
		if err := ndbinfoMemoryusageRows.Scan(
			&nodeID, &memoryType, &used,
			&usedPages, &total, &totalPages); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoMemoryusageUsedDesc, prometheus.GaugeValue, float64(used),
			strconv.FormatUint(nodeID, 10), memoryType)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoMemoryusageTotalDesc, prometheus.GaugeValue, float64(total),
			strconv.FormatUint(nodeID, 10), memoryType)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoMemoryusagePagesDesc, prometheus.GaugeValue, float64(usedPages),
			strconv.FormatUint(nodeID, 10), memoryType)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoMemoryusageTotalPagesDesc, prometheus.GaugeValue, float64(totalPages),
			strconv.FormatUint(nodeID, 10), memoryType)
	}
	return nil
}
