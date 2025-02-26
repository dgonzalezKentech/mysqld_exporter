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

// Scrape `ndbinfo.cluster_operations`

package collector

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoClusterOperationsQuery = `
	SELECT node_id, operation_type, IFNULL(state,'') AS state, count(*)
	FROM ndbinfo.cluster_operations
	GROUP BY node_id, operation_type, state
	`

var (
	ndbinfoClusterOperationsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "cluster_operations"),
		"Number of operations for each node, operation type and state",
		[]string{"nodeID", "operationType", "state"}, nil,
	)
)

// ScrapeNdbinfoClusterOperations collects for `ndbinfo.cluster_operations`
type ScrapeNdbinfoClusterOperations struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNdbinfoClusterOperations) Name() string {
	return "ndbinfo.cluster_operations"
}

// Help describes the role of the Scraper
func (ScrapeNdbinfoClusterOperations) Help() string {
	return "Collect metrics from ndbinfo.cluster_operations"
}

// Version of MySQL from which scraper is available
func (ScrapeNdbinfoClusterOperations) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric
func (ScrapeNdbinfoClusterOperations) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	ndbinfoClusterOperationsRows, err := db.QueryContext(ctx, ndbinfoClusterOperationsQuery)
	if err != nil {
		logger.Error("Error querying ndbinfo.cluster_operations", "err", err)
		return err
	}
	defer ndbinfoClusterOperationsRows.Close()

	var (
		nodeID, count        uint64
		operationType, state string
	)

	// Iterate over the memory settings
	for ndbinfoClusterOperationsRows.Next() {
		if err := ndbinfoClusterOperationsRows.Scan(
			&nodeID, &operationType, &state, &count); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoClusterOperationsDesc, prometheus.GaugeValue, float64(count),
			strconv.FormatUint(nodeID, 10), operationType, state)
	}
	return nil
}
