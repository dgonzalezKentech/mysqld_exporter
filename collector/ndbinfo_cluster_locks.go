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

// Scrape `ndbinfo.cluster_locks`

package collector

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoClusterLocksQuery = `
	SELECT node_id, mode, state, op, count(*), avg(duration_millis) 
	FROM ndbinfo.cluster_locks 
	GROUP BY node_id, mode, state, op;
	`

var (
	ndbinfoClusterLocksCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "cluster_locks_count"),
		"Number of locks for each node, mode, state and operation type",
		[]string{"nodeID", "mode", "state", "operationType"}, nil,
	)
	ndbinfoClusterLocksAvgDurationDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "cluster_locks_avg_duration"),
		"Lock state average duraton for each node, mode, state and operation type",
		[]string{"nodeID", "mode", "state", "operationType"}, nil,
	)
)

// ScrapeNdbinfoClusterLocks collects for `ndbinfo.cluster_locks`
type ScrapeNdbinfoClusterLocks struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNdbinfoClusterLocks) Name() string {
	return "ndbinfo.cluster_locks"
}

// Help describes the role of the Scraper
func (ScrapeNdbinfoClusterLocks) Help() string {
	return "Collect metrics from ndbinfo.cluster_locks"
}

// Version of MySQL from which scraper is available
func (ScrapeNdbinfoClusterLocks) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric
func (ScrapeNdbinfoClusterLocks) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	ndbinfoClusterLocksRows, err := db.QueryContext(ctx, ndbinfoClusterLocksQuery)
	if err != nil {
		logger.Error("Error querying ndbinfo.cluster_locks", "err", err)
		return err
	}
	defer ndbinfoClusterLocksRows.Close()

	//SELECT node_id, mode, state, op, count(*), avg(duration_millis)
	//FROM cluster_locks
	//GROUP BY node_id, mode, state, op;
	var (
		nodeID, count          uint64
		average                float64
		mode, state, operation string
	)

	// Iterate over the memory settings
	for ndbinfoClusterLocksRows.Next() {
		if err := ndbinfoClusterLocksRows.Scan(
			&nodeID, &mode, &state, &operation, &count, &average); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoClusterLocksCountDesc, prometheus.GaugeValue, float64(count),
			strconv.FormatUint(nodeID, 10), mode, state, operation)

		ch <- prometheus.MustNewConstMetric(
			ndbinfoClusterLocksAvgDurationDesc, prometheus.GaugeValue, average,
			strconv.FormatUint(nodeID, 10), mode, state, operation)
	}
	return nil
}
