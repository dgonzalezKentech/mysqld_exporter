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

// Scrape `ndbinfo.cluster_transactions`

package collector

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoClusterTransactionsQuery = `
	SELECT node_id, state, count(*) as cnt
	FROM ndbinfo.cluster_transactions
	GROUP BY node_id, state
	`

var (
	ndbinfoClusterTransactionsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "cluster_transactions"),
		"Number of transactions for each node and state",
		[]string{"nodeID", "state"}, nil,
	)
)

// ScrapeNdbinfoClusterTransactions collects for `ndbinfo.cluster_transactions`
type ScrapeNdbinfoClusterTransactions struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNdbinfoClusterTransactions) Name() string {
	return "ndbinfo.cluster_transactions"
}

// Help describes the role of the Scraper
func (ScrapeNdbinfoClusterTransactions) Help() string {
	return "Collect metrics from ndbinfo.cluster_transactions"
}

// Version of MySQL from which scraper is available
func (ScrapeNdbinfoClusterTransactions) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric
func (ScrapeNdbinfoClusterTransactions) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	ndbinfoClusterTransactionsRows, err := db.QueryContext(ctx, ndbinfoClusterTransactionsQuery)
	if err != nil {
		logger.Error("Error querying ndbinfo.cluster_transactions", "err", err)
		return err
	}
	defer ndbinfoClusterTransactionsRows.Close()

	var (
		nodeID, count uint64
		state         string
	)

	// Iterate over the memory settings
	for ndbinfoClusterTransactionsRows.Next() {
		if err := ndbinfoClusterTransactionsRows.Scan(
			&nodeID, &state, &count); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoClusterTransactionsDesc, prometheus.GaugeValue, float64(count),
			strconv.FormatUint(nodeID, 10), state)
	}
	return nil
}
