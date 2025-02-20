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

// Scrape `ndbinfo.processes`

package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoProcessesQuery = `
	SELECT node_type, process_name, count(*) 
	FROM ndbinfo.processes
	GROUP by node_type, process_name;
	`

var (
	ndbinfoProcessesCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "processes"),
		"Number of processes for each node type and process name",
		[]string{"nodeType", "processName"}, nil,
	)
)

// ScrapeNdbinfoProcesses collects for `ndbinfo.processes`
type ScrapeNdbinfoProcesses struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNdbinfoProcesses) Name() string {
	return "ndbinfo.processes"
}

// Help describes the role of the Scraper
func (ScrapeNdbinfoProcesses) Help() string {
	return "Collect metrics from ndbinfo.processes"
}

// Version of MySQL from which scraper is available
func (ScrapeNdbinfoProcesses) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric
func (ScrapeNdbinfoProcesses) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	ndbinfoProcessesRows, err := db.QueryContext(ctx, ndbinfoProcessesQuery)
	if err != nil {
		logger.Error("Error querying ndbinfo.processes", "err", err)
		return err
	}
	defer ndbinfoProcessesRows.Close()

	var (
		nodeType, processName string
		count                 uint64
	)

	// Iterate over the list
	for ndbinfoProcessesRows.Next() {
		if err := ndbinfoProcessesRows.Scan(
			&nodeType, &processName, &count); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoProcessesCountDesc, prometheus.GaugeValue, float64(count),
			nodeType, processName)
	}
	return nil
}
