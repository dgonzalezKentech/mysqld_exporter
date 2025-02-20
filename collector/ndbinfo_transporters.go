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

// Scrape `ndbinfo.transporters`

package collector

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoTransportersQuery = `
	SELECT  node_id, remote_node_id, bytes_sent, bytes_received,
	connect_count, overloaded, overload_count, slowdown, slowdown_count
	FROM ndbinfo.transporters;
	`

var (
	ndbinfoTransportersBytesSentDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "transporters_bytes_sent"),
		"Number of bytes sent using this connection",
		[]string{"nodeID", "remoteNodeID"}, nil,
	)
	ndbinfoTransportersBytesReceivedDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "transporters_bytes_received"),
		"Number of bytes received using this connection",
		[]string{"nodeID", "remoteNodeID"}, nil,
	)
	ndbinfoTransportersConnectionCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "transporters_connection_count"),
		"Number of times connection established on this transporter",
		[]string{"nodeID", "remoteNodeID"}, nil,
	)
	ndbinfoTransportersOverloadedDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "transporters_overloaded"),
		"1 if this transporter is currently overloaded, otherwise 0",
		[]string{"nodeID", "remoteNodeID"}, nil,
	)
	ndbinfoTransportersOverloadedCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "transporters_overloaded_count"),
		"Number of times this transporter has entered overload state since connecting",
		[]string{"nodeID", "remoteNodeID"}, nil,
	)
	ndbinfoTransportersSlowdownDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "transporters_slowdown"),
		"1 if this transporter is in slowdown state, otherwise 0",
		[]string{"nodeID", "remoteNodeID"}, nil,
	)
	ndbinfoTransportersSlowdownCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "transporters_slowdown_count"),
		"Number of times this transporter has entered slowdown state since connecting",
		[]string{"nodeID", "remoteNodeID"}, nil,
	)
)

// ScrapeNdbinfoTransporters collects for `ndbinfo.transporters`
type ScrapeNdbinfoTransporters struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNdbinfoTransporters) Name() string {
	return "ndbinfo.transporters"
}

// Help describes the role of the Scraper
func (ScrapeNdbinfoTransporters) Help() string {
	return "Collect metrics from ndbinfo.transporters"
}

// Version of MySQL from which scraper is available
func (ScrapeNdbinfoTransporters) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric
func (ScrapeNdbinfoTransporters) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	ndbinfoTransportersRows, err := db.QueryContext(ctx, ndbinfoTransportersQuery)
	if err != nil {
		logger.Error("Error querying ndbinfo.transporters", "err", err)
		return err
	}
	defer ndbinfoTransportersRows.Close()

	var (
		nodeID, remoteNodeID, bytesSent, bytesReceived, connectionCount uint64
		overloaded, overloadedCount, slowdown, slowdownCount            uint64
	)

	// Iterate over transporters
	for ndbinfoTransportersRows.Next() {
		if err := ndbinfoTransportersRows.Scan(
			&nodeID, &remoteNodeID, &bytesSent, &bytesReceived,
			&connectionCount, &overloaded, &overloadedCount,
			&slowdown, &slowdownCount); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTransportersBytesSentDesc, prometheus.CounterValue, float64(bytesSent),
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(remoteNodeID, 10))
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTransportersBytesReceivedDesc, prometheus.CounterValue, float64(bytesReceived),
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(remoteNodeID, 10))
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTransportersConnectionCountDesc, prometheus.CounterValue, float64(connectionCount),
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(remoteNodeID, 10))
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTransportersOverloadedDesc, prometheus.GaugeValue, float64(overloaded),
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(remoteNodeID, 10))
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTransportersOverloadedCountDesc, prometheus.CounterValue, float64(overloadedCount),
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(remoteNodeID, 10))
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTransportersSlowdownDesc, prometheus.CounterValue, float64(slowdown),
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(remoteNodeID, 10))
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTransportersSlowdownCountDesc, prometheus.CounterValue, float64(slowdownCount),
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(remoteNodeID, 10))
	}
	return nil
}
