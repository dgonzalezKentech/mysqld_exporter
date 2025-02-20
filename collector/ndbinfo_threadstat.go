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

// Scrape `ndbinfo.threadstat`

package collector

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoThreadstatQuery = `
	SELECT node_id, thr_no, thr_nm, c_loop, c_exec, c_wait, os_tid, os_now,
		   os_ru_utime, os_ru_stime, os_ru_minflt, os_ru_majflt, os_ru_nvcsw, os_ru_nivcsw
	FROM ndbinfo.threadstat;
	`

var (
	ndbinfoThreadstatLoopsDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "threadstat_loop"),
		"Number of lops in the main loop for each thread on each node - ms",
		[]string{"nodeID", "threadNO", "threadName"}, nil,
	)

	ndbinfoThreadstatExecDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "threadstat_exec"),
		"Number of signals executed for each thread on each node - ms",
		[]string{"nodeID", "threadNO", "threadName"}, nil,
	)

	ndbinfoThreadstatWaitDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "threadstat_wait"),
		"Number of times waiting for additional input for each thread on each node - ms",
		[]string{"nodeID", "threadNO", "threadName"}, nil,
	)

	ndbinfoThreadstatOSTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "threadstat_os_time"),
		"OS time for each thread on each node - ms",
		[]string{"nodeID", "threadNO", "threadName"}, nil,
	)

	ndbinfoThreadstatOSUserTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "threadstat_user_time"),
		"OS user time for each thread on each node - µs",
		[]string{"nodeID", "threadNO", "threadName"}, nil,
	)

	ndbinfoThreadstatOSSystemTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "threadstat_system_time"),
		"OS system time for each thread on each node - µs",
		[]string{"nodeID", "threadNO", "threadName"}, nil,
	)

	ndbinfoThreadstatSoftPageFaultsDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "threadstat_soft_pagfault"),
		"Soft page faults for each thread on each node",
		[]string{"nodeID", "threadNO", "threadName"}, nil,
	)

	ndbinfoThreadstatHardPageFaultsDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "threadstat_hard_pagfault"),
		"Hard page faults for each thread on each node",
		[]string{"nodeID", "threadNO", "threadName"}, nil,
	)

	ndbinfoThreadstatVoluntaryCtxSwitchDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "threadstat_ctx_switch_voluntary"),
		"Voluntary context switches for each thread on each node",
		[]string{"nodeID", "threadNO", "threadName"}, nil,
	)

	ndbinfoThreadstatInvoluntaryCtxSwitchDesc = prometheus.NewDesc(
		prometheus.BuildFQName("ndb", ndbinfo, "threadstat_ctx_switch_involuntary"),
		"Involuntary context switches for each thread on each node",
		[]string{"nodeID", "threadNO", "threadName"}, nil,
	)
)

// ScrapeNdbinfoThreadstat collects for `ndbinfo.threadstat`
type ScrapeNdbinfoThreadstat struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNdbinfoThreadstat) Name() string {
	return "ndbinfo.threadstat"
}

// Help describes the role of the Scraper
func (ScrapeNdbinfoThreadstat) Help() string {
	return "Collect metrics from ndbinfo.threadstat"
}

// Version of MySQL from which scraper is available
func (ScrapeNdbinfoThreadstat) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric
func (ScrapeNdbinfoThreadstat) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	ndbinfoThreadstatRows, err := db.QueryContext(ctx, ndbinfoThreadstatQuery)
	if err != nil {
		logger.Error("Error querying ndbinfo.threadstat", "err", err)
		return err
	}
	defer ndbinfoThreadstatRows.Close()

	var (
		nodeID, threadNO, OSthreadNO, OSTime, OSUserTime, OSSystemTime               uint64
		softPageFaults, hardPageFaults, voluntaryCtxSwitch, involuntaryContextSwitch uint64
		threadName, loopCounter, signalCounter, waitingCounter                       string
	)

	//	SELECT node_id, thr_no, thr_nm, c_loop, c_exec, c_wait, os_tid, os_now,
	//	   os_ru_utime, os_ru_stime, os_ru_minflt, os_ru_majflt, os_ru_nvcsw, os_ru_nivcsw
	// Iterate over the memory settings
	for ndbinfoThreadstatRows.Next() {
		if err := ndbinfoThreadstatRows.Scan(
			&nodeID, &threadNO, &threadName, &loopCounter, &signalCounter, &waitingCounter,
			&OSthreadNO, &OSTime, &OSUserTime, &OSSystemTime, &softPageFaults, &hardPageFaults,
			&voluntaryCtxSwitch, &involuntaryContextSwitch); err != nil {
			return err
		}

		loopCounterFloat, err := strconv.ParseFloat(loopCounter, 64)
		if err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatLoopsDesc, prometheus.CounterValue, loopCounterFloat,
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(threadNO, 10), threadName,
		)

		signalCounterFloat, err := strconv.ParseFloat(signalCounter, 64)
		if err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatExecDesc, prometheus.CounterValue, signalCounterFloat,
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(threadNO, 10), threadName,
		)

		waitingCounterFloat, err := strconv.ParseFloat(waitingCounter, 64)
		if err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatWaitDesc, prometheus.CounterValue, waitingCounterFloat,
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(threadNO, 10), threadName,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatOSTimeDesc, prometheus.CounterValue, float64(OSTime),
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(threadNO, 10), threadName,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatOSUserTimeDesc, prometheus.CounterValue, float64(OSUserTime),
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(threadNO, 10), threadName,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatOSSystemTimeDesc, prometheus.CounterValue, float64(OSSystemTime),
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(threadNO, 10), threadName,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatSoftPageFaultsDesc, prometheus.CounterValue, float64(softPageFaults),
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(threadNO, 10), threadName,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatHardPageFaultsDesc, prometheus.CounterValue, float64(hardPageFaults),
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(threadNO, 10), threadName,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatVoluntaryCtxSwitchDesc, prometheus.CounterValue, float64(voluntaryCtxSwitch),
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(threadNO, 10), threadName,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatInvoluntaryCtxSwitchDesc, prometheus.CounterValue, float64(involuntaryContextSwitch),
			strconv.FormatUint(nodeID, 10), strconv.FormatUint(threadNO, 10), threadName,
		)
	}
	return nil
}
