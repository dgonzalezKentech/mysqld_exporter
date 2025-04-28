package collector

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoOperationsPerFragmentQuery = `
	SELECT  
		a.node_id,
		SUM(tot_key_reads           ) AS tot_key_reads           ,
		SUM(tot_key_inserts         ) AS tot_key_inserts         ,
		SUM(tot_key_updates         ) AS tot_key_updates         ,
		SUM(tot_key_writes          ) AS tot_key_writes          ,
		SUM(tot_key_deletes         ) AS tot_key_deletes         ,
		SUM(tot_key_refs            ) AS tot_key_refs            ,
		SUM(tot_key_attrinfo_bytes  ) AS tot_key_attrinfo_bytes  ,
		SUM(tot_key_keyinfo_bytes   ) AS tot_key_keyinfo_bytes   ,
		SUM(tot_key_prog_bytes      ) AS tot_key_prog_bytes      ,
		SUM(tot_key_inst_exec       ) AS tot_key_inst_exec       ,
		SUM(tot_key_bytes_returned  ) AS tot_key_bytes_returned  ,
		SUM(tot_frag_scans          ) AS tot_frag_scans          ,
		SUM(tot_scan_rows_examined  ) AS tot_scan_rows_examined  ,
		SUM(tot_scan_rows_returned  ) AS tot_scan_rows_returned  ,
		SUM(tot_scan_bytes_returned ) AS tot_scan_bytes_returned ,
		SUM(tot_scan_prog_bytes     ) AS tot_scan_prog_bytes     ,
		SUM(tot_scan_bound_bytes    ) AS tot_scan_bound_bytes    ,
		SUM(tot_scan_inst_exec      ) AS tot_scan_inst_exec      ,
		SUM(tot_qd_frag_scans       ) AS tot_qd_frag_scans       ,
		SUM(conc_frag_scans         ) AS conc_frag_scans         ,
		SUM(conc_qd_plain_frag_scans) AS conc_qd_plain_frag_scans,
		SUM(conc_qd_tup_frag_scans  ) AS conc_qd_tup_frag_scans  ,
		SUM(conc_qd_acc_frag_scans  ) AS conc_qd_acc_frag_scans  ,
		SUM(tot_commits             ) AS tot_commits             
	FROM ndbinfo.ndb$frag_operations a 
	GROUP BY node_id;
`

var (
	ndbinfoTotalKeyReadsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_key_reads"),
		"Total number of key read operations",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalKeyWritesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_key_writes"),
		"Total number of key write operations",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalKeyUpdatesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_key_updates"),
		"Total number of key update operations",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalKeyDeletesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_key_deletes"),
		"Total number of key delete operations",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalKeyRefsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_key_refs"),
		"Total number of key references",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalKeyAttrinfoBytesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_key_attrinfo_bytes"),
		"Total number of key attribute info bytes",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalKeyKeyinfoBytesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_key_keyinfo_bytes"),
		"Total number of key key info bytes",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalKeyProgBytesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_key_prog_bytes"),
		"Total number of key prog bytes",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalKeyInstExecDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_key_inst_exec"),
		"Total number of key instruction exec",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalKeyBytesReturnedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_key_bytes_returned"),
		"Total number of key bytes returned",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalFragScansDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_frag_scans"),
		"Total number of fragment scans",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalScanRowsExaminedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_scan_rows_examined"),
		"Total number of scan rows examined",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalScanRowsReturnedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_scan_rows_returned"),
		"Total number of scan rows returned",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalScanBytesReturnedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_scan_bytes_returned"),
		"Total number of scan bytes returned",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalScanProgBytesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_scan_prog_bytes"),
		"Total number of scan prog bytes",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalScanBoundBytesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_scan_bound_bytes"),
		"Total number of scan bound bytes",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalScanInstExecDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_scan_inst_exec"),
		"Total number of scan instruction exec",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalQdFragScansDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_qd_frag_scans"),
		"Total number of QD fragment scans",
		[]string{"node_id"}, nil,
	)
	ndbinfoConcFragScansDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_conc_frag_scans"),
		"Total number of concurrent fragment scans",
		[]string{"node_id"}, nil,
	)
	ndbinfoConcQdPlainFragScansDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_conc_qd_plain_frag_scans"),
		"Total number of concurrent QD plain fragment scans",
		[]string{"node_id"}, nil,
	)
	ndbinfoConcQdTupFragScansDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_conc_qd_tup_frag_scans"),
		"Total number of concurrent QD tuple fragment scans",
		[]string{"node_id"}, nil,
	)
	ndbinfoConcQdAccFragScansDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_conc_qd_acc_frag_scans"),
		"Total number of concurrent QD acc fragment scans",
		[]string{"node_id"}, nil,
	)
	ndbinfoTotalCommitsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbinfo, "operations_total_commits"),
		"Total number of commits",
		[]string{"node_id"}, nil,
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
	logger.Debug("Starting Scrape for ndbinfo.operations_per_fragment")
	logger.Debug("Establishing database connection")
	db := instance.getDB()
	if db == nil {
		logger.Error("Database connection is nil")
		return fmt.Errorf("database connection is nil")
	}

	logger.Debug("Executing query", "query", ndbinfoOperationsPerFragmentQuery)
	rows, err := db.QueryContext(ctx, ndbinfoOperationsPerFragmentQuery)
	if err != nil {
		logger.Error("Error querying ndbinfo.operations_per_fragment", "err", err)
		return err
	}
	defer rows.Close()

	logger.Debug("Query executed successfully, scanning rows")
	var (
		nodeID                                                      string
		totKeyReads, totKeyInserts, totKeyUpdates, totKeyWrites     float64
		totKeyDeletes, totKeyRefs, totKeyAttrinfoBytes              float64
		totKeyKeyinfoBytes, totKeyProgBytes, totKeyInstExec         float64
		totKeyBytesReturned, totFragScans, totScanRowsExamined      float64
		totScanRowsReturned, totScanBytesReturned, totScanProgBytes float64
		totScanBoundBytes, totScanInstExec, totQdFragScans          float64
		concFragScans, concQdPlainFragScans, concQdTupFragScans     float64
		concQdAccFragScans, totCommits                              float64
	)

	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &totKeyReads, &totKeyInserts, &totKeyUpdates, &totKeyWrites,
			&totKeyDeletes, &totKeyRefs, &totKeyAttrinfoBytes, &totKeyKeyinfoBytes,
			&totKeyProgBytes, &totKeyInstExec, &totKeyBytesReturned, &totFragScans,
			&totScanRowsExamined, &totScanRowsReturned, &totScanBytesReturned,
			&totScanProgBytes, &totScanBoundBytes, &totScanInstExec, &totQdFragScans,
			&concFragScans, &concQdPlainFragScans, &concQdTupFragScans,
			&concQdAccFragScans, &totCommits,
		); err != nil {
			logger.Error("Error scanning row", "err", err)
			return err
		}

		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalKeyReadsDesc, prometheus.GaugeValue, totKeyReads, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalKeyRefsDesc, prometheus.GaugeValue, totKeyRefs, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalKeyAttrinfoBytesDesc, prometheus.GaugeValue, totKeyAttrinfoBytes, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalKeyKeyinfoBytesDesc, prometheus.GaugeValue, totKeyKeyinfoBytes, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalKeyProgBytesDesc, prometheus.GaugeValue, totKeyProgBytes, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalKeyInstExecDesc, prometheus.GaugeValue, totKeyInstExec, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalKeyBytesReturnedDesc, prometheus.GaugeValue, totKeyBytesReturned, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalFragScansDesc, prometheus.GaugeValue, totFragScans, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalScanRowsExaminedDesc, prometheus.GaugeValue, totScanRowsExamined, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalScanRowsReturnedDesc, prometheus.GaugeValue, totScanRowsReturned, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalScanBytesReturnedDesc, prometheus.GaugeValue, totScanBytesReturned, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalScanProgBytesDesc, prometheus.GaugeValue, totScanProgBytes, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalScanBoundBytesDesc, prometheus.GaugeValue, totScanBoundBytes, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalScanInstExecDesc, prometheus.GaugeValue, totScanInstExec, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalQdFragScansDesc, prometheus.GaugeValue, totQdFragScans, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoConcFragScansDesc, prometheus.GaugeValue, concFragScans, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoConcQdPlainFragScansDesc, prometheus.GaugeValue, concQdPlainFragScans, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoConcQdTupFragScansDesc, prometheus.GaugeValue, concQdTupFragScans, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoConcQdAccFragScansDesc, prometheus.GaugeValue, concQdAccFragScans, nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTotalCommitsDesc, prometheus.GaugeValue, totCommits, nodeID,
		)
	}

	if err := rows.Err(); err != nil {
		logger.Error("Error iterating rows", "err", err)
		return err
	}

	logger.Debug("Scrape for ndbinfo.operations_per_fragment completed successfully")
	return nil
}
