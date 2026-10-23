package server

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/bruin-data/ingestr/internal/timescaledb"
)

type RunJobRequest struct {
	SourceCredentialID string `json:"sourceCredentialId"`
	DestCredentialID   string `json:"destCredentialId"`
	SourceURI          string `json:"sourceUri"`
	DestURI            string `json:"destUri"`
	SourceTable        string `json:"sourceTable"`
	DestTable          string `json:"destTable"`

	IncrementalStrategy  string   `json:"incrementalStrategy"`
	Strategy             string   `json:"strategy"`
	IncrementalKey       string   `json:"incrementalKey"`
	IncrementalPredicate string   `json:"incrementalPredicate"`
	IntervalStart        string   `json:"intervalStart"`
	IntervalEnd          string   `json:"intervalEnd"`
	PrimaryKeys          []string `json:"primaryKeys"`

	PartitionBy string `json:"partitionBy"`
	ClusterBy   string `json:"clusterBy"`

	FullRefresh      bool   `json:"fullRefresh"`
	SchemaContract   string `json:"schemaContract"`
	SchemaNaming     string `json:"schemaNaming"`
	CDCTableNaming   string `json:"cdcTableNaming"`
	Progress         string `json:"progress"`
	PageSize         int    `json:"pageSize"`
	LoaderFileSize   int    `json:"loaderFileSize"`
	LoaderFileFormat string `json:"loaderFileFormat"`

	ExtractParallelism       int      `json:"extractParallelism"`
	ExtractPartitionBy       string   `json:"extractPartitionBy"`
	ExtractPartitionInterval string   `json:"extractPartitionInterval"`
	SQLLimit                 int      `json:"sqlLimit"`
	SQLExcludeColumns        []string `json:"sqlExcludeColumns"`
	SQLBackend               []string `json:"sqlBackend"`
	Columns                  string   `json:"columns"`
	NoInference              bool     `json:"noInference"`
	Mask                     []string `json:"mask"`
	TrimWhitespace           bool     `json:"trimWhitespace"`
	NoLoadTimestamp          bool     `json:"noLoadTimestamp"`
	NoRunID                  bool     `json:"noRunId"`

	PipelinesDir     string `json:"pipelinesDir"`
	StagingBucket    string `json:"stagingBucket"`
	StagingDataset   string `json:"stagingDataset"`
	Debug            bool   `json:"debug"`
	Stream           bool   `json:"stream"`
	FlushInterval    string `json:"flushInterval"`
	FlushRecords     int    `json:"flushRecords"`
	QueryAnnotations string `json:"queryAnnotations"`

	Timescale *TimescaleRunOptions `json:"timescale,omitempty"`
}

// TimescaleRunOptions are TimescaleDB destination settings chosen per run;
// they are passed to ingestr as query parameters of the destination URI.
type TimescaleRunOptions struct {
	TimeColumn    string `json:"timeColumn"`
	ChunkInterval string `json:"chunkInterval"`
	SegmentBy     string `json:"segmentBy"`
	OrderBy       string `json:"orderBy"`
	CompressAfter string `json:"compressAfter"`
	Compress      string `json:"compress"`
	MigrateData   bool   `json:"migrateData"`
}

func (o *TimescaleRunOptions) params() map[string]string {
	params := map[string]string{
		"time_column":    o.TimeColumn,
		"chunk_interval": o.ChunkInterval,
		"segment_by":     o.SegmentBy,
		"order_by":       o.OrderBy,
		"compress_after": o.CompressAfter,
		"compress":       o.Compress,
	}
	if o.MigrateData {
		params["migrate_data"] = "true"
	}
	for key, value := range params {
		if params[key] = strings.TrimSpace(value); params[key] == "" {
			delete(params, key)
		}
	}
	return params
}

// applyTimescaleOptions sets the run's TimescaleDB options on destURI,
// overriding any of the same parameters already in it.
func applyTimescaleOptions(destURI string, opts *TimescaleRunOptions) (string, error) {
	if opts == nil {
		return destURI, nil
	}
	params := opts.params()
	if len(params) == 0 {
		return destURI, nil
	}
	if !timescaledb.IsURI(destURI) {
		return "", fmt.Errorf("timescaledb options require a timescaledb destination")
	}
	parsed, err := url.Parse(destURI)
	if err != nil {
		return "", fmt.Errorf("invalid destination uri: %w", err)
	}
	query := parsed.Query()
	for key, value := range params {
		query.Set(key, value)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func (r RunJobRequest) IngestArgs() []string {
	args := []string{}
	appendString := func(name, value string) {
		if value != "" {
			args = append(args, "--"+name+"="+value)
		}
	}
	appendInt := func(name string, value int) {
		if value > 0 {
			args = append(args, "--"+name+"="+strconv.Itoa(value))
		}
	}
	appendBool := func(name string, value bool) {
		if value {
			args = append(args, "--"+name)
		}
	}
	appendStringSlice := func(name string, values []string) {
		for _, value := range values {
			if value != "" {
				args = append(args, "--"+name+"="+value)
			}
		}
	}

	appendString("source-uri", r.SourceURI)
	appendString("dest-uri", r.DestURI)
	appendString("source-table", r.SourceTable)
	appendString("dest-table", r.DestTable)
	appendString("incremental-key", r.IncrementalKey)
	appendString("incremental-predicate", r.IncrementalPredicate)
	appendString("incremental-strategy", r.IncrementalStrategy)
	appendString("interval-start", r.IntervalStart)
	appendString("interval-end", r.IntervalEnd)
	appendStringSlice("primary-key", r.PrimaryKeys)
	appendString("partition-by", r.PartitionBy)
	appendString("cluster-by", r.ClusterBy)
	appendBool("full-refresh", r.FullRefresh)
	appendString("schema-contract", r.SchemaContract)
	appendString("schema-naming", r.SchemaNaming)
	appendString("cdc-table-naming", r.CDCTableNaming)
	appendString("progress", r.Progress)
	appendInt("page-size", r.PageSize)
	appendInt("loader-file-size", r.LoaderFileSize)
	appendString("loader-file-format", r.LoaderFileFormat)
	appendInt("extract-parallelism", r.ExtractParallelism)
	appendString("extract-partition-by", r.ExtractPartitionBy)
	appendString("extract-partition-interval", r.ExtractPartitionInterval)
	appendInt("sql-limit", r.SQLLimit)
	appendStringSlice("sql-exclude-columns", r.SQLExcludeColumns)
	appendStringSlice("sql-backend", r.SQLBackend)
	appendString("columns", r.Columns)
	appendBool("no-inference", r.NoInference)
	appendStringSlice("mask", r.Mask)
	appendBool("trim-whitespace", r.TrimWhitespace)
	appendBool("no-load-timestamp", r.NoLoadTimestamp)
	appendBool("no-run-id", r.NoRunID)
	appendString("pipelines-dir", r.PipelinesDir)
	appendString("staging-bucket", r.StagingBucket)
	appendString("staging-dataset", r.StagingDataset)
	appendBool("debug", r.Debug)
	appendBool("stream", r.Stream)
	appendString("flush-interval", r.FlushInterval)
	appendInt("flush-records", r.FlushRecords)
	appendString("query-annotations", r.QueryAnnotations)

	args = append(args, "--yes")

	return args
}
