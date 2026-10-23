package timescaledb

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const (
	CompressOff    = "off"
	CompressPolicy = "policy"
	CompressNow    = "now"
)

var optionParams = []string{
	"time_column",
	"chunk_interval",
	"segment_by",
	"order_by",
	"compress_after",
	"compress",
	"migrate_data",
}

// Options configures how a TimescaleDB destination lays out and compresses its target hypertable.
type Options struct {
	TimeColumn    string
	ChunkInterval string
	SegmentBy     []string
	OrderBy       string
	CompressAfter string
	Compress      string
	MigrateData   bool
}

// CompressionEnabled reports whether the hypertable needs compression settings.
func (o *Options) CompressionEnabled() bool {
	return o.Compress != CompressOff || len(o.SegmentBy) > 0 || o.OrderBy != ""
}

func IsScheme(scheme string) bool {
	switch strings.ToLower(scheme) {
	case "timescaledb", "timescale":
		return true
	}
	return false
}

// IsURI reports whether rawURI uses a TimescaleDB scheme.
func IsURI(rawURI string) bool {
	scheme, _, ok := strings.Cut(rawURI, "://")
	return ok && IsScheme(scheme)
}

// ConnectionURI rewrites a TimescaleDB URI into a PostgreSQL URI without the
// TimescaleDB options, which pgx would otherwise send as server parameters.
func ConnectionURI(rawURI string) (string, error) {
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return "", fmt.Errorf("invalid timescaledb uri: %w", err)
	}
	parsed.Scheme = "postgres"
	query := parsed.Query()
	for _, key := range optionParams {
		query.Del(key)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

// ParseOptions reads and validates the TimescaleDB destination options from rawURI.
func ParseOptions(rawURI string) (*Options, error) {
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return nil, fmt.Errorf("invalid timescaledb uri: %w", err)
	}
	query := parsed.Query()

	opts := &Options{
		TimeColumn:    strings.TrimSpace(query.Get("time_column")),
		ChunkInterval: strings.TrimSpace(query.Get("chunk_interval")),
		OrderBy:       strings.TrimSpace(query.Get("order_by")),
		CompressAfter: strings.TrimSpace(query.Get("compress_after")),
		Compress:      strings.ToLower(strings.TrimSpace(query.Get("compress"))),
	}
	if opts.TimeColumn == "" {
		return nil, fmt.Errorf("timescaledb uri: time_column is required")
	}
	for _, column := range strings.Split(query.Get("segment_by"), ",") {
		if column = strings.TrimSpace(column); column != "" {
			opts.SegmentBy = append(opts.SegmentBy, column)
		}
	}
	if raw := query.Get("migrate_data"); raw != "" {
		opts.MigrateData, err = strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("timescaledb uri: invalid migrate_data value %q: %w", raw, err)
		}
	}

	if opts.Compress == "" {
		opts.Compress = CompressOff
		if opts.CompressAfter != "" {
			opts.Compress = CompressPolicy
		}
	}
	switch opts.Compress {
	case CompressOff:
		if opts.CompressAfter != "" {
			return nil, fmt.Errorf("timescaledb uri: compress_after has no effect with compress=off")
		}
	case CompressPolicy:
		if opts.CompressAfter == "" {
			return nil, fmt.Errorf("timescaledb uri: compress=policy requires compress_after")
		}
	case CompressNow:
	default:
		return nil, fmt.Errorf("timescaledb uri: invalid compress value %q (expected off, policy, or now)", opts.Compress)
	}
	return opts, nil
}
