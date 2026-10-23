package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bruin-data/ingestr/internal/config"
	"github.com/bruin-data/ingestr/internal/output"
	"github.com/bruin-data/ingestr/internal/timescaledb"
	"github.com/bruin-data/ingestr/pkg/destination"
	"github.com/bruin-data/ingestr/pkg/schema"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const managedStagingSchema = "_bruin_staging"

// timescaleMinVersion is the first release that allows INSERT ... ON CONFLICT,
// UPDATE and DELETE on compressed chunks, which merge and delete+insert rely on.
var timescaleMinVersion = [2]int{2, 11}

// timescaleByRangeVersion introduced create_hypertable(..., by_range(...)).
var timescaleByRangeVersion = [2]int{2, 13}

// timescaleRecompressVersion is the first release whose compress_chunk also
// recompresses partially compressed chunks; older ones need recompress_chunk.
var timescaleRecompressVersion = [2]int{2, 14}

type timescaleState struct {
	opts    *timescaledb.Options
	version [2]int
}

func (d *PostgresDestination) checkTimescaleExtension(ctx context.Context) error {
	var extVersion string
	err := d.pool.QueryRow(ctx, `SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'`).Scan(&extVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("the timescaledb extension is not installed in this database; run CREATE EXTENSION timescaledb")
	}
	if err != nil {
		return fmt.Errorf("failed to read timescaledb extension version: %w", err)
	}
	version, err := parseTimescaleVersion(extVersion)
	if err != nil {
		return err
	}
	if compareTimescaleVersion(version, timescaleMinVersion) < 0 {
		return fmt.Errorf("timescaledb %s is not supported; version %d.%d or newer is required", extVersion, timescaleMinVersion[0], timescaleMinVersion[1])
	}
	d.timescale.version = version

	if d.timescale.opts.CompressionEnabled() {
		var license string
		if err := d.pool.QueryRow(ctx, `SELECT coalesce(current_setting('timescaledb.license', true), '')`).Scan(&license); err != nil {
			return fmt.Errorf("failed to read timescaledb license: %w", err)
		}
		if strings.EqualFold(license, "apache") {
			return fmt.Errorf("timescaledb compression is not available in the Apache-licensed build of the extension; remove the compression options or use the Community edition")
		}
	}
	config.Debug("[DEST] TimescaleDB %s detected", extVersion)
	return nil
}

func parseTimescaleVersion(raw string) ([2]int, error) {
	parts := strings.SplitN(raw, ".", 3)
	if len(parts) < 2 {
		return [2]int{}, fmt.Errorf("unrecognized timescaledb version %q", raw)
	}
	var version [2]int
	for i := range version {
		digits := strings.TrimRightFunc(parts[i], func(r rune) bool { return r < '0' || r > '9' })
		n, err := strconv.Atoi(digits)
		if err != nil {
			return [2]int{}, fmt.Errorf("unrecognized timescaledb version %q", raw)
		}
		version[i] = n
	}
	return version, nil
}

func compareTimescaleVersion(a, b [2]int) int {
	if a[0] != b[0] {
		return a[0] - b[0]
	}
	return a[1] - b[1]
}

// isTimescaleHypertable reports whether PrepareTable should turn the table into
// a hypertable. Run-scoped staging and the managed CDC bookkeeping tables stay
// plain; staging that is swapped into the target (TargetTable set) does not.
func (d *PostgresDestination) isTimescaleHypertable(opts destination.PrepareOptions, schemaName string) bool {
	if d.timescale == nil {
		return false
	}
	if opts.TargetTable != "" {
		return true
	}
	return opts.ExpiresAfter == 0 && schemaName != managedStagingSchema
}

func validateTimescaleTable(opts *timescaledb.Options, prepare destination.PrepareOptions) error {
	idx := slices.IndexFunc(prepare.Schema.Columns, func(c schema.Column) bool { return c.Name == opts.TimeColumn })
	if idx < 0 {
		return fmt.Errorf("timescaledb time_column %q is not a column of table %s", opts.TimeColumn, prepare.Table)
	}
	switch prepare.Schema.Columns[idx].DataType {
	case schema.TypeTimestamp, schema.TypeTimestampTZ, schema.TypeDate:
	default:
		return fmt.Errorf("timescaledb time_column %q must be a timestamp, timestamptz, or date column, got %s", opts.TimeColumn, prepare.Schema.Columns[idx].DataType)
	}
	for _, keys := range [][]string{prepare.PrimaryKeys, prepare.DeferredPrimaryKeys} {
		if len(keys) > 0 && !slices.Contains(keys, opts.TimeColumn) {
			return fmt.Errorf("timescaledb requires the primary key to include time_column %q, got (%s); add it to --primary-key", opts.TimeColumn, strings.Join(keys, ", "))
		}
	}
	return nil
}

func (d *PostgresDestination) ensureHypertable(ctx context.Context, schemaName, tableName string) error {
	opts := d.timescale.opts
	table := quotePostgresTable(schemaName, tableName)

	timeColumn, isHypertable, err := d.hypertableTimeColumn(ctx, schemaName, tableName)
	if err != nil {
		return err
	}
	if isHypertable {
		if timeColumn != opts.TimeColumn {
			return fmt.Errorf("table %s is already a hypertable partitioned by %q, not time_column %q", table, timeColumn, opts.TimeColumn)
		}
	} else {
		if !opts.MigrateData {
			var hasRows bool
			if err := d.pool.QueryRow(ctx, fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s)", table)).Scan(&hasRows); err != nil {
				return fmt.Errorf("failed to check whether %s is empty: %w", table, err)
			}
			if hasRows {
				return fmt.Errorf("table %s already holds rows and is not a hypertable; add migrate_data=true to the destination URI to convert it", table)
			}
		}
		start := time.Now()
		sql, args := buildCreateHypertableSQL(table, opts, compareTimescaleVersion(d.timescale.version, timescaleByRangeVersion) >= 0)
		if _, err := d.pool.Exec(ctx, sql, args...); err != nil {
			config.LogFailedQuery(sql, err)
			return fmt.Errorf("failed to create hypertable %s: %w", table, err)
		}
		config.Debug("[DEST] create_hypertable took %v", time.Since(start))
	}

	if !opts.CompressionEnabled() {
		return nil
	}
	var compressionEnabled bool
	if err := d.pool.QueryRow(ctx,
		`SELECT compression_enabled FROM timescaledb_information.hypertables WHERE hypertable_schema = $1 AND hypertable_name = $2`,
		schemaName, tableName).Scan(&compressionEnabled); err != nil {
		return fmt.Errorf("failed to read compression settings of %s: %w", table, err)
	}
	if compressionEnabled {
		config.Debug("[DEST] Compression already enabled on %s; keeping its existing settings", table)
		return nil
	}
	sql := buildCompressionSettingsSQL(table, opts)
	if _, err := d.pool.Exec(ctx, sql); err != nil {
		config.LogFailedQuery(sql, err)
		return fmt.Errorf("failed to enable compression on %s: %w", table, err)
	}
	return nil
}

func (d *PostgresDestination) hypertableTimeColumn(ctx context.Context, schemaName, tableName string) (string, bool, error) {
	var column string
	err := d.pool.QueryRow(ctx,
		`SELECT column_name FROM timescaledb_information.dimensions WHERE hypertable_schema = $1 AND hypertable_name = $2 ORDER BY dimension_number LIMIT 1`,
		schemaName, tableName).Scan(&column)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("failed to look up hypertable %s.%s: %w", schemaName, tableName, err)
	}
	return column, true, nil
}

func buildCreateHypertableSQL(table string, opts *timescaledb.Options, byRange bool) (string, []any) {
	args := []any{table, opts.TimeColumn}
	var dimension string
	switch {
	case byRange && opts.ChunkInterval != "":
		dimension = "by_range($2::name, $3::interval)"
		args = append(args, opts.ChunkInterval)
	case byRange:
		dimension = "by_range($2::name)"
	case opts.ChunkInterval != "":
		dimension = "$2::name, chunk_time_interval => $3::interval"
		args = append(args, opts.ChunkInterval)
	default:
		dimension = "$2::name"
	}
	args = append(args, opts.MigrateData)
	return fmt.Sprintf("SELECT create_hypertable($1::regclass, %s, if_not_exists => true, migrate_data => $%d)", dimension, len(args)), args
}

func buildCompressionSettingsSQL(table string, opts *timescaledb.Options) string {
	settings := []string{"timescaledb.compress"}
	if len(opts.SegmentBy) > 0 {
		settings = append(settings, "timescaledb.compress_segmentby = "+quotePostgresLiteral(strings.Join(quoteColumns(opts.SegmentBy), ", ")))
	}
	if opts.OrderBy != "" {
		settings = append(settings, "timescaledb.compress_orderby = "+quotePostgresLiteral(opts.OrderBy))
	}
	return fmt.Sprintf("ALTER TABLE %s SET (%s)", table, strings.Join(settings, ", "))
}

func quotePostgresLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// FinalizeTable adds the compression policy when compress_after is set and,
// with compress=now, compresses every chunk older than compress_after (every
// chunk without it) that is still uncompressed or only partially compressed
// once the load has committed.
func (d *PostgresDestination) FinalizeTable(ctx context.Context, table string) error {
	if d.timescale == nil || d.timescale.opts.Compress == timescaledb.CompressOff {
		return nil
	}
	schemaName, tableName, err := d.resolveSchemaTable(ctx, d.pool, table)
	if err != nil {
		return err
	}
	if _, isHypertable, err := d.hypertableTimeColumn(ctx, schemaName, tableName); err != nil {
		return err
	} else if !isHypertable {
		return nil
	}
	target := quotePostgresTable(schemaName, tableName)
	opts := d.timescale.opts

	if opts.CompressAfter != "" {
		policySQL := `SELECT add_compression_policy($1::regclass, compress_after => $2::interval, if_not_exists => true)`
		if _, err := d.pool.Exec(ctx, policySQL, target, opts.CompressAfter); err != nil {
			config.LogFailedQuery(policySQL, err)
			return fmt.Errorf("failed to add compression policy on %s: %w", target, err)
		}
	}
	if opts.Compress != timescaledb.CompressNow {
		return nil
	}

	start := time.Now()
	chunksSQL, chunksArgs := `SELECT show_chunks($1::regclass)::text`, []any{target}
	if opts.CompressAfter != "" {
		chunksSQL = `SELECT show_chunks($1::regclass, older_than => $2::interval)::text`
		chunksArgs = append(chunksArgs, opts.CompressAfter)
	}
	rows, err := d.pool.Query(ctx, chunksSQL, chunksArgs...)
	if err != nil {
		return fmt.Errorf("failed to list chunks of %s: %w", target, err)
	}
	chunks, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return fmt.Errorf("failed to list chunks of %s: %w", target, err)
	}
	for _, chunk := range chunks {
		if err := d.compressChunk(ctx, chunk); err != nil {
			return fmt.Errorf("failed to compress chunk %s of %s: %w", chunk, target, err)
		}
		config.Debug("[DEST] Compressed chunk %s", chunk)
	}
	scope := ""
	if opts.CompressAfter != "" {
		scope = " older than " + opts.CompressAfter
	}
	output.Statusf("[TIMESCALEDB] %s | Compressed %d chunk(s) of %s%s in %v\n",
		time.Now().Format("15:04:05"), len(chunks), target, scope, time.Since(start).Round(time.Millisecond))
	return nil
}

// compressChunk compresses an uncompressed chunk or recompresses a partially
// compressed one. It tolerates the compression policy job compressing the same
// chunk concurrently, which if_not_compressed does not cover.
func (d *PostgresDestination) compressChunk(ctx context.Context, chunk string) error {
	_, err := d.pool.Exec(ctx, `SELECT compress_chunk($1::regclass, if_not_compressed => true)`, chunk)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42710" {
		err = nil
	}
	if err != nil || compareTimescaleVersion(d.timescale.version, timescaleRecompressVersion) >= 0 {
		return err
	}
	var partial bool
	if err := d.pool.QueryRow(ctx, `
		SELECT status & 8 = 8 FROM _timescaledb_catalog.chunk
		WHERE format('%I.%I', schema_name, table_name)::regclass = $1::regclass`, chunk).Scan(&partial); err != nil {
		return fmt.Errorf("failed to read compression status: %w", err)
	}
	if !partial {
		return nil
	}
	_, err = d.pool.Exec(ctx, `CALL recompress_chunk($1::regclass)`, chunk)
	return err
}
