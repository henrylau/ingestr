package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/bruin-data/ingestr/internal/config"
	"github.com/bruin-data/ingestr/internal/output"
	"github.com/bruin-data/ingestr/internal/timescaledb"
	"github.com/bruin-data/ingestr/pkg/destination"
	"github.com/bruin-data/ingestr/pkg/source"
	"github.com/jackc/pgx/v5"
)

// timescaleCommitInterval bounds how long a COPY stays open while chunks are
// compressed during the load, since only committed rows let the watermark move.
var timescaleCommitInterval = 10 * time.Second

// chunkCompressor compresses hypertable chunks while an append is still
// loading, as soon as no dispatched-but-uncommitted row can fall into them.
// It assumes rows arrive roughly in time order; a late row that lands in an
// already compressed chunk is still written correctly by TimescaleDB.
type chunkCompressor struct {
	d             *PostgresDestination
	target        string
	timeColumn    string
	olderThanSQL  string
	compressAfter string

	mu       sync.Mutex
	inFlight map[arrow.RecordBatch]time.Time
	maxSeen  time.Time
	seen     bool

	wake       chan struct{}
	stop       chan struct{}
	done       chan struct{}
	compressed map[string]struct{}
}

// startChunkCompressor returns nil unless the write appends straight into a
// compressed hypertable with compress=now.
func (d *PostgresDestination) startChunkCompressor(ctx context.Context, opts destination.WriteOptions) (*chunkCompressor, error) {
	if d.timescale == nil || d.timescale.opts.Compress != timescaledb.CompressNow || opts.StagingTable {
		return nil, nil
	}
	schemaName, tableName, err := d.resolveSchemaTable(ctx, d.pool, opts.Table)
	if err != nil {
		return nil, err
	}
	var columnType string
	err = d.pool.QueryRow(ctx, `
		SELECT d.column_type::text
		FROM timescaledb_information.dimensions d
		JOIN timescaledb_information.hypertables h USING (hypertable_schema, hypertable_name)
		WHERE d.hypertable_schema = $1 AND d.hypertable_name = $2 AND d.column_name = $3 AND h.compression_enabled`,
		schemaName, tableName, d.timescale.opts.TimeColumn).Scan(&columnType)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to look up hypertable %s.%s: %w", schemaName, tableName, err)
	}
	olderThanSQL, ok := watermarkSQL(columnType, d.timescale.opts.CompressAfter != "")
	if !ok {
		return nil, nil
	}

	c := &chunkCompressor{
		d:             d,
		target:        quotePostgresTable(schemaName, tableName),
		timeColumn:    d.timescale.opts.TimeColumn,
		olderThanSQL:  olderThanSQL,
		compressAfter: d.timescale.opts.CompressAfter,
		inFlight:      make(map[arrow.RecordBatch]time.Time),
		wake:          make(chan struct{}, 1),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
		compressed:    make(map[string]struct{}),
	}
	go c.run(ctx)
	return c, nil
}

// watermarkSQL casts the UTC wall-clock watermark ($2) to the time column's
// type and, when capped, caps it at now() - compress_after ($3).
func watermarkSQL(columnType string, capped bool) (string, bool) {
	var cast string
	switch columnType {
	case "timestamp with time zone":
		cast = "($2::timestamp AT TIME ZONE 'UTC')"
	case "timestamp without time zone":
		cast = "$2::timestamp"
	case "date":
		cast = "$2::timestamp::date"
	default:
		return "", false
	}
	if !capped {
		return cast, true
	}
	return fmt.Sprintf("least(%s, (now() - $3::interval)::%s)", cast, columnType), true
}

// wrap registers every batch as in flight before a worker can pick it up.
func (c *chunkCompressor) wrap(records <-chan source.RecordBatchResult) <-chan source.RecordBatchResult {
	if c == nil {
		return records
	}
	out := make(chan source.RecordBatchResult)
	go func() {
		defer close(out)
		for result := range records {
			if result.Err == nil && result.Batch != nil {
				c.dispatched(result.Batch)
			}
			select {
			case out <- result:
			case <-c.stop:
				if result.Batch != nil {
					result.Batch.Release()
				}
				return
			}
		}
	}()
	return out
}

func (c *chunkCompressor) dispatched(batch arrow.RecordBatch) {
	minTime, maxTime, ok := batchTimeRange(batch, c.timeColumn)
	if !ok {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inFlight[batch] = minTime
	if !c.seen || maxTime.After(c.maxSeen) {
		c.maxSeen = maxTime
		c.seen = true
	}
}

// settled is called once a COPY has committed or rolled back; either way its
// rows can no longer land in a chunk.
func (c *chunkCompressor) settled(batches ...arrow.RecordBatch) {
	if c == nil {
		return
	}
	c.mu.Lock()
	for _, batch := range batches {
		delete(c.inFlight, batch)
	}
	c.mu.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// watermark is the earliest time that may still be written: the oldest row
// of any uncommitted batch, or the newest row seen once everything committed.
func (c *chunkCompressor) watermark() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.seen {
		return time.Time{}, false
	}
	mark := c.maxSeen
	for _, minTime := range c.inFlight {
		if minTime.Before(mark) {
			mark = minTime
		}
	}
	return mark, true
}

func (c *chunkCompressor) run(ctx context.Context) {
	defer close(c.done)
	var last time.Time
	for {
		select {
		case <-c.stop:
			return
		case <-ctx.Done():
			return
		case <-c.wake:
		}
		mark, ok := c.watermark()
		if !ok || !mark.After(last) {
			continue
		}
		if err := c.compressBefore(ctx, mark); err != nil {
			if ctx.Err() == nil {
				output.Statusf("[TIMESCALEDB] Warning: compressing chunks of %s during the load failed, the final pass will retry: %v\n", c.target, err)
			}
			return
		}
		last = mark
	}
}

func (c *chunkCompressor) compressBefore(ctx context.Context, mark time.Time) error {
	args := []any{c.target, mark.UTC().Format("2006-01-02 15:04:05.999999")}
	if c.compressAfter != "" {
		args = append(args, c.compressAfter)
	}
	rows, err := c.d.pool.Query(ctx, fmt.Sprintf(`
		SELECT c::text FROM show_chunks($1::regclass, older_than => %s) c
		JOIN timescaledb_information.chunks i ON format('%%I.%%I', i.chunk_schema, i.chunk_name)::regclass = c
		WHERE NOT i.is_compressed`, c.olderThanSQL), args...)
	if err != nil {
		return fmt.Errorf("failed to list chunks: %w", err)
	}
	chunks, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return fmt.Errorf("failed to list chunks: %w", err)
	}
	for _, chunk := range chunks {
		if _, ok := c.compressed[chunk]; ok {
			continue
		}
		select {
		case <-c.stop:
			return nil
		default:
		}
		start := time.Now()
		if err := c.d.compressChunk(ctx, chunk); err != nil {
			return fmt.Errorf("failed to compress chunk %s: %w", chunk, err)
		}
		c.compressed[chunk] = struct{}{}
		config.Debug("[DEST] Compressed completed chunk %s of %s in %v", chunk, c.target, time.Since(start))
	}
	return nil
}

// close stops the compressor after the chunk it is compressing, if any.
func (c *chunkCompressor) close() {
	if c == nil {
		return
	}
	close(c.stop)
	<-c.done
	if len(c.compressed) > 0 {
		output.Statusf("[TIMESCALEDB] %s | Compressed %d completed chunk(s) of %s during the load\n",
			time.Now().Format("15:04:05"), len(c.compressed), c.target)
	}
}

func batchTimeRange(batch arrow.RecordBatch, column string) (time.Time, time.Time, bool) {
	indices := batch.Schema().FieldIndices(column)
	if len(indices) == 0 {
		return time.Time{}, time.Time{}, false
	}
	col := batch.Column(indices[0])
	var at func(int) time.Time
	switch arr := col.(type) {
	case *array.Timestamp:
		toTime, err := arr.DataType().(*arrow.TimestampType).GetToTimeFunc()
		if err != nil {
			return time.Time{}, time.Time{}, false
		}
		at = func(i int) time.Time { return toTime(arr.Value(i)) }
	case *array.Date32:
		at = func(i int) time.Time { return arr.Value(i).ToTime() }
	case *array.Date64:
		at = func(i int) time.Time { return arr.Value(i).ToTime() }
	default:
		return time.Time{}, time.Time{}, false
	}
	var minTime, maxTime time.Time
	found := false
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		t := at(i)
		if !found || t.Before(minTime) {
			minTime = t
		}
		if !found || t.After(maxTime) {
			maxTime = t
		}
		found = true
	}
	return minTime, maxTime, found
}
