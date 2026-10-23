package postgres

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func timestampRecord(t *testing.T, values ...*time.Time) arrow.RecordBatch {
	t.Helper()
	tsType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: tsType, Nullable: true},
	}, nil))
	defer b.Release()
	col := b.Field(0).(*array.TimestampBuilder)
	for _, v := range values {
		if v == nil {
			col.AppendNull()
			continue
		}
		col.Append(arrow.Timestamp(v.UnixMicro()))
	}
	record := b.NewRecordBatch()
	t.Cleanup(record.Release)
	return record
}

func at(day int) *time.Time {
	v := time.Date(2026, 1, day, 12, 0, 0, 0, time.UTC)
	return &v
}

func TestBatchTimeRange(t *testing.T) {
	minTime, maxTime, ok := batchTimeRange(timestampRecord(t, at(5), nil, at(2), at(9)), "ts")
	require.True(t, ok)
	require.Equal(t, *at(2), minTime.UTC())
	require.Equal(t, *at(9), maxTime.UTC())

	_, _, ok = batchTimeRange(timestampRecord(t, nil), "ts")
	require.False(t, ok)
	_, _, ok = batchTimeRange(timestampRecord(t, at(1)), "missing")
	require.False(t, ok)

	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "day", Type: arrow.FixedWidthTypes.Date32}}, nil))
	defer b.Release()
	b.Field(0).(*array.Date32Builder).AppendValues([]arrow.Date32{
		arrow.Date32FromTime(time.Date(2026, 3, 4, 0, 0, 0, 0, time.UTC)),
		arrow.Date32FromTime(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)),
	}, nil)
	dates := b.NewRecordBatch()
	defer dates.Release()
	minTime, maxTime, ok = batchTimeRange(dates, "day")
	require.True(t, ok)
	require.Equal(t, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), minTime.UTC())
	require.Equal(t, time.Date(2026, 3, 4, 0, 0, 0, 0, time.UTC), maxTime.UTC())
}

func TestChunkCompressorWatermark(t *testing.T) {
	c := &chunkCompressor{timeColumn: "ts", inFlight: make(map[arrow.RecordBatch]time.Time), wake: make(chan struct{}, 1)}
	_, ok := c.watermark()
	require.False(t, ok)

	first := timestampRecord(t, at(1), at(2))
	second := timestampRecord(t, at(3), at(4))
	third := timestampRecord(t, at(5), at(6))
	c.dispatched(first)
	c.dispatched(second)
	c.dispatched(third)

	mark, _ := c.watermark()
	require.Equal(t, *at(1), mark.UTC())

	c.settled(second)
	mark, _ = c.watermark()
	require.Equal(t, *at(1), mark.UTC(), "an older uncommitted batch holds the watermark back")

	c.settled(first)
	mark, _ = c.watermark()
	require.Equal(t, *at(5), mark.UTC())

	c.settled(third)
	mark, _ = c.watermark()
	require.Equal(t, *at(6), mark.UTC(), "once everything committed the newest row is the watermark")
}

func TestWatermarkSQL(t *testing.T) {
	sql, ok := watermarkSQL("timestamp with time zone", true)
	require.True(t, ok)
	require.Equal(t, "least(($2::timestamp AT TIME ZONE 'UTC'), (now() - $3::interval)::timestamp with time zone)", sql)

	sql, ok = watermarkSQL("date", true)
	require.True(t, ok)
	require.Equal(t, "least($2::timestamp::date, (now() - $3::interval)::date)", sql)

	sql, ok = watermarkSQL("timestamp without time zone", false)
	require.True(t, ok)
	require.Equal(t, "$2::timestamp", sql)

	_, ok = watermarkSQL("bigint", true)
	require.False(t, ok)
}
