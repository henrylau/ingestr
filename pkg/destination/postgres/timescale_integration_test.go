//go:build integration

package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/bruin-data/ingestr/pkg/destination"
	"github.com/bruin-data/ingestr/pkg/schema"
	"github.com/bruin-data/ingestr/pkg/source"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startTimescaleContainer(t *testing.T, ctx context.Context, image string) string {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: image,
			Env: map[string]string{
				"POSTGRES_USER": "testuser", "POSTGRES_PASSWORD": "testpass", "POSTGRES_DB": "testdb",
			},
			ExposedPorts: []string{"5432/tcp"},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)
	return fmt.Sprintf("testuser:testpass@%s:%s/testdb?sslmode=disable", host, port.Port())
}

func connectTimescale(t *testing.T, ctx context.Context, hostPart, options string) *PostgresDestination {
	t.Helper()
	dest := NewPostgresDestination()
	require.NoError(t, dest.Connect(ctx, "timescaledb://"+hostPart+"&"+options))
	t.Cleanup(func() { _ = dest.Close(context.Background()) })
	return dest
}

var timescaleEventsSchema = &schema.TableSchema{Columns: []schema.Column{
	{Name: "device_id", DataType: schema.TypeInt64},
	{Name: "ts", DataType: schema.TypeTimestampTZ},
	{Name: "value", DataType: schema.TypeFloat64, Nullable: true},
}}

func TestTimescaleHypertableAndCompression(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	for _, image := range []string{"timescale/timescaledb:latest-pg17", "timescale/timescaledb:2.12.2-pg15"} {
		t.Run(strings.ReplaceAll(image, "/", "_"), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			hostPart := startTimescaleContainer(t, ctx, image)
			dest := connectTimescale(t, ctx, hostPart,
				"time_column=ts&chunk_interval=1+day&segment_by=device_id&order_by=ts+DESC&compress_after=7+days&compress=now")

			require.NoError(t, dest.PrepareTable(ctx, destination.PrepareOptions{
				Table: "public.events", Schema: timescaleEventsSchema, PrimaryKeys: []string{"device_id", "ts"},
			}))
			require.NoError(t, dest.PrepareTable(ctx, destination.PrepareOptions{
				Table: "_bruin_staging.events_staging", Schema: timescaleEventsSchema, DropFirst: true, ExpiresAfter: destination.ManagedStagingTTL,
			}))

			var hypertables int
			require.NoError(t, dest.pool.QueryRow(ctx, `SELECT count(*) FROM timescaledb_information.hypertables`).Scan(&hypertables))
			require.Equal(t, 1, hypertables, "only the target becomes a hypertable")

			var interval time.Duration
			require.NoError(t, dest.pool.QueryRow(ctx,
				`SELECT time_interval FROM timescaledb_information.dimensions WHERE hypertable_name = 'events'`).Scan(&interval))
			require.Equal(t, 24*time.Hour, interval)

			require.NoError(t, dest.Exec(ctx, `
				INSERT INTO public.events
				SELECT d, g, d * 1.5 FROM generate_series(now() - interval '20 days', now(), interval '1 hour') g, generate_series(1, 3) d`))

			require.NoError(t, dest.FinalizeTable(ctx, "public.events"))

			var compressed, uncompressed int
			require.NoError(t, dest.pool.QueryRow(ctx, `
				SELECT count(*) FILTER (WHERE is_compressed), count(*) FILTER (WHERE NOT is_compressed)
				FROM timescaledb_information.chunks WHERE hypertable_name = 'events'`).Scan(&compressed, &uncompressed))
			require.GreaterOrEqual(t, compressed, 12)
			require.GreaterOrEqual(t, uncompressed, 7)

			var policies int
			require.NoError(t, dest.pool.QueryRow(ctx, `
				SELECT count(*) FROM timescaledb_information.jobs
				WHERE hypertable_name = 'events' AND proc_name = 'policy_compression'`).Scan(&policies))
			require.Equal(t, 1, policies)

			// Upserting into a compressed chunk leaves it partially compressed;
			// finalizing again must recompress it.
			require.NoError(t, dest.Exec(ctx, `
				INSERT INTO public.events VALUES (1, date_trunc('hour', now() - interval '15 days'), -1), (9, now() - interval '15 days', 9)
				ON CONFLICT (device_id, ts) DO UPDATE SET value = excluded.value`))
			require.NoError(t, dest.FinalizeTable(ctx, "public.events"))

			var total, updated int
			require.NoError(t, dest.pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE value < 0 OR device_id = 9) FROM public.events`).Scan(&total, &updated))
			require.Equal(t, 2, updated)

			var partial int
			require.NoError(t, dest.pool.QueryRow(ctx, `
				SELECT count(*) FROM _timescaledb_catalog.chunk c
				JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id
				WHERE h.table_name = 'events' AND c.status & 8 = 8`).Scan(&partial))
			require.Zero(t, partial, "no chunk should remain partially compressed")

			// PrepareTable on the existing hypertable is idempotent.
			require.NoError(t, dest.PrepareTable(ctx, destination.PrepareOptions{
				Table: "public.events", Schema: timescaleEventsSchema, PrimaryKeys: []string{"device_id", "ts"},
			}))
		})
	}
}

func TestTimescalePrepareTableGuards(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	hostPart := startTimescaleContainer(t, ctx, "timescale/timescaledb:latest-pg17")

	dest := connectTimescale(t, ctx, hostPart, "time_column=ts")
	err := dest.PrepareTable(ctx, destination.PrepareOptions{
		Table: "public.keyed", Schema: timescaleEventsSchema, PrimaryKeys: []string{"device_id"},
	})
	require.ErrorContains(t, err, `primary key to include time_column "ts"`)

	require.NoError(t, dest.Exec(ctx, `
		CREATE TABLE public.legacy (device_id bigint, ts timestamptz, value double precision);
		INSERT INTO public.legacy VALUES (1, now(), 1)`))
	err = dest.PrepareTable(ctx, destination.PrepareOptions{Table: "public.legacy", Schema: timescaleEventsSchema})
	require.ErrorContains(t, err, "migrate_data=true")

	migrating := connectTimescale(t, ctx, hostPart, "time_column=ts&migrate_data=true")
	require.NoError(t, migrating.PrepareTable(ctx, destination.PrepareOptions{Table: "public.legacy", Schema: timescaleEventsSchema}))
	_, isHypertable, err := migrating.hypertableTimeColumn(ctx, "public", "legacy")
	require.NoError(t, err)
	require.True(t, isHypertable)

	otherColumn := connectTimescale(t, ctx, hostPart, "time_column=other_ts")
	err = otherColumn.PrepareTable(ctx, destination.PrepareOptions{Table: "public.legacy", Schema: &schema.TableSchema{Columns: append(
		[]schema.Column{{Name: "other_ts", DataType: schema.TypeTimestampTZ, Nullable: true}}, timescaleEventsSchema.Columns...)}})
	require.ErrorContains(t, err, `already a hypertable partitioned by "ts"`)

	plain := NewPostgresDestination()
	require.NoError(t, plain.Connect(ctx, "postgres://"+hostPart))
	defer func() { _ = plain.Close(context.Background()) }()
	require.NoError(t, plain.Exec(ctx, `DROP EXTENSION timescaledb CASCADE`))
	err = NewPostgresDestination().Connect(ctx, "timescaledb://"+hostPart+"&time_column=ts")
	require.ErrorContains(t, err, "extension is not installed")
}

func TestTimescaleSwapKeepsHypertable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	hostPart := startTimescaleContainer(t, ctx, "timescale/timescaledb:latest-pg17")
	dest := connectTimescale(t, ctx, hostPart, "time_column=ts&compress_after=1+day")

	require.NoError(t, dest.PrepareTable(ctx, destination.PrepareOptions{Table: "public.events", Schema: timescaleEventsSchema}))
	require.NoError(t, dest.PrepareTable(ctx, destination.PrepareOptions{
		Table: "_bruin_staging.events_swap", TargetTable: "public.events", Schema: timescaleEventsSchema, DropFirst: true,
	}))
	require.NoError(t, dest.Exec(ctx, `INSERT INTO _bruin_staging.events_swap VALUES (1, now() - interval '3 days', 1)`))
	require.NoError(t, dest.SwapTable(ctx, destination.SwapOptions{StagingTable: "_bruin_staging.events_swap", TargetTable: "public.events"}))
	require.NoError(t, dest.FinalizeTable(ctx, "public.events"))

	_, isHypertable, err := dest.hypertableTimeColumn(ctx, "public", "events")
	require.NoError(t, err)
	require.True(t, isHypertable)

	var policies int
	require.NoError(t, dest.pool.QueryRow(ctx, `
		SELECT count(*) FROM timescaledb_information.jobs
		WHERE hypertable_schema = 'public' AND hypertable_name = 'events' AND proc_name = 'policy_compression'`).Scan(&policies))
	require.Equal(t, 1, policies)
}

func TestTimescaleInPlaceReplaceKeepsContinuousAggregate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	hostPart := startTimescaleContainer(t, ctx, "timescale/timescaledb:latest-pg17")
	dest := connectTimescale(t, ctx, hostPart, "time_column=ts&compress_after=1+day&compress=now")

	require.NoError(t, dest.PrepareTable(ctx, destination.PrepareOptions{Table: "public.events", Schema: timescaleEventsSchema}))
	require.NoError(t, dest.Exec(ctx, `INSERT INTO public.events VALUES (1, now() - interval '30 days', 1)`))
	require.NoError(t, dest.FinalizeTable(ctx, "public.events"))
	require.NoError(t, dest.Exec(ctx, `
		CREATE MATERIALIZED VIEW public.events_daily WITH (timescaledb.continuous) AS
		SELECT time_bucket('1 day', ts) AS bucket, device_id, sum(value) AS total FROM public.events GROUP BY 1, 2
		WITH NO DATA`))

	staging := "_bruin_staging.events_ti"
	require.NoError(t, dest.PrepareTable(ctx, destination.PrepareOptions{
		Table: staging, Schema: timescaleEventsSchema, DropFirst: true, ExpiresAfter: destination.ManagedStagingTTL,
	}))
	require.NoError(t, dest.Exec(ctx, `INSERT INTO _bruin_staging.events_ti VALUES (2, now() - interval '10 days', 2), (3, now(), 3)`))
	require.NoError(t, dest.TruncateInsertFromStaging(ctx, destination.TruncateInsertFromStagingOptions{
		StagingTable: staging, TargetTable: "public.events", Columns: []string{"device_id", "ts", "value"},
	}))
	require.NoError(t, dest.FinalizeTable(ctx, "public.events"))

	var devices []int64
	rows, err := dest.pool.Query(ctx, `SELECT device_id FROM public.events ORDER BY device_id`)
	require.NoError(t, err)
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		devices = append(devices, id)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int64{2, 3}, devices)

	require.NoError(t, dest.Exec(ctx, `CALL refresh_continuous_aggregate('public.events_daily', NULL, NULL)`))
	var buckets int
	require.NoError(t, dest.pool.QueryRow(ctx, `SELECT count(*) FROM public.events_daily`).Scan(&buckets))
	require.Equal(t, 2, buckets)
}

func TestTimescaleAppendCompressesCompletedChunks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	hostPart := startTimescaleContainer(t, ctx, "timescale/timescaledb:latest-pg17")
	dest := connectTimescale(t, ctx, hostPart, "time_column=ts&chunk_interval=1+day&segment_by=device_id&compress_after=7+days&compress=now")
	require.NoError(t, dest.PrepareTable(ctx, destination.PrepareOptions{Table: "public.events", Schema: timescaleEventsSchema}))
	previousInterval := timescaleCommitInterval
	timescaleCommitInterval = 200 * time.Millisecond
	t.Cleanup(func() { timescaleCommitInterval = previousInterval })

	countCompressed := func() int {
		var n int
		require.NoError(t, dest.pool.QueryRow(ctx, `
			SELECT count(*) FROM timescaledb_information.chunks
			WHERE hypertable_name = 'events' AND is_compressed`).Scan(&n))
		return n
	}

	records := make(chan source.RecordBatchResult)
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- dest.WriteParallel(ctx, records, destination.WriteOptions{Table: "public.events", Schema: timescaleEventsSchema, Parallelism: 2})
	}()

	start := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -20)
	for day := range 20 {
		records <- source.RecordBatchResult{Batch: timescaleEventsRecord(t, start.AddDate(0, 0, day), 24)}
		if day == 5 {
			require.Eventually(t, func() bool { return countCompressed() >= 4 }, 30*time.Second, 100*time.Millisecond,
				"chunks the load has moved past are compressed while it is still running")
		}
	}
	close(records)
	require.NoError(t, <-writeErr)
	require.NoError(t, dest.FinalizeTable(ctx, "public.events"))

	var total int
	require.NoError(t, dest.pool.QueryRow(ctx, `SELECT count(*) FROM public.events`).Scan(&total))
	require.Equal(t, 20*24, total)
	require.GreaterOrEqual(t, countCompressed(), 12)

	var partial int
	require.NoError(t, dest.pool.QueryRow(ctx, `
		SELECT count(*) FROM _timescaledb_catalog.chunk c
		JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id
		WHERE h.table_name = 'events' AND c.status & 8 = 8`).Scan(&partial))
	require.Zero(t, partial)
}

func timescaleEventsRecord(t *testing.T, from time.Time, hours int) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(memory.DefaultAllocator, timescaleEventsSchema.ToArrowSchema())
	defer b.Release()
	for h := range hours {
		b.Field(0).(*array.Int64Builder).Append(1)
		b.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(from.Add(time.Duration(h) * time.Hour).UnixMicro()))
		b.Field(2).(*array.Float64Builder).Append(float64(h))
	}
	return b.NewRecordBatch()
}

func TestTimescaleCompressNowWithoutCompressAfter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	hostPart := startTimescaleContainer(t, ctx, "timescale/timescaledb:latest-pg17")
	dest := connectTimescale(t, ctx, hostPart, "time_column=ts&chunk_interval=1+day&compress=now")
	require.NoError(t, dest.PrepareTable(ctx, destination.PrepareOptions{Table: "public.events", Schema: timescaleEventsSchema}))

	records := make(chan source.RecordBatchResult, 5)
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for day := range 5 {
		records <- source.RecordBatchResult{Batch: timescaleEventsRecord(t, start.AddDate(0, 0, day), 24)}
	}
	close(records)
	require.NoError(t, dest.WriteParallel(ctx, records, destination.WriteOptions{Table: "public.events", Schema: timescaleEventsSchema}))
	require.NoError(t, dest.FinalizeTable(ctx, "public.events"))

	var compressed, uncompressed int
	require.NoError(t, dest.pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE is_compressed), count(*) FILTER (WHERE NOT is_compressed)
		FROM timescaledb_information.chunks WHERE hypertable_name = 'events'`).Scan(&compressed, &uncompressed))
	require.Equal(t, 5, compressed)
	require.Zero(t, uncompressed)

	var policies int
	require.NoError(t, dest.pool.QueryRow(ctx, `
		SELECT count(*) FROM timescaledb_information.jobs
		WHERE hypertable_name = 'events' AND proc_name = 'policy_compression'`).Scan(&policies))
	require.Zero(t, policies, "no policy without compress_after")
}

func TestTimescaleAppendKeepsCompressingAfterFailedCopy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	hostPart := startTimescaleContainer(t, ctx, "timescale/timescaledb:latest-pg17")
	dest := connectTimescale(t, ctx, hostPart, "time_column=ts&chunk_interval=1+day&compress=now")
	require.NoError(t, dest.PrepareTable(ctx, destination.PrepareOptions{
		Table: "public.events", Schema: timescaleEventsSchema, PrimaryKeys: []string{"device_id", "ts"},
	}))
	previousInterval := timescaleCommitInterval
	timescaleCommitInterval = 200 * time.Millisecond
	t.Cleanup(func() { timescaleCommitInterval = previousInterval })

	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, dest.Exec(ctx, `INSERT INTO public.events VALUES (1, $1, 0)`, start))

	completedUncompressed := func() (int, int) {
		var compressed, pending int
		require.NoError(t, dest.pool.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE is_compressed),
			       count(*) FILTER (WHERE NOT is_compressed AND range_end <= (SELECT max(ts) FROM public.events))
			FROM timescaledb_information.chunks WHERE hypertable_name = 'events'`).Scan(&compressed, &pending))
		return compressed, pending
	}

	records := make(chan source.RecordBatchResult)
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- dest.WriteParallel(ctx, records, destination.WriteOptions{Table: "public.events", Schema: timescaleEventsSchema, Parallelism: 2})
	}()
	records <- source.RecordBatchResult{Batch: timescaleEventsRecord(t, start, 24)}
	time.Sleep(4 * timescaleCommitInterval)
	for day := 1; day <= 6; day++ {
		records <- source.RecordBatchResult{Batch: timescaleEventsRecord(t, start.AddDate(0, 0, day), 24)}
	}
	require.Eventually(t, func() bool {
		compressed, pending := completedUncompressed()
		return compressed >= 5 && pending == 0
	}, 30*time.Second, 100*time.Millisecond, "a rolled back COPY must not hold the watermark back")
	close(records)
	require.ErrorContains(t, <-writeErr, "duplicate key")
}
