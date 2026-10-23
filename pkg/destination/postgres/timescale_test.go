package postgres

import (
	"testing"

	"github.com/bruin-data/ingestr/internal/timescaledb"
	"github.com/bruin-data/ingestr/pkg/destination"
	"github.com/bruin-data/ingestr/pkg/schema"
	"github.com/stretchr/testify/require"
)

func TestBuildCreateHypertableSQL(t *testing.T) {
	tests := []struct {
		name     string
		opts     *timescaledb.Options
		byRange  bool
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "by_range with interval",
			opts:     &timescaledb.Options{TimeColumn: "ts", ChunkInterval: "1 day"},
			byRange:  true,
			wantSQL:  `SELECT create_hypertable($1::regclass, by_range($2::name, $3::interval), if_not_exists => true, migrate_data => $4)`,
			wantArgs: []any{`"public"."events"`, "ts", "1 day", false},
		},
		{
			name:     "by_range default interval",
			opts:     &timescaledb.Options{TimeColumn: "ts", MigrateData: true},
			byRange:  true,
			wantSQL:  `SELECT create_hypertable($1::regclass, by_range($2::name), if_not_exists => true, migrate_data => $3)`,
			wantArgs: []any{`"public"."events"`, "ts", true},
		},
		{
			name:     "legacy with interval",
			opts:     &timescaledb.Options{TimeColumn: "ts", ChunkInterval: "7 days"},
			wantSQL:  `SELECT create_hypertable($1::regclass, $2::name, chunk_time_interval => $3::interval, if_not_exists => true, migrate_data => $4)`,
			wantArgs: []any{`"public"."events"`, "ts", "7 days", false},
		},
		{
			name:     "legacy default interval",
			opts:     &timescaledb.Options{TimeColumn: "ts"},
			wantSQL:  `SELECT create_hypertable($1::regclass, $2::name, if_not_exists => true, migrate_data => $3)`,
			wantArgs: []any{`"public"."events"`, "ts", false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, args := buildCreateHypertableSQL(`"public"."events"`, tt.opts, tt.byRange)
			require.Equal(t, tt.wantSQL, sql)
			require.Equal(t, tt.wantArgs, args)
		})
	}
}

func TestBuildCompressionSettingsSQL(t *testing.T) {
	require.Equal(t,
		`ALTER TABLE "public"."events" SET (timescaledb.compress)`,
		buildCompressionSettingsSQL(`"public"."events"`, &timescaledb.Options{}))
	require.Equal(t,
		`ALTER TABLE "public"."events" SET (timescaledb.compress, timescaledb.compress_segmentby = '"device_id", "it''s"', timescaledb.compress_orderby = 'ts DESC')`,
		buildCompressionSettingsSQL(`"public"."events"`, &timescaledb.Options{SegmentBy: []string{"device_id", "it's"}, OrderBy: "ts DESC"}))
}

func TestIsTimescaleHypertable(t *testing.T) {
	d := &PostgresDestination{timescale: &timescaleState{opts: &timescaledb.Options{TimeColumn: "ts"}}}

	require.True(t, d.isTimescaleHypertable(destination.PrepareOptions{Table: "public.events"}, "public"))
	require.False(t, d.isTimescaleHypertable(destination.PrepareOptions{Table: "_bruin_staging.x", ExpiresAfter: destination.ManagedStagingTTL}, managedStagingSchema))
	require.False(t, d.isTimescaleHypertable(destination.PrepareOptions{Table: "_bruin_staging._ingestr_cdc_state"}, managedStagingSchema))
	require.True(t, d.isTimescaleHypertable(destination.PrepareOptions{Table: "_bruin_staging.x", TargetTable: "public.events", ExpiresAfter: destination.ManagedStagingTTL}, managedStagingSchema))
	require.False(t, (&PostgresDestination{}).isTimescaleHypertable(destination.PrepareOptions{Table: "public.events"}, "public"))
}

func TestValidateTimescaleTable(t *testing.T) {
	opts := &timescaledb.Options{TimeColumn: "ts"}
	tableSchema := &schema.TableSchema{Columns: []schema.Column{
		{Name: "id", DataType: schema.TypeInt64},
		{Name: "ts", DataType: schema.TypeTimestampTZ},
		{Name: "label", DataType: schema.TypeString},
	}}

	require.NoError(t, validateTimescaleTable(opts, destination.PrepareOptions{Table: "events", Schema: tableSchema}))
	require.NoError(t, validateTimescaleTable(opts, destination.PrepareOptions{Table: "events", Schema: tableSchema, PrimaryKeys: []string{"id", "ts"}}))

	err := validateTimescaleTable(opts, destination.PrepareOptions{Table: "events", Schema: tableSchema, PrimaryKeys: []string{"id"}})
	require.ErrorContains(t, err, `primary key to include time_column "ts"`)

	err = validateTimescaleTable(opts, destination.PrepareOptions{Table: "events", Schema: tableSchema, DeferredPrimaryKeys: []string{"id"}})
	require.ErrorContains(t, err, `primary key to include time_column "ts"`)

	err = validateTimescaleTable(&timescaledb.Options{TimeColumn: "missing"}, destination.PrepareOptions{Table: "events", Schema: tableSchema})
	require.ErrorContains(t, err, `time_column "missing" is not a column`)

	err = validateTimescaleTable(&timescaledb.Options{TimeColumn: "label"}, destination.PrepareOptions{Table: "events", Schema: tableSchema})
	require.ErrorContains(t, err, "must be a timestamp, timestamptz, or date column")
}

func TestParseTimescaleVersion(t *testing.T) {
	v, err := parseTimescaleVersion("2.17.2")
	require.NoError(t, err)
	require.Equal(t, [2]int{2, 17}, v)

	v, err = parseTimescaleVersion("2.13-dev")
	require.NoError(t, err)
	require.Equal(t, [2]int{2, 13}, v)

	_, err = parseTimescaleVersion("garbage")
	require.Error(t, err)

	require.Negative(t, compareTimescaleVersion([2]int{2, 10}, timescaleMinVersion))
	require.Zero(t, compareTimescaleVersion([2]int{2, 13}, timescaleByRangeVersion))
	require.Positive(t, compareTimescaleVersion([2]int{3, 0}, timescaleByRangeVersion))
}
