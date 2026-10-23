package timescaledb

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectionURIStripsOptions(t *testing.T) {
	got, err := ConnectionURI("timescaledb://user:p%40ss@localhost:5432/db?sslmode=disable&time_column=ts&chunk_interval=1+day&segment_by=a,b&order_by=ts+DESC&compress_after=7+days&compress=now&migrate_data=true")
	require.NoError(t, err)

	parsed, err := url.Parse(got)
	require.NoError(t, err)
	require.Equal(t, "postgres", parsed.Scheme)
	require.Equal(t, "p@ss", func() string { p, _ := parsed.User.Password(); return p }())
	require.Equal(t, url.Values{"sslmode": {"disable"}}, parsed.Query())
}

func TestParseOptions(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    *Options
		wantErr string
	}{
		{
			name:  "time column only",
			query: "time_column=ts",
			want:  &Options{TimeColumn: "ts", Compress: CompressOff},
		},
		{
			name:  "compress_after defaults to policy",
			query: "time_column=ts&compress_after=7+days&segment_by=+device_id+,,tenant_id&order_by=ts+DESC&chunk_interval=1+day",
			want: &Options{
				TimeColumn:    "ts",
				ChunkInterval: "1 day",
				SegmentBy:     []string{"device_id", "tenant_id"},
				OrderBy:       "ts DESC",
				CompressAfter: "7 days",
				Compress:      CompressPolicy,
			},
		},
		{
			name:  "compress now with migrate",
			query: "time_column=ts&compress=NOW&compress_after=1+hour&migrate_data=true",
			want:  &Options{TimeColumn: "ts", CompressAfter: "1 hour", Compress: CompressNow, MigrateData: true},
		},
		{name: "missing time column", query: "compress=off", wantErr: "time_column is required"},
		{name: "now without compress_after", query: "time_column=ts&compress=now", want: &Options{TimeColumn: "ts", Compress: CompressNow}},
		{name: "policy without compress_after", query: "time_column=ts&compress=policy", wantErr: "compress=policy requires compress_after"},
		{name: "off with compress_after", query: "time_column=ts&compress=off&compress_after=1+day", wantErr: "no effect with compress=off"},
		{name: "invalid compress", query: "time_column=ts&compress=always", wantErr: `invalid compress value "always"`},
		{name: "invalid migrate_data", query: "time_column=ts&migrate_data=maybe", wantErr: "invalid migrate_data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseOptions("timescaledb://localhost/db?" + tt.query)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCompressionEnabled(t *testing.T) {
	require.False(t, (&Options{Compress: CompressOff}).CompressionEnabled())
	require.True(t, (&Options{Compress: CompressOff, SegmentBy: []string{"a"}}).CompressionEnabled())
	require.True(t, (&Options{Compress: CompressOff, OrderBy: "ts"}).CompressionEnabled())
	require.True(t, (&Options{Compress: CompressPolicy}).CompressionEnabled())
}

func TestIsURI(t *testing.T) {
	require.True(t, IsURI("timescaledb://localhost/db"))
	require.True(t, IsURI("TimescaleDB://localhost/db"))
	require.True(t, IsURI("timescale://localhost/db"))
	require.False(t, IsURI("postgres://localhost/db"))
	require.False(t, IsURI("timescaledb"))
}
