/*
Copyright 2015 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
Package bttest contains test helpers for working with the bigtable package.

To use a Server, create it, and then connect to it with no security:
(The project/instance values are ignored.)
	srv, err := bttest.NewServer("localhost:0")
	...
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	...
	client, err := bigtable.NewClient(ctx, proj, instance,
	        option.WithGRPCConn(conn))
	...
*/
package bttest // import "cloud.google.com/go/bigtable/bttest"

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	emptypb "github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
	btapb "google.golang.org/genproto/googleapis/bigtable/admin/v2"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/genproto/googleapis/longrunning"
	statpb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"rsc.io/binaryregexp"
)

const (
	// MilliSeconds field of the minimum valid Timestamp.
	minValidMilliSeconds = 0

	// MilliSeconds field of the max valid Timestamp.
	// Must match the max value of type TimestampMicros (int64)
	// truncated to the millis granularity by subtracting a remainder of 1000.
	maxValidMilliSeconds = math.MaxInt64 - math.MaxInt64%1000
)

var validLabelTransformer = regexp.MustCompile(`[a-z0-9\-]{1,15}`)

// Server is an in-memory Cloud Bigtable fake.
// It is unauthenticated, and only a rough approximation.
type Server struct {
	Addr string

	l   net.Listener
	srv *grpc.Server
	s   *server
}

// server is the real implementation of the fake.
// It is a separate and unexported type so the API won't be cluttered with
// methods that are only relevant to the fake's implementation.
type server struct {
	rowsFactory RowsFactory

	mu     sync.Mutex
	tables map[string]*table // keyed by fully qualified name
	gcc    chan int          // set when gcloop starts, closed when server shuts down

	// Any unimplemented methods will return unimplemented.
	*btapb.UnimplementedBigtableTableAdminServer
	*btapb.UnimplementedBigtableInstanceAdminServer
	*btpb.UnimplementedBigtableServer
}

// NewServer creates a new Server.
// The Server will be listening for gRPC connections, without TLS,
// on the provided address. The resolved address is named by the Addr field.
func NewServer(laddr string, opt ...grpc.ServerOption) (*Server, error) {
	l, err := net.Listen("tcp", laddr)
	if err != nil {
		return nil, err
	}

	s := &Server{
		Addr: l.Addr().String(),
		l:    l,
		srv:  grpc.NewServer(opt...),
		s: &server{
			tables: make(map[string]*table),
		},
	}
	btapb.RegisterBigtableInstanceAdminServer(s.srv, s.s)
	btapb.RegisterBigtableTableAdminServer(s.srv, s.s)
	btpb.RegisterBigtableServer(s.srv, s.s)

	go s.srv.Serve(s.l)

	return s, nil
}

// Close shuts down the server.
func (s *Server) Close() {
	s.s.mu.Lock()
	if s.s.gcc != nil {
		close(s.s.gcc)
	}
	s.s.mu.Unlock()

	s.srv.Stop()
	s.l.Close()
}

func (s *Server) SetFactory(fac RowsFactory) {
	s.s.rowsFactory = fac
}

func (s *server) CreateTable(ctx context.Context, req *btapb.CreateTableRequest) (*btapb.Table, error) {
	tbl := req.Parent + "/tables/" + req.TableId

	s.mu.Lock()
	if _, ok := s.tables[tbl]; ok {
		s.mu.Unlock()
		return nil, status.Errorf(codes.AlreadyExists, "table %q already exists", tbl)
	}
	rowsFactory := s.rowsFactory
	if rowsFactory == nil {
		rowsFactory = btreeFactory{}
	}
	s.tables[tbl] = newTable(req, rowsFactory)
	s.mu.Unlock()

	ct := &btapb.Table{
		Name:           tbl,
		ColumnFamilies: req.GetTable().GetColumnFamilies(),
		Granularity:    req.GetTable().GetGranularity(),
	}
	if ct.Granularity == 0 {
		ct.Granularity = btapb.Table_MILLIS
	}
	return ct, nil
}

func (s *server) CreateTableFromSnapshot(context.Context, *btapb.CreateTableFromSnapshotRequest) (*longrunning.Operation, error) {
	return nil, status.Errorf(codes.Unimplemented, "the emulator does not currently support snapshots")
}

func (s *server) ListTables(ctx context.Context, req *btapb.ListTablesRequest) (*btapb.ListTablesResponse, error) {
	res := &btapb.ListTablesResponse{}
	prefix := req.Parent + "/tables/"

	s.mu.Lock()
	for tbl := range s.tables {
		if strings.HasPrefix(tbl, prefix) {
			res.Tables = append(res.Tables, &btapb.Table{Name: tbl})
		}
	}
	s.mu.Unlock()

	return res, nil
}

func (s *server) GetTable(ctx context.Context, req *btapb.GetTableRequest) (*btapb.Table, error) {
	tbl := req.Name

	s.mu.Lock()
	tblIns, ok := s.tables[tbl]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", tbl)
	}

	return &btapb.Table{
		Name:           tbl,
		ColumnFamilies: toColumnFamilies(tblIns.columnFamilies()),
	}, nil
}

func (s *server) DeleteTable(ctx context.Context, req *btapb.DeleteTableRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tables[req.Name]; !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.Name)
	}
	delete(s.tables, req.Name)
	return &emptypb.Empty{}, nil
}

func (s *server) ModifyColumnFamilies(ctx context.Context, req *btapb.ModifyColumnFamiliesRequest) (*btapb.Table, error) {
	s.mu.Lock()
	tbl, ok := s.tables[req.Name]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.Name)
	}

	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	for _, mod := range req.Modifications {
		if create := mod.GetCreate(); create != nil {
			if _, ok := tbl.families[mod.Id]; ok {
				return nil, status.Errorf(codes.AlreadyExists, "family %q already exists", mod.Id)
			}
			newcf := &columnFamily{
				name:   req.Name + "/columnFamilies/" + mod.Id,
				order:  tbl.counter,
				gcRule: create.GcRule,
			}
			tbl.counter++
			tbl.families[mod.Id] = newcf
		} else if mod.GetDrop() {
			if _, ok := tbl.families[mod.Id]; !ok {
				return nil, fmt.Errorf("can't delete unknown family %q", mod.Id)
			}
			delete(tbl.families, mod.Id)
		} else if modify := mod.GetUpdate(); modify != nil {
			if _, ok := tbl.families[mod.Id]; !ok {
				return nil, fmt.Errorf("no such family %q", mod.Id)
			}
			newcf := &columnFamily{
				name:   req.Name + "/columnFamilies/" + mod.Id,
				gcRule: modify.GcRule,
			}
			// assume that we ALWAYS want to replace by the new setting
			// we may need partial update through
			tbl.families[mod.Id] = newcf
		}
	}

	s.needGC()
	return &btapb.Table{
		Name:           req.Name,
		ColumnFamilies: toColumnFamilies(tbl.families),
		Granularity:    btapb.Table_TimestampGranularity(btapb.Table_MILLIS),
	}, nil
}

func (s *server) DropRowRange(ctx context.Context, req *btapb.DropRowRangeRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tbl, ok := s.tables[req.Name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.Name)
	}

	if req.GetDeleteAllDataFromTable() {
		tbl.rows.Clear()
	} else {
		// Delete rows by prefix.
		prefixBytes := req.GetRowKeyPrefix()
		if prefixBytes == nil {
			return nil, fmt.Errorf("missing row key prefix")
		}

		// Rows does not specify what happens if rows are deleted during
		// iteration, and it provides no "delete range" method.
		// So we collect the rows first, then delete them one by one.
		var rowsToDelete []keyType
		tbl.rows.AscendGreaterOrEqual(prefixBytes, func(r *btpb.Row) bool {
			if bytes.HasPrefix(r.Key, prefixBytes) {
				rowsToDelete = append(rowsToDelete, r.Key)
				return true
			}
			return false // stop iteration
		})
		for _, r := range rowsToDelete {
			tbl.rows.Delete(r)
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *server) GenerateConsistencyToken(ctx context.Context, req *btapb.GenerateConsistencyTokenRequest) (*btapb.GenerateConsistencyTokenResponse, error) {
	// Check that the table exists.
	_, ok := s.tables[req.Name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.Name)
	}

	return &btapb.GenerateConsistencyTokenResponse{
		ConsistencyToken: "TokenFor-" + req.Name,
	}, nil
}

func (s *server) CheckConsistency(ctx context.Context, req *btapb.CheckConsistencyRequest) (*btapb.CheckConsistencyResponse, error) {
	// Check that the table exists.
	_, ok := s.tables[req.Name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.Name)
	}

	// Check this is the right token.
	if req.ConsistencyToken != "TokenFor-"+req.Name {
		return nil, status.Errorf(codes.InvalidArgument, "token %q not valid", req.ConsistencyToken)
	}

	// Single cluster instances are always consistent.
	return &btapb.CheckConsistencyResponse{
		Consistent: true,
	}, nil
}

func (s *server) SnapshotTable(context.Context, *btapb.SnapshotTableRequest) (*longrunning.Operation, error) {
	return nil, status.Errorf(codes.Unimplemented, "the emulator does not currently support snapshots")
}

func (s *server) GetSnapshot(context.Context, *btapb.GetSnapshotRequest) (*btapb.Snapshot, error) {
	return nil, status.Errorf(codes.Unimplemented, "the emulator does not currently support snapshots")
}

func (s *server) ListSnapshots(context.Context, *btapb.ListSnapshotsRequest) (*btapb.ListSnapshotsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "the emulator does not currently support snapshots")
}

func (s *server) DeleteSnapshot(context.Context, *btapb.DeleteSnapshotRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "the emulator does not currently support snapshots")
}

func (s *server) ReadRows(req *btpb.ReadRowsRequest, stream btpb.Bigtable_ReadRowsServer) error {
	s.mu.Lock()
	tbl, ok := s.tables[req.TableName]
	s.mu.Unlock()
	if !ok {
		return status.Errorf(codes.NotFound, "table %q not found", req.TableName)
	}

	if err := validateRowRanges(req); err != nil {
		return err
	}

	// Rows to read can be specified by a set of row keys and/or a set of row ranges.
	// Output is a stream of sorted, de-duped rows.
	tbl.mu.RLock()
	rowSet := make(map[string]*btpb.Row)

	addRow := func(r *btpb.Row) bool {
		rowSet[string(r.Key)] = r
		return true
	}

	if req.Rows != nil &&
		len(req.Rows.RowKeys)+len(req.Rows.RowRanges) > 0 {
		// Add the explicitly given keys
		for _, key := range req.Rows.RowKeys {
			if i := tbl.rows.Get(key); i != nil {
				addRow(i)
			}
		}

		// Add keys from row ranges
		for _, rr := range req.Rows.RowRanges {
			var start, end keyType
			switch sk := rr.StartKey.(type) {
			case *btpb.RowRange_StartKeyClosed:
				start = sk.StartKeyClosed
			case *btpb.RowRange_StartKeyOpen:
				start = append(sk.StartKeyOpen, 0)
			}
			switch ek := rr.EndKey.(type) {
			case *btpb.RowRange_EndKeyClosed:
				end = append(ek.EndKeyClosed, 0)
			case *btpb.RowRange_EndKeyOpen:
				end = ek.EndKeyOpen
			}
			switch {
			case len(start) == 0 && len(end) == 0:
				tbl.rows.Ascend(addRow) // all rows
			case len(start) == 0:
				tbl.rows.AscendLessThan(end, addRow)
			case len(end) == 0:
				tbl.rows.AscendGreaterOrEqual(start, addRow)
			default:
				tbl.rows.AscendRange(start, end, addRow)
			}
		}
	} else {
		// Read all rows
		tbl.rows.Ascend(addRow)
	}
	tbl.mu.RUnlock()

	rows := make([]*btpb.Row, 0, len(rowSet))
	for _, r := range rowSet {
		fams := len(r.Families)
		if fams != 0 {
			rows = append(rows, r)
		}
	}
	sort.Sort(byRowKey(rows))

	limit := int(req.RowsLimit)
	count := 0
	for _, r := range rows {
		if limit > 0 && count >= limit {
			return nil
		}
		streamed, err := streamRow(stream, tbl.columnFamilies(), r, req.Filter)
		if err != nil {
			return err
		}
		if streamed {
			count++
		}
	}
	return nil
}

// streamRow filters the given row and sends it via the given stream.
// Returns true if at least one cell matched the filter and was streamed, false otherwise.
func streamRow(stream btpb.Bigtable_ReadRowsServer, fs map[string]*columnFamily, r *btpb.Row, f *btpb.RowFilter) (bool, error) {
	match, err := filterRow(f, r)
	if err != nil {
		return false, err
	}
	if !match {
		return false, nil
	}

	rrr := &btpb.ReadRowsResponse{}
	for _, fam := range r.Families {
		for _, col := range fam.Columns {
			cells := col.Cells
			if len(cells) == 0 {
				continue
			}
			for _, cell := range cells {
				rrr.Chunks = append(rrr.Chunks, &btpb.ReadRowsResponse_CellChunk{
					RowKey:          r.Key,
					FamilyName:      &wrappers.StringValue{Value: fam.Name},
					Qualifier:       &wrappers.BytesValue{Value: col.Qualifier},
					TimestampMicros: cell.TimestampMicros,
					Value:           cell.Value,
					Labels:          cell.Labels,
				})
			}
		}
	}
	// We can't have a cell with just COMMIT set, which would imply a new empty cell.
	// So modify the last cell to have the COMMIT flag set.
	if len(rrr.Chunks) > 0 {
		rrr.Chunks[len(rrr.Chunks)-1].RowStatus = &btpb.ReadRowsResponse_CellChunk_CommitRow{CommitRow: true}
	}

	return true, stream.Send(rrr)
}

// filterRow modifies a row with the given filter. Returns true if at least one cell from the row matches,
// false otherwise. If a filter is invalid, filterRow returns false and an error.
func filterRow(f *btpb.RowFilter, r *btpb.Row) (bool, error) {
	if f == nil {
		return true, nil
	}
	// Handle filters that apply beyond just including/excluding cells.
	switch f := f.Filter.(type) {
	case *btpb.RowFilter_BlockAllFilter:
		return !f.BlockAllFilter, nil
	case *btpb.RowFilter_PassAllFilter:
		return f.PassAllFilter, nil
	case *btpb.RowFilter_Chain_:
		for _, sub := range f.Chain.Filters {
			match, err := filterRow(sub, r)
			if err != nil {
				return false, err
			}
			if !match {
				return false, nil
			}
		}
		return true, nil
	case *btpb.RowFilter_Interleave_:
		srs := make([]*btpb.Row, 0, len(f.Interleave.Filters))
		for _, sub := range f.Interleave.Filters {
			sr := copyRow(r)
			match, err := filterRow(sub, sr)
			if err != nil {
				return false, err
			}
			if match {
				srs = append(srs, sr)
			}
		}
		// merge
		// TODO(dsymonds): is this correct?
		r.Families = nil
		for _, sr := range srs {
			for _, fam := range sr.Families {
				f := getOrCreateFamily(r, fam.Name)
				for _, col := range fam.Columns {
					c := getOrCreateColumn(f, col.Qualifier)
					c.Cells = append(c.Cells, col.Cells...)
				}
			}
		}
		var count int
		for _, fam := range r.Families {
			for _, col := range fam.Columns {
				sort.Sort(byDescTS(col.Cells))
				count += len(col.Cells)
			}
		}
		return count > 0, nil
	case *btpb.RowFilter_CellsPerColumnLimitFilter:
		lim := int(f.CellsPerColumnLimitFilter)
		for _, fam := range r.Families {
			for _, col := range fam.Columns {
				if len(col.Cells) > lim {
					col.Cells = col.Cells[:lim]
				}
			}
		}
		return true, nil
	case *btpb.RowFilter_Condition_:
		match, err := filterRow(f.Condition.PredicateFilter, copyRow(r))
		if err != nil {
			return false, err
		}
		if match {
			if f.Condition.TrueFilter == nil {
				return false, nil
			}
			return filterRow(f.Condition.TrueFilter, r)
		}
		if f.Condition.FalseFilter == nil {
			return false, nil
		}
		return filterRow(f.Condition.FalseFilter, r)
	case *btpb.RowFilter_RowKeyRegexFilter:
		rx, err := newRegexp(f.RowKeyRegexFilter)
		if err != nil {
			return false, status.Errorf(codes.InvalidArgument, "Error in field 'rowkey_regex_filter' : %v", err)
		}
		if !rx.Match(r.Key) {
			return false, nil
		}
	case *btpb.RowFilter_CellsPerRowLimitFilter:
		// Grab the first n cells in the row.
		lim := int(f.CellsPerRowLimitFilter)
		for _, fam := range r.Families {
			for _, col := range fam.Columns {
				if len(col.Cells) > lim {
					col.Cells = col.Cells[:lim]
					lim = 0
				} else {
					lim -= len(col.Cells)
				}
			}
		}
		return true, nil
	case *btpb.RowFilter_CellsPerRowOffsetFilter:
		// Skip the first n cells in the row.
		offset := int(f.CellsPerRowOffsetFilter)
		for _, fam := range r.Families {
			for _, col := range fam.Columns {
				if len(col.Cells) > offset {
					col.Cells = col.Cells[offset:]
					return true, nil
				}
				col.Cells = col.Cells[:0]
				offset -= len(col.Cells)
			}
		}
		return true, nil
	case *btpb.RowFilter_RowSampleFilter:
		// The row sample filter "matches all cells from a row with probability
		// p, and matches no cells from the row with probability 1-p."
		// See https://github.com/googleapis/googleapis/blob/master/google/bigtable/v2/data.proto
		if f.RowSampleFilter <= 0.0 || f.RowSampleFilter >= 1.0 {
			return false, status.Error(codes.InvalidArgument, "row_sample_filter argument must be between 0.0 and 1.0")
		}
		return randFloat() < f.RowSampleFilter, nil
	}

	// Any other case, operate on a per-cell basis.
	cellCount := 0
	for _, fam := range r.Families {
		for _, col := range fam.Columns {
			filtered, err := filterCells(f, fam.Name, col.Qualifier, col.Cells)
			if err != nil {
				return false, err
			}
			col.Cells = filtered
			cellCount += len(col.Cells)
		}
	}
	return cellCount > 0, nil
}

var randFloat = rand.Float64

func filterCells(f *btpb.RowFilter, fam string, col []byte, cs []*btpb.Cell) ([]*btpb.Cell, error) {
	var ret []*btpb.Cell
	for _, cell := range cs {
		include, err := includeCell(f, fam, col, cell)
		if err != nil {
			return nil, err
		}
		if include {
			cell, err = modifyCell(f, cell)
			if err != nil {
				return nil, err
			}
			ret = append(ret, cell)
		}
	}
	return ret, nil
}

func modifyCell(f *btpb.RowFilter, c *btpb.Cell) (*btpb.Cell, error) {
	if f == nil {
		return c, nil
	}
	// Consider filters that may modify the cell contents
	switch filter := f.Filter.(type) {
	case *btpb.RowFilter_StripValueTransformer:
		return &btpb.Cell{TimestampMicros: c.TimestampMicros}, nil
	case *btpb.RowFilter_ApplyLabelTransformer:
		if !validLabelTransformer.MatchString(filter.ApplyLabelTransformer) {
			return &btpb.Cell{}, status.Errorf(
				codes.InvalidArgument,
				`apply_label_transformer must match RE2([a-z0-9\-]+), but found %v`,
				filter.ApplyLabelTransformer,
			)
		}
		return &btpb.Cell{
			TimestampMicros: c.TimestampMicros,
			Value:           c.Value,
			Labels:          []string{filter.ApplyLabelTransformer},
		}, nil
	default:
		return c, nil
	}
}

func includeCell(f *btpb.RowFilter, fam string, col []byte, cell *btpb.Cell) (bool, error) {
	if f == nil {
		return true, nil
	}
	// TODO(dsymonds): Implement many more filters.
	switch f := f.Filter.(type) {
	case *btpb.RowFilter_CellsPerColumnLimitFilter:
		// Don't log, row-level filter
		return true, nil
	case *btpb.RowFilter_RowKeyRegexFilter:
		// Don't log, row-level filter
		return true, nil
	case *btpb.RowFilter_StripValueTransformer:
		// Don't log, cell-modifying filter
		return true, nil
	case *btpb.RowFilter_ApplyLabelTransformer:
		// Don't log, cell-modifying filter
		return true, nil
	default:
		log.Printf("WARNING: don't know how to handle filter of type %T (ignoring it)", f)
		return true, nil
	case *btpb.RowFilter_FamilyNameRegexFilter:
		rx, err := newRegexp([]byte(f.FamilyNameRegexFilter))
		if err != nil {
			return false, status.Errorf(codes.InvalidArgument, "Error in field 'family_name_regex_filter' : %v", err)
		}
		return rx.MatchString(fam), nil
	case *btpb.RowFilter_ColumnQualifierRegexFilter:
		rx, err := newRegexp(f.ColumnQualifierRegexFilter)
		if err != nil {
			return false, status.Errorf(codes.InvalidArgument, "Error in field 'column_qualifier_regex_filter' : %v", err)
		}
		return rx.Match(col), nil
	case *btpb.RowFilter_ValueRegexFilter:
		rx, err := newRegexp(f.ValueRegexFilter)
		if err != nil {
			return false, status.Errorf(codes.InvalidArgument, "Error in field 'value_regex_filter' : %v", err)
		}
		return rx.Match(cell.Value), nil
	case *btpb.RowFilter_ColumnRangeFilter:
		if fam != f.ColumnRangeFilter.FamilyName {
			return false, nil
		}
		// Start qualifier defaults to empty string closed
		inRangeStart := func() bool { return bytes.Compare(col, nil) >= 0 }
		switch sq := f.ColumnRangeFilter.StartQualifier.(type) {
		case *btpb.ColumnRange_StartQualifierOpen:
			inRangeStart = func() bool { return bytes.Compare(col, sq.StartQualifierOpen) > 0 }
		case *btpb.ColumnRange_StartQualifierClosed:
			inRangeStart = func() bool { return bytes.Compare(col, sq.StartQualifierClosed) >= 0 }
		}
		// End qualifier defaults to no upper boundary
		inRangeEnd := func() bool { return true }
		switch eq := f.ColumnRangeFilter.EndQualifier.(type) {
		case *btpb.ColumnRange_EndQualifierClosed:
			inRangeEnd = func() bool { return bytes.Compare(col, eq.EndQualifierClosed) <= 0 }
		case *btpb.ColumnRange_EndQualifierOpen:
			inRangeEnd = func() bool { return bytes.Compare(col, eq.EndQualifierOpen) < 0 }
		}
		return inRangeStart() && inRangeEnd(), nil
	case *btpb.RowFilter_TimestampRangeFilter:
		// Server should only support millisecond precision.
		if f.TimestampRangeFilter.StartTimestampMicros%int64(time.Millisecond/time.Microsecond) != 0 || f.TimestampRangeFilter.EndTimestampMicros%int64(time.Millisecond/time.Microsecond) != 0 {
			return false, status.Errorf(codes.InvalidArgument, "Error in field 'timestamp_range_filter'. Maximum precision allowed in filter is millisecond.\nGot:\nStart: %v\nEnd: %v", f.TimestampRangeFilter.StartTimestampMicros, f.TimestampRangeFilter.EndTimestampMicros)
		}
		// Lower bound is inclusive and defaults to 0, upper bound is exclusive and defaults to infinity.
		return cell.TimestampMicros >= f.TimestampRangeFilter.StartTimestampMicros &&
			(f.TimestampRangeFilter.EndTimestampMicros == 0 || cell.TimestampMicros < f.TimestampRangeFilter.EndTimestampMicros), nil
	case *btpb.RowFilter_ValueRangeFilter:
		v := cell.Value
		// Start value defaults to empty string closed
		inRangeStart := func() bool { return bytes.Compare(v, []byte{}) >= 0 }
		switch sv := f.ValueRangeFilter.StartValue.(type) {
		case *btpb.ValueRange_StartValueOpen:
			inRangeStart = func() bool { return bytes.Compare(v, sv.StartValueOpen) > 0 }
		case *btpb.ValueRange_StartValueClosed:
			inRangeStart = func() bool { return bytes.Compare(v, sv.StartValueClosed) >= 0 }
		}
		// End value defaults to no upper boundary
		inRangeEnd := func() bool { return true }
		switch ev := f.ValueRangeFilter.EndValue.(type) {
		case *btpb.ValueRange_EndValueClosed:
			inRangeEnd = func() bool { return bytes.Compare(v, ev.EndValueClosed) <= 0 }
		case *btpb.ValueRange_EndValueOpen:
			inRangeEnd = func() bool { return bytes.Compare(v, ev.EndValueOpen) < 0 }
		}
		return inRangeStart() && inRangeEnd(), nil
	}
}

// escapeUTF is used to escape non-ASCII characters in pattern strings passed
// to binaryregexp. This makes regexp column and row key matching work more
// closely to what's seen with the real BigTable.
func escapeUTF(in []byte) []byte {
	var toEsc int
	for _, c := range in {
		if c > 127 {
			toEsc++
		}
	}
	if toEsc == 0 {
		return in
	}
	// Each escaped byte becomes 4 bytes (byte a1 becomes \xA1)
	out := make([]byte, 0, len(in)+3*toEsc)
	for _, c := range in {
		if c > 127 {
			h, l := c>>4, c&0xF
			const conv = "0123456789ABCDEF"
			out = append(out, '\\', 'x', conv[h], conv[l])
		} else {
			out = append(out, c)
		}
	}
	return out
}

func newRegexp(pat []byte) (*binaryregexp.Regexp, error) {
	re, err := binaryregexp.Compile("^(?:" + string(escapeUTF(pat)) + ")$") // match entire target
	if err != nil {
		log.Printf("Bad pattern %q: %v", pat, err)
	}
	return re, err
}

func (s *server) MutateRow(ctx context.Context, req *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error) {
	s.mu.Lock()
	tbl, ok := s.tables[req.TableName]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.TableName)
	}

	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	r := tbl.getOrCreateRow(req.RowKey)

	if err := applyMutations(tbl, r, req.Mutations, tbl.families); err != nil {
		return nil, err
	}
	tbl.rows.ReplaceOrInsert(scrubRow(r, tbl.families))
	return &btpb.MutateRowResponse{}, nil
}

func (s *server) MutateRows(req *btpb.MutateRowsRequest, stream btpb.Bigtable_MutateRowsServer) error {
	s.mu.Lock()
	tbl, ok := s.tables[req.TableName]
	s.mu.Unlock()
	if !ok {
		return status.Errorf(codes.NotFound, "table %q not found", req.TableName)
	}
	res := &btpb.MutateRowsResponse{Entries: make([]*btpb.MutateRowsResponse_Entry, len(req.Entries))}

	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	for i, entry := range req.Entries {
		func() {
			r := tbl.getOrCreateRow(entry.RowKey)

			code, msg := int32(codes.OK), ""
			if err := applyMutations(tbl, r, entry.Mutations, tbl.families); err != nil {
				code = int32(codes.Internal)
				msg = err.Error()
			}
			tbl.rows.ReplaceOrInsert(scrubRow(r, tbl.families))
			res.Entries[i] = &btpb.MutateRowsResponse_Entry{
				Index:  int64(i),
				Status: &statpb.Status{Code: code, Message: msg},
			}
		}()
	}
	return stream.Send(res)
}

func (s *server) CheckAndMutateRow(ctx context.Context, req *btpb.CheckAndMutateRowRequest) (*btpb.CheckAndMutateRowResponse, error) {
	s.mu.Lock()
	tbl, ok := s.tables[req.TableName]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.TableName)
	}
	res := &btpb.CheckAndMutateRowResponse{}

	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	r := tbl.getOrCreateRow(req.RowKey)

	// Figure out which mutation to apply.
	whichMut := false
	if req.PredicateFilter == nil {
		// Use true_mutations iff row contains any cells.
		whichMut = !isEmpty(r)
	} else {
		// Use true_mutations iff any cells in the row match the filter.
		// TODO(dsymonds): This could be cheaper.
		nr := copyRow(r)

		match, err := filterRow(req.PredicateFilter, nr)
		if err != nil {
			return nil, err
		}
		whichMut = match && !isEmpty(nr)
	}
	res.PredicateMatched = whichMut
	muts := req.FalseMutations
	if whichMut {
		muts = req.TrueMutations
	}

	if err := applyMutations(tbl, r, muts, tbl.families); err != nil {
		return nil, err
	}
	tbl.rows.ReplaceOrInsert(scrubRow(r, tbl.families))
	return res, nil
}

// applyMutations applies a sequence of mutations to a row.
// fam should be a snapshot of the keys of tbl.families.
// It assumes r.mu is locked.
func applyMutations(tbl *table, r *btpb.Row, muts []*btpb.Mutation, fs map[string]*columnFamily) error {
	for _, mut := range muts {
		switch mut := mut.Mutation.(type) {
		default:
			return fmt.Errorf("can't handle mutation type %T", mut)
		case *btpb.Mutation_SetCell_:
			set := mut.SetCell
			if _, ok := fs[set.FamilyName]; !ok {
				return fmt.Errorf("unknown family %q", set.FamilyName)
			}
			ts := set.TimestampMicros
			if ts == -1 { // bigtable.ServerTime
				ts = newTimestamp()
			}
			if !tbl.validTimestamp(ts) {
				return fmt.Errorf("invalid timestamp %d", ts)
			}
			fam := set.FamilyName
			col := set.ColumnQualifier

			newCell := &btpb.Cell{TimestampMicros: ts, Value: set.Value}
			f := getOrCreateFamily(r, fam)
			c := getOrCreateColumn(f, col)
			c.Cells = appendOrReplaceCell(c.Cells, newCell)
		case *btpb.Mutation_DeleteFromColumn_:
			del := mut.DeleteFromColumn
			if _, ok := fs[del.FamilyName]; !ok {
				return fmt.Errorf("unknown family %q", del.FamilyName)
			}
			fam := getFamily(r, del.FamilyName)
			if fam == nil {
				break
			}
			col := getColumn(fam, del.ColumnQualifier)
			if col == nil {
				break
			}
			cs := col.Cells
			if del.TimeRange != nil {
				tsr := del.TimeRange
				if !tbl.validTimestamp(tsr.StartTimestampMicros) {
					return fmt.Errorf("invalid timestamp %d", tsr.StartTimestampMicros)
				}
				if !tbl.validTimestamp(tsr.EndTimestampMicros) && tsr.EndTimestampMicros != 0 {
					return fmt.Errorf("invalid timestamp %d", tsr.EndTimestampMicros)
				}
				if tsr.StartTimestampMicros >= tsr.EndTimestampMicros && tsr.EndTimestampMicros != 0 {
					return fmt.Errorf("inverted or invalid timestamp range [%d, %d]", tsr.StartTimestampMicros, tsr.EndTimestampMicros)
				}

				// Find half-open interval to remove.
				// Cells are in descending timestamp order,
				// so the predicates to sort.Search are inverted.
				si, ei := 0, len(cs)
				if tsr.StartTimestampMicros > 0 {
					ei = sort.Search(len(cs), func(i int) bool { return cs[i].TimestampMicros < tsr.StartTimestampMicros })
				}
				if tsr.EndTimestampMicros > 0 {
					si = sort.Search(len(cs), func(i int) bool { return cs[i].TimestampMicros < tsr.EndTimestampMicros })
				}
				if si < ei {
					copy(cs[si:], cs[ei:])
					cs = cs[:len(cs)-(ei-si)]
				}
			} else {
				cs = nil
			}
			col.Cells = cs
		case *btpb.Mutation_DeleteFromRow_:
			r.Families = nil
		case *btpb.Mutation_DeleteFromFamily_:
			if f := getFamily(r, mut.DeleteFromFamily.FamilyName); f != nil {
				f.Columns = nil
			}
		}
	}
	return nil
}

// Remove empty families / columns
func scrubRow(r *btpb.Row, fs map[string]*columnFamily) *btpb.Row {
	wIdx := 0
	for _, f := range r.Families {
		f = scrubFam(f)
		if len(f.Columns) > 0 {
			r.Families[wIdx] = f
			wIdx++
		}
	}
	r.Families = r.Families[:wIdx]

	// Sort by creation order.
	sort.Slice(r.Families, func(i, j int) bool {
		a := r.Families[i]
		b := r.Families[j]
		return fs[a.Name].order < fs[b.Name].order
	})
	return r
}

// Remove empty columns
func scrubFam(f *btpb.Family) *btpb.Family {
	wIdx := 0
	for _, c := range f.Columns {
		if len(c.Cells) > 0 {
			f.Columns[wIdx] = c
			wIdx++
		}
	}
	f.Columns = f.Columns[:wIdx]
	sort.Slice(f.Columns, func(i, j int) bool {
		return bytes.Compare(f.Columns[i].Qualifier, f.Columns[j].Qualifier) < 0
	})
	return f
}

func maxTimestamp(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func newTimestamp() int64 {
	ts := time.Now().UnixNano() / 1e3
	ts -= ts % 1000 // round to millisecond granularity
	return ts
}

func appendOrReplaceCell(cs []*btpb.Cell, newCell *btpb.Cell) []*btpb.Cell {
	replaced := false
	for i, cell := range cs {
		if cell.TimestampMicros == newCell.TimestampMicros {
			cs[i] = newCell
			replaced = true
			break
		}
	}
	if !replaced {
		cs = append(cs, newCell)
	}
	sort.Sort(byDescTS(cs))
	return cs
}

func (s *server) ReadModifyWriteRow(ctx context.Context, req *btpb.ReadModifyWriteRowRequest) (*btpb.ReadModifyWriteRowResponse, error) {
	s.mu.Lock()
	tbl, ok := s.tables[req.TableName]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.TableName)
	}

	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	r := tbl.getOrCreateRow(req.RowKey)
	resultRow := newRow(req.RowKey) // copy of updated cells

	// Assume all mutations apply to the most recent version of the cell.
	// TODO(dsymonds): Verify this assumption and document it in the proto.
	for _, rule := range req.Rules {
		if _, ok := tbl.families[rule.FamilyName]; !ok {
			return nil, fmt.Errorf("unknown family %q", rule.FamilyName)
		}

		fam := getOrCreateFamily(r, rule.FamilyName)
		col := getOrCreateColumn(fam, rule.ColumnQualifier)
		ts := newTimestamp()
		var newCell *btpb.Cell
		var prevVal []byte
		if len(col.Cells) > 0 {
			prevVal = col.Cells[0].Value

			// ts is the max of now or the prev cell's timestamp in case the
			// prev cell is in the future
			ts = maxTimestamp(ts, col.Cells[0].TimestampMicros)
		}

		switch rule := rule.Rule.(type) {
		default:
			return nil, fmt.Errorf("unknown RMW rule oneof %T", rule)
		case *btpb.ReadModifyWriteRule_AppendValue:
			newCell = &btpb.Cell{TimestampMicros: ts, Value: append(prevVal, rule.AppendValue...)}
		case *btpb.ReadModifyWriteRule_IncrementAmount:
			var v int64
			if prevVal != nil {
				if len(prevVal) != 8 {
					return nil, fmt.Errorf("increment on non-64-bit value")
				}
				v = int64(binary.BigEndian.Uint64(prevVal))
			}
			v += rule.IncrementAmount
			var val [8]byte
			binary.BigEndian.PutUint64(val[:], uint64(v))
			newCell = &btpb.Cell{TimestampMicros: ts, Value: val[:]}
		}

		// Store the new cell
		col.Cells = appendOrReplaceCell(col.Cells, newCell)

		// Store a copy for the result row
		resultFamily := getOrCreateFamily(resultRow, fam.Name)
		resultCol := getOrCreateColumn(resultFamily, col.Qualifier)
		resultCol.Cells = []*btpb.Cell{newCell}
	}

	tbl.rows.ReplaceOrInsert(scrubRow(r, tbl.families))
	return &btpb.ReadModifyWriteRowResponse{Row: scrubRow(resultRow, tbl.families)}, nil
}

func (s *server) SampleRowKeys(req *btpb.SampleRowKeysRequest, stream btpb.Bigtable_SampleRowKeysServer) error {
	s.mu.Lock()
	tbl, ok := s.tables[req.TableName]
	s.mu.Unlock()
	if !ok {
		return status.Errorf(codes.NotFound, "table %q not found", req.TableName)
	}

	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	// The return value of SampleRowKeys is very loosely defined. Return at least the
	// final row key in the table and choose other row keys randomly.
	var offset int64
	var err error
	var lastRow *btpb.Row
	tbl.rows.Ascend(func(r *btpb.Row) bool {
		if rand.Int31n(100) == 0 {
			resp := &btpb.SampleRowKeysResponse{
				RowKey:      r.Key,
				OffsetBytes: offset,
			}
			err = stream.Send(resp)
			if err != nil {
				return false
			}
			lastRow = nil
		} else {
			lastRow = r
		}
		offset += int64(rowsize(r))
		return true
	})
	if err == nil && lastRow != nil {
		resp := &btpb.SampleRowKeysResponse{
			RowKey:      lastRow.Key,
			OffsetBytes: offset - int64(rowsize(lastRow)),
		}
		err = stream.Send(resp)
	}
	return err
}

// needGC is invoked whenever the server needs gcloop running.
func (s *server) needGC() {
	s.mu.Lock()
	if s.gcc == nil {
		s.gcc = make(chan int)
		go s.gcloop(s.gcc)
	}
	s.mu.Unlock()
}

func (s *server) gcloop(done <-chan int) {
	const (
		minWait = 500  // ms
		maxWait = 1500 // ms
	)

	for {
		// Wait for a random time interval.
		d := time.Duration(minWait+rand.Intn(maxWait-minWait)) * time.Millisecond
		select {
		case <-time.After(d):
		case <-done:
			return // server has been closed
		}

		// Do a GC pass over all tables.
		var tables []*table
		s.mu.Lock()
		for _, tbl := range s.tables {
			tables = append(tables, tbl)
		}
		s.mu.Unlock()
		for _, tbl := range tables {
			tbl.gc()
		}
	}
}

type table struct {
	mu       sync.RWMutex
	counter  uint64                   // increment by 1 when a new family is created
	families map[string]*columnFamily // keyed by plain family name
	rows     Rows                     // indexed by row key
}

func newTable(ctr *btapb.CreateTableRequest, rf RowsFactory) *table {
	fams := make(map[string]*columnFamily)
	c := uint64(0)
	if ctr.Table != nil {
		for id, cf := range ctr.Table.ColumnFamilies {
			fams[id] = &columnFamily{
				name:   ctr.Parent + "/columnFamilies/" + id,
				order:  c,
				gcRule: cf.GcRule,
			}
			c++
		}
	}
	return &table{
		families: fams,
		counter:  c,
		rows:     rf.NewRows(ctr.Parent, ctr.TableId),
	}
}

func (t *table) validTimestamp(ts int64) bool {
	if ts < minValidMilliSeconds || ts > maxValidMilliSeconds {
		return false
	}

	// Assume millisecond granularity is required.
	return ts%1000 == 0
}

func (t *table) columnFamilies() map[string]*columnFamily {
	cp := make(map[string]*columnFamily)
	t.mu.RLock()
	for fam, cf := range t.families {
		cp[fam] = cf
	}
	t.mu.RUnlock()
	return cp
}

// Must hold table lock.
func (t *table) getOrCreateRow(key keyType) *btpb.Row {
	r := t.rows.Get(key)
	if r != nil {
		return r
	}
	return newRow(key)
}

func (t *table) gc() {
	// TODO(scottb): deal with GC better later; only dirty tables; less frequent.
	t.mu.Lock()
	defer t.mu.Unlock()

	// Gather GC rules we'll apply.
	rules := make(map[string]*btapb.GcRule) // keyed by "fam"
	for fam, cf := range t.families {
		if cf.gcRule != nil {
			rules[fam] = cf.gcRule
		}
	}
	if len(rules) == 0 {
		return
	}

	i := 0
	t.rows.Ascend(func(r *btpb.Row) bool {
		i++
		if i%100 == 0 {
			t.mu.Unlock()
			t.mu.Lock()
		}
		gcRow(r, rules)
		t.rows.ReplaceOrInsert(scrubRow(r, t.families))
		return true
	})
}

type byRowKey []*btpb.Row

func (b byRowKey) Len() int           { return len(b) }
func (b byRowKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byRowKey) Less(i, j int) bool { return bytes.Compare(b[i].Key, b[j].Key) < 0 }

func newRow(key keyType) *btpb.Row {
	return &btpb.Row{Key: key}
}

// copy returns a copy of the row.
// Cell values are aliased.
// r.mu should be held.
func copyRow(r *btpb.Row) *btpb.Row {
	nr := newRow(r.Key)
	for _, fam := range r.Families {
		f := &btpb.Family{
			Name: fam.Name,
		}
		for _, col := range fam.Columns {
			// Copy the cell slice, but not the []byte inside each cell.
			f.Columns = append(f.Columns, &btpb.Column{
				Qualifier: col.Qualifier,
				Cells:     append([]*btpb.Cell{}, col.Cells...),
			})
		}
		nr.Families = append(nr.Families, f)
	}
	return nr
}

// isEmpty returns true if a row doesn't contain any cell
func isEmpty(r *btpb.Row) bool {
	for _, fam := range r.Families {
		for _, cs := range fam.Columns {
			if len(cs.Cells) > 0 {
				return false
			}
		}
	}
	return true
}

func getFamily(r *btpb.Row, name string) *btpb.Family {
	for _, fam := range r.Families {
		if fam.Name == name {
			return fam
		}
	}
	return nil
}

func getOrCreateFamily(r *btpb.Row, name string) *btpb.Family {
	if fam := getFamily(r, name); fam != nil {
		return fam
	}
	fam := &btpb.Family{Name: name}
	r.Families = append(r.Families, fam)
	return fam
}

// gcRow applies the given GC rules to the row.
func gcRow(r *btpb.Row, rules map[string]*btapb.GcRule) {
	for _, fam := range r.Families {
		rule, ok := rules[fam.Name]
		if !ok {
			continue
		}
		for _, col := range fam.Columns {
			col.Cells = applyGC(col.Cells, rule)
		}
	}
}

// rowsize returns the total size of all cell values in the row.
func rowsize(r *btpb.Row) int {
	size := 0
	for _, fam := range r.Families {
		for _, col := range fam.Columns {
			for _, cell := range col.Cells {
				size += len(cell.Value)
			}
		}
	}
	return size
}

var gcTypeWarn sync.Once

// applyGC applies the given GC rule to the cells.
func applyGC(cells []*btpb.Cell, rule *btapb.GcRule) []*btpb.Cell {
	switch rule := rule.Rule.(type) {
	default:
		// TODO(dsymonds): Support GcRule_Intersection_
		gcTypeWarn.Do(func() {
			log.Printf("Unsupported GC rule type %T", rule)
		})
	case *btapb.GcRule_Union_:
		for _, sub := range rule.Union.Rules {
			cells = applyGC(cells, sub)
		}
		return cells
	case *btapb.GcRule_MaxAge:
		// Timestamps are in microseconds.
		cutoff := time.Now().UnixNano() / 1e3
		cutoff -= rule.MaxAge.Seconds * 1e6
		cutoff -= int64(rule.MaxAge.Nanos) / 1e3
		// The slice of cells in in descending timestamp order.
		// This sort.Search will return the index of the first cell whose timestamp is chronologically before the cutoff.
		si := sort.Search(len(cells), func(i int) bool { return cells[i].TimestampMicros < cutoff })
		if si < len(cells) {
			log.Printf("bttest: GC MaxAge(%v) deleted %d cells.", rule.MaxAge, len(cells)-si)
		}
		return cells[:si]
	case *btapb.GcRule_MaxNumVersions:
		n := int(rule.MaxNumVersions)
		if len(cells) > n {
			cells = cells[:n]
		}
		return cells
	}
	return cells
}

func getColumn(fam *btpb.Family, name []byte) *btpb.Column {
	for _, col := range fam.Columns {
		if bytes.Equal(col.Qualifier, name) {
			return col
		}
	}
	return nil
}

func getOrCreateColumn(fam *btpb.Family, name []byte) *btpb.Column {
	if col := getColumn(fam, name); col != nil {
		return col
	}
	col := &btpb.Column{Qualifier: name}
	fam.Columns = append(fam.Columns, col)
	return col
}

type byDescTS []*btpb.Cell

func (b byDescTS) Len() int           { return len(b) }
func (b byDescTS) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byDescTS) Less(i, j int) bool { return b[i].TimestampMicros > b[j].TimestampMicros }

type columnFamily struct {
	name   string
	order  uint64 // Creation order of column family
	gcRule *btapb.GcRule
}

func (c *columnFamily) proto() *btapb.ColumnFamily {
	return &btapb.ColumnFamily{
		GcRule: c.gcRule,
	}
}

func toColumnFamilies(families map[string]*columnFamily) map[string]*btapb.ColumnFamily {
	fs := make(map[string]*btapb.ColumnFamily)
	for k, v := range families {
		fs[k] = v.proto()
	}
	return fs
}
