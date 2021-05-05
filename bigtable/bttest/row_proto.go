package bttest

import (
	"sort"

	"github.com/golang/protobuf/proto"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
)

func fromProto(buf []byte) *row {
	var p btpb.Row
	if err := proto.Unmarshal(buf, &p); err != nil {
		panic(err)
	}

	return &row{
		key:      string(p.Key),
		families: fromProtoFamilies(p.Families),
	}
}
func fromProtoFamilies(pfs []*btpb.Family) map[string]*family {
	ret := make(map[string]*family, len(pfs))
	for _, pf := range pfs {
		ret[pf.Name] = fromProtoFamily(pf)
	}
	return ret
}

func fromProtoFamily(pf *btpb.Family) *family {
	ret := &family{
		name:     pf.Name,
		colNames: nil,
		cells:    map[string][]cell{},
	}

	for _, col := range pf.Columns {
		qual := string(col.Qualifier)
		ret.colNames = append(ret.colNames, qual)

		for _, c := range col.Cells {
			ret.cells[qual] = append(ret.cells[qual], cell{
				ts:     c.TimestampMicros,
				value:  c.Value,
				labels: c.Labels,
			})
		}
	}
	// TODO: verify no dups in colNames.
	sort.Strings(ret.colNames)
	return ret
}

func toProto(r *row) []byte {
	pr := btpb.Row{
		Key:      []byte(r.key),
		Families: toProtoFamilies(r.families),
	}
	if buf, err := proto.Marshal(&pr); err != nil {
		panic(err)
	} else {
		return buf
	}
}

func toProtoFamilies(fs map[string]*family) []*btpb.Family {
	ret := make([]*btpb.Family, 0, len(fs))
	for _, f := range fs {
		ret = append(ret, toProtoFamily(f))
	}
	return ret
}

func toProtoFamily(f *family) *btpb.Family {
	var pcolls []*btpb.Column
	for colName, cells := range f.cells {
		pcells := make([]*btpb.Cell, len(cells))
		for i, c := range cells {
			pcells[i] = &btpb.Cell{
				TimestampMicros: c.ts,
				Value:           c.value,
				Labels:          c.labels,
			}
		}
		pcolls = append(pcolls, &btpb.Column{
			Qualifier: []byte(colName),
			Cells:     pcells,
		})
	}
	return &btpb.Family{
		Name:    f.name,
		Columns: pcolls,
	}
}
