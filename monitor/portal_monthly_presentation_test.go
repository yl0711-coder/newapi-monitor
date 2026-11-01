package monitor

import (
	"encoding/xml"
	"testing"
)

func TestMonthlyWorkbookPresentationHierarchy(t *testing.T) {
	// Decode fonts and references without involving bill values.
	var fonts struct {
		Rows []struct {
			Size struct {
				Value int `xml:"val,attr"`
			} `xml:"sz"`
		} `xml:"fonts>font"`
	}
	if err := xml.Unmarshal([]byte(monthlyXLSXStyles), &fonts); err != nil {
		t.Fatal(err)
	}
	var xfs struct {
		Rows []struct {
			Font int `xml:"fontId,attr"`
			Fill int `xml:"fillId,attr"`
		} `xml:"cellXfs>xf"`
	}
	if err := xml.Unmarshal([]byte(monthlyXLSXStyles), &xfs); err != nil {
		t.Fatal(err)
	}
	if len(fonts.Rows) != 4 || len(xfs.Rows) != 17 {
		t.Fatal("unexpected style hierarchy")
	}
	for _, id := range []int{0, 12, 15, 16} {
		x := xfs.Rows[id]
		if x.Fill != 0 || fonts.Rows[x.Font].Size.Value != 11 {
			t.Fatal("body, label or balance label has alternating fill/oversized text", id)
		}
	}
	for _, id := range []int{5, 9, 10, 11, 13, 14} {
		x := xfs.Rows[id]
		if x.Fill != 0 || fonts.Rows[x.Font].Size.Value != 14 {
			t.Fatal("summary metrics must share restrained emphasis", id)
		}
	}
	if xfs.Rows[2].Fill != 3 || fonts.Rows[xfs.Rows[2].Font].Size.Value != 11 {
		t.Fatal("section heading must use the single light-fill hierarchy")
	}
}
