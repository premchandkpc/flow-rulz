package mongo

import "testing"

func TestNormalizeQuarantinePagination(t *testing.T) {
	for _, tc := range []struct{ limit, offset, wantLimit, wantOffset int }{
		{0, -2, 100, 0}, {-1, 3, 100, 3}, {50, 5, 50, 5}, {900, 1, 500, 1},
	} {
		gotLimit, gotOffset := normalizeQuarantinePagination(tc.limit, tc.offset)
		if gotLimit != tc.wantLimit || gotOffset != tc.wantOffset {
			t.Errorf("normalizeQuarantinePagination(%d, %d) = (%d, %d), want (%d, %d)",
				tc.limit, tc.offset, gotLimit, gotOffset, tc.wantLimit, tc.wantOffset)
		}
	}
}
