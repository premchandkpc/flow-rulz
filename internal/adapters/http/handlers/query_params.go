package handlers

import (
	"fmt"
	"net/http"
	"strconv"
)

// Pagination contains validated offset pagination parameters shared by HTTP handlers.
type Pagination struct {
	Limit  int
	Offset int
}

// parsePagination applies consistent bounds to list endpoints. A zero maxOffset
// means offset is only constrained to be non-negative.
func parsePagination(r *http.Request, defaultLimit, maxLimit, maxOffset int) (Pagination, error) {
	p := Pagination{Limit: defaultLimit}
	query := r.URL.Query()
	if value := query.Get(queryParamLimit); value != "" {
		limit, err := strconv.Atoi(value)
		if err != nil || limit < 1 || limit > maxLimit {
			return Pagination{}, fmt.Errorf("limit must be an integer between 1 and %d", maxLimit)
		}
		p.Limit = limit
	}
	if value := query.Get(queryParamOffset); value != "" {
		offset, err := strconv.Atoi(value)
		if err != nil || offset < 0 || (maxOffset > 0 && offset > maxOffset) {
			if maxOffset > 0 {
				return Pagination{}, fmt.Errorf("offset must be an integer between 0 and %d", maxOffset)
			}
			return Pagination{}, fmt.Errorf("offset must be a non-negative integer")
		}
		p.Offset = offset
	}
	return p, nil
}
