package mongoDBFilterModel

import (
	"time"
)

type FilterPayload struct {
	Sort       string `json:"sort"`
	PageNumber int    `json:"pageNumber"`
	PageSize   int    `json:"pageSize"`
}

type TimeFilter struct {
	CurrentTime bool      `json:"currentTime"`
	TimeStamp   time.Time `json:"timeStamp"`
}

type FilterParam struct {
	Key                 interface{} `json:"key"`
	ConditionalOperator string      `json:"conditionalOperator"`
	FilterOperator      string      `json:"filterOperator"`
	Value               interface{} `json:"value"`
}
