package mongoDBFilterModel

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoDataFilterOption struct {
	AvoidPagination                       bool
	NonTimeSeriesCollection               bool
	IntervalFilter                        bool
	IntervalFilterData                    IntervalFilterData
	AppendRemainingStagesBeforePagination bool
	AppendRemainingStages                 mongo.Pipeline
	LessThanEqualToTimeStampFilter        bool
	AppendInPrimaryAndFilterArrayAsBsonD  bson.D
	DateBasedCollection                   bool
}

type IntervalFilterData struct {
	FieldName string
	StartTime time.Time
	EndTime   time.Time
}
