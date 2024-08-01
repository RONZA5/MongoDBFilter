package mongoDBFilterModel

import "time"

type TimeStampStageMapper struct {
	TimeStamp time.Time `bson:"timeStamp"`
}
