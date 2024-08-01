package mongoDBFilterModel

type MongoDataFilterMapper[A any] struct {
	TotalRecords []TotalRecordsMapper `bson:"totalRecords"`
	Data         A                    `bson:"data"`
}

type TotalRecordsMapper struct {
	Count int64 `bson:"count"`
}
