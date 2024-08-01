package mongoDBFilterModel

type MongoFilterMapper struct {
	Key                 interface{}
	ConditionalOperator string
	FilterOperator      string
	Value               interface{}
}
