package mongoDBFilterModel

type MongoJoinFilterConfig struct {
	From                  string
	To                    string
	JoinCondition         string
	IsSecondaryCollection bool
	AvoidUnwind           bool
}
