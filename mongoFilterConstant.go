package mongoDBFilter

const (
	UNKNOWN_FIELD               string = "UNKNOWN_FIELD"
	MISSING_FIELD               string = "MISSING_FIELD"
	UNSUPPORTED_TYPE            string = "UNSUPPORTED_TYPE"
	UNSUPPORTED_FILTER_OPERATOR string = "UNSUPPORTED_FILTER_OPERATOR"
	INTERNAL_ERROR              string = "INTERNAL_ERROR"
	TIME_STAMP_NOT_FOUND        string = "TIME_STAMP_NOT_FOUND"

	EQ_OPERATOR    string = "$eq"
	LT_OPERATOR    string = "$lt"
	LTE_OPERATOR   string = "$lte"
	GT_OPERATOR    string = "$gt"
	GTE_OPERATOR   string = "$gte"
	REGEX_OPERATOR string = "$regex"
	IN_OPERATOR    string = "$in"

	OR_OPERATOR  string = "$or"
	AND_OPERATOR string = "$and"
)
