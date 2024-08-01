package mongoDBFilter

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	mongoDBFilterModel "github.com/RONZA5/mongoDBFilter/mongoDBFilterModel"

	linq "github.com/ahmetb/go-linq/v3"
	"go.mongodb.org/mongo-driver/bson"
)

func appendFilter(filter string, remaingFilter string) string {
	if len(filter) == 0 {
		return remaingFilter
	} else {
		filter = filter + " $and " + remaingFilter
		return filter
	}
}

func extractFilter(filter string, toExtract []string) (string, error) {
	var extractFilter []string
	filterSplit := toSliceWithNoSpace(filter, "$and")
	for _, te := range toExtract {
		var foundFilter string
		for _, fs := range filterSplit {
			if strings.Contains(fs, " $eq ") {
				eqKeyValueSplit := toSliceWithNoSpace(fs, "$eq")
				if eqKeyValueSplit[0] == te {
					foundFilter = fs
				}
			} else if strings.Contains(fs, " $lte ") {
				ltEqKeyValueSplit := toSliceWithNoSpace(fs, "$lte")
				if ltEqKeyValueSplit[0] == te {
					foundFilter = fs
				}
			} else if strings.Contains(fs, " $lt ") {
				ltKeyValueSplit := toSliceWithNoSpace(fs, "$lt")
				if ltKeyValueSplit[0] == te {
					foundFilter = fs
				}
			} else if strings.Contains(fs, " $gte ") {
				gtEqKeyValueSplit := toSliceWithNoSpace(fs, "$gte")
				if gtEqKeyValueSplit[0] == te {
					foundFilter = fs
				}
			} else if strings.Contains(fs, " $gt ") {
				gtKeyValueSplit := toSliceWithNoSpace(fs, "$gt")
				if gtKeyValueSplit[0] == te {
					foundFilter = fs
				}
			} else if strings.Contains(fs, " $regex ") {
				regexKeyValueSplit := toSliceWithNoSpace(fs, "$regex")
				if regexKeyValueSplit[0] == te {
					foundFilter = fs
				}
			} else if strings.Contains(fs, " $in ") {
				inKeyValueSplit := toSliceWithNoSpace(fs, "$in")
				if inKeyValueSplit[0] == te {
					foundFilter = fs
				}
			}
		}
		if len(foundFilter) == 0 {
			return "", errors.New("ExtractFilter for this " + te + " field is not found")
		} else {
			extractFilter = append(extractFilter, foundFilter)
		}
	}
	return strings.Join(extractFilter[:], " $and "), nil
}

func userDefinedVariableNameFormatter(data string) string {
	if data[0] == '_' {
		return data[1:]
	}

	eqKeyValueSplit := toSliceWithNoSpace(data, "$as")
	if len(eqKeyValueSplit) == 2 {
		return eqKeyValueSplit[1]
	}
	return data
}

func beginningOfDay(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location())
}

func collectionNameTimeStampFormater(collectionName string, time time.Time) string {
	filterTimeYear, filterTimeMonth, filterTimeDay := time.Date()
	return fmt.Sprintf("%v_%v_%v_%v", collectionName, filterTimeYear, filterTimeMonth, filterTimeDay)
}

func toSliceWithNoSpace(data string, splitOperator string) []string {
	splitString := strings.Split(data, splitOperator)
	for i, value := range splitString {
		splitString[i] = strings.TrimSpace(value)
	}
	return splitString
}

func toSliceWithNoSpaceForGivenStringArray(data []string) []string {
	var noSpaceStringArray []string
	for _, value := range data {
		noSpaceStringArray = append(noSpaceStringArray, strings.TrimSpace(value))
	}
	return noSpaceStringArray
}

func customInvalidTimeFormat() time.Time {
	return time.Date(9, time.September, 9, 0, 0, 0, 0, time.Now().Location())
}

func isInt(d string) bool {
	if _, err := strconv.ParseInt(d, 10, 64); err == nil {
		return true
	}
	return false
}

func isFloat(d string) bool {
	if _, err := strconv.ParseFloat(d, 64); err == nil {
		return true
	}
	return false
}

func isBool(d string) bool {
	if _, err := strconv.ParseBool(d); err == nil {
		return true
	}
	return false
}

func isSlice(v interface{}) bool {
	return reflect.TypeOf(v).Kind() == reflect.Slice || reflect.TypeOf(v).Kind() == reflect.Array
}

func isTime(timeStr string) bool {
	if _, err := time.Parse(time.RFC3339, timeStr); err == nil {
		return true
	}
	return false
}

func eqBsonD(key string, value string) bson.D {
	if isInt(value) {
		eqInt, _ := strconv.ParseInt(value, 10, 64)
		return bson.D{{key, bson.D{{"$eq", eqInt}}}}
	} else if isFloat(value) {
		eqFloat, _ := strconv.ParseFloat(value, 64)
		return bson.D{{key, bson.D{{"$eq", eqFloat}}}}
	} else {
		return bson.D{{key, bson.D{{"$eq", value}}}}
	}
}

func eqExprBsonD(key string, value interface{}, fieldType string) (bson.D, error) {
	var eqbsonD bson.D
	if fieldType == "int" || fieldType == "int32" || fieldType == "int64" || fieldType == "float32" || fieldType == "float64" || fieldType == "*float64" {
		if val, ok := value.(float64); ok {
			eqbsonD = bson.D{{"$eq", bson.A{key, val}}}
		}
	} else if fieldType == "bool" {
		if val, ok := value.(bool); ok {
			eqbsonD = bson.D{{"$eq", bson.A{key, val}}}
		}
	} else if fieldType == "primitive.ObjectID" {
		if val, ok := value.(string); ok {
			eqbsonD = bson.D{{"$eq", bson.A{key, bson.D{{"$toObjectId", val}}}}}
		}
	} else if fieldType == "time.Time" {
		if val, ok := value.(string); ok {
			eqbsonD = bson.D{{"$eq", bson.A{key, bson.D{{"$toDate", val}}}}}
		}
	} else {
		if val, ok := value.(string); ok {
			eqbsonD = bson.D{{"$eq", bson.A{key, val}}}
		}
	}

	if eqbsonD == nil {
		return nil, errors.New(UNSUPPORTED_TYPE + ": Unsupported type for $eq operator")
	}

	return eqbsonD, nil
}

func ltBsonD(key string, value string) (bson.D, error) {
	if isInt(value) {
		ltInt, _ := strconv.ParseInt(value, 10, 64)
		return bson.D{{key, bson.D{{"$lt", ltInt}}}}, nil
	} else if isFloat(value) {
		ltFloat, _ := strconv.ParseFloat(value, 64)
		return bson.D{{key, bson.D{{"$lt", ltFloat}}}}, nil
	}
	return nil, errors.New(UNSUPPORTED_TYPE + ": Unsupported type for $lt operator")
}

func ltExprBsonD(key string, value interface{}, fieldType string) (bson.D, error) {
	if fieldType == "int" || fieldType == "int32" || fieldType == "int64" || fieldType == "float32" || fieldType == "float64" || fieldType == "*float64" {
		if val, ok := value.(float64); ok {
			return bson.D{{"$lt", bson.A{key, val}}}, nil
		}
	} else if fieldType == "time.Time" {
		if val, ok := value.(string); ok {
			return bson.D{{"$lt", bson.A{key, bson.D{{"$toDate", val}}}}}, nil
		}
	}
	return nil, errors.New(UNSUPPORTED_TYPE + ": Unsupported type for $lt operator")
}

func ltEqBsonD(key string, value string) (bson.D, error) {
	if isInt(value) {
		ltEqInt, _ := strconv.ParseInt(value, 10, 64)
		return bson.D{{key, bson.D{{"$lte", ltEqInt}}}}, nil
	} else if isFloat(value) {
		ltEqFloat, _ := strconv.ParseFloat(value, 64)
		return bson.D{{key, bson.D{{"$lte", ltEqFloat}}}}, nil
	}
	return nil, errors.New(UNSUPPORTED_TYPE + ": Unsupported type for $lte operator")
}

func ltEqExprBsonD(key string, value interface{}, fieldType string) (bson.D, error) {
	if fieldType == "int" || fieldType == "int32" || fieldType == "int64" || fieldType == "float32" || fieldType == "float64" || fieldType == "*float64" {
		if val, ok := value.(float64); ok {
			return bson.D{{"$lte", bson.A{key, val}}}, nil
		}
	} else if fieldType == "time.Time" {
		if val, ok := value.(string); ok {
			return bson.D{{"$lte", bson.A{key, bson.D{{"$toDate", val}}}}}, nil
		}
	}
	return nil, errors.New(UNSUPPORTED_TYPE + ": Unsupported type for $lte operator")
}

func gtBsonD(key string, value string) (bson.D, error) {
	if isInt(value) {
		gtInt, _ := strconv.ParseInt(value, 10, 64)
		return bson.D{{key, bson.D{{"$gt", gtInt}}}}, nil
	} else if isFloat(value) {
		gtFloat, _ := strconv.ParseFloat(value, 64)
		return bson.D{{key, bson.D{{"$gt", gtFloat}}}}, nil
	}
	return nil, errors.New(UNSUPPORTED_TYPE + ": Unsupported type for $gt operator")
}
func gtExprBsonD(key string, value interface{}, fieldType string) (bson.D, error) {
	if fieldType == "int" || fieldType == "int32" || fieldType == "int64" || fieldType == "float32" || fieldType == "float64" || fieldType == "*float64" {
		if val, ok := value.(float64); ok {
			return bson.D{{"$gt", bson.A{key, val}}}, nil
		}
	} else if fieldType == "time.Time" {
		if val, ok := value.(string); ok {
			return bson.D{{"$gt", bson.A{key, bson.D{{"$toDate", val}}}}}, nil
		}
	}
	return nil, errors.New(UNSUPPORTED_TYPE + ": Unsupported type for $gt operator")
}

func gtEqBsonD(key string, value string) (bson.D, error) {
	if isInt(value) {
		gtEqInt, _ := strconv.ParseInt(value, 10, 64)
		return bson.D{{key, bson.D{{"$gte", gtEqInt}}}}, nil
	} else if isFloat(value) {
		gtEqFloat, _ := strconv.ParseFloat(value, 64)
		return bson.D{{key, bson.D{{"$gte", gtEqFloat}}}}, nil
	}
	return nil, errors.New(UNSUPPORTED_TYPE + ": Unsupported type for $gte operator")
}

func gtEqExprBsonD(key string, value interface{}, fieldType string) (bson.D, error) {
	if fieldType == "int" || fieldType == "int32" || fieldType == "int64" || fieldType == "float32" || fieldType == "float64" || fieldType == "*float64" {
		if val, ok := value.(float64); ok {
			return bson.D{{"$gte", bson.A{key, val}}}, nil
		}
	} else if fieldType == "time.Time" {
		if val, ok := value.(string); ok {
			return bson.D{{"$gte", bson.A{key, bson.D{{"$toDate", val}}}}}, nil
		}
	}
	return nil, errors.New(UNSUPPORTED_TYPE + ": Unsupported type for $gte operator")
}

func regexBsonD(key string, value string) bson.D {
	regexp := "^" + value + ".*"
	return bson.D{{key, bson.D{{"$regex", regexp}}}}
}

func regexExprBsonD(key string, value interface{}) (bson.D, error) {
	var regexBson bson.D
	if val, ok := value.(string); ok {
		regexBson = bson.D{{"$regexMatch", bson.D{{"input", key}, {"regex", val}, {"options", "i"}}}}
	}

	if regexBson == nil {
		return nil, errors.New(UNSUPPORTED_TYPE + ": Unsupported type for $regex operator")
	}
	return regexBson, nil
}

func inBsonD(key string, value string) bson.D {
	inCommaValueSplit := strings.Split(value, ",")
	var inArray bson.A
	for _, inValue := range inCommaValueSplit {
		if isInt(inValue) {
			inInt, _ := strconv.ParseInt(inValue, 10, 64)
			inArray = append(inArray, inInt)
		} else if isFloat(inValue) {
			inFloat, _ := strconv.ParseFloat(inValue, 64)
			inArray = append(inArray, inFloat)
		} else {
			inArray = append(inArray, inValue)
		}
	}
	return bson.D{{key, bson.D{{"$in", inArray}}}}
}

func inExprBsonD(key string, value interface{}, fieldType string) (bson.D, error) {
	var inArray bson.A

	var out []interface{}
	rv := reflect.ValueOf(value)
	for i := 0; i < rv.Len(); i++ {
		out = append(out, rv.Index(i).Interface())
	}
	for _, ins := range out {
		if fieldType == "int" || fieldType == "int32" || fieldType == "int64" || fieldType == "float32" || fieldType == "float64" || fieldType == "*float64" {
			if val, ok := ins.(float64); ok {
				inArray = append(inArray, val)
			}
		} else if fieldType == "string" {
			if val, ok := ins.(string); ok {
				inArray = append(inArray, val)
			}
		}
	}

	if inArray == nil {
		return nil, errors.New(UNSUPPORTED_TYPE + ": Unsupported type for $in operator")
	}

	return bson.D{{"$in", bson.A{key, inArray}}}, nil
}

func boolEqBsonD(key string, value string) (bson.D, error) {
	if isBool(value) {
		boolEqValue, _ := strconv.ParseBool(value)
		return bson.D{{key, bson.D{{"$eq", boolEqValue}}}}, nil
	}
	return nil, errors.New("Unsupported type for $booleq operator")
}

func boolEqExprBsonD(key string, value string) (bson.D, error) {
	if isBool(value) {
		boolEqValue, _ := strconv.ParseBool(value)
		return bson.D{{"$eq", bson.A{key, boolEqValue}}}, nil
	}
	return nil, errors.New("Unsupported type for $booleq operator")
}

func strEqBsonD(key string, value string) bson.D {
	return bson.D{{key, bson.D{{"$eq", value}}}}
}

func strEqExprBsonD(key string, value string) bson.D {
	return bson.D{{"$eq", bson.A{key, value}}}
}

func strInBsonD(key string, value string) bson.D {
	strInCommaValueSplit := strings.Split(value, ",")
	var strInArray bson.A
	for _, strInValue := range strInCommaValueSplit {
		strInArray = append(strInArray, strInValue)
	}
	return bson.D{{key, bson.D{{"$in", strInArray}}}}
}

func strInExprBsonD(key string, value string) bson.D {
	strInCommaValueSplit := strings.Split(value, ",")
	var strInArray bson.A
	for _, strInValue := range strInCommaValueSplit {
		strInArray = append(strInArray, strInValue)
	}
	return bson.D{{"$in", bson.A{key, strInArray}}}
}

func metaDataFilter(collectionName string, filterMapper []mongoDBFilterModel.MongoFilterMapper, fieldSetter fieldAttributesConfig) (bson.D, error) {
	var metaFilter []fieldAttributesOfT
	linq.From(fieldSetter.fieldAttributes).
		WhereT(func(d fieldAttributesOfT) bool { return d.collectionName == collectionName && d.metaDataFilter }).
		SelectT(func(d fieldAttributesOfT) fieldAttributesOfT { return d }).
		OrderByT(func(d fieldAttributesOfT) string { return d.metaDataBsonKey }).ToSlice(&metaFilter)
	// fmt.Println(metaFilter)
	var MetaDataFilterCount int

	var metaFilterBsonD bson.D
	for _, mf := range metaFilter {
		for _, fm := range filterMapper {
			if key, ok := fm.Key.(string); ok {
				if mf.jsonKey == key {
					MetaDataFilterCount += 1
					dataType_err := checkFieldValueDataType(key, fm.Value, mf.fieldType)
					if dataType_err != nil {
						return nil, errors.New(UNSUPPORTED_TYPE + ": MetaField '" + key + "' value should be in " + mf.fieldType)
					}
					if mf.fieldType == "int" || mf.fieldType == "int32" || mf.fieldType == "int64" ||
						mf.fieldType == "float32" || mf.fieldType == "float64" {
						if val, ok := fm.Value.(float64); ok {
							metaFilterBsonD = append(metaFilterBsonD, bson.E{mf.metaDataBsonKey, val})
						}
					} else if mf.fieldType == "string" {
						if val, ok := fm.Value.(string); ok {
							metaFilterBsonD = append(metaFilterBsonD, bson.E{mf.metaDataBsonKey, val})
						}
					} else {
						return nil, errors.New(UNSUPPORTED_TYPE + ": Unsupported type for MetaField " + key)
					}
				}
			}
		}
	}
	if metaFilterBsonD == nil || MetaDataFilterCount < len(metaFilter) {
		return nil, nil
	}
	return bson.D{{"$eq", bson.A{"$metaData", metaFilterBsonD}}}, nil
}

func getNonMetaFilter(fieldSetter fieldAttributesConfig) fieldAttributesConfig {
	var nonNonMetaFilter []fieldAttributesOfT
	linq.From(fieldSetter.fieldAttributes).
		WhereT(func(d fieldAttributesOfT) bool { return !d.metaDataFilter }).
		SelectT(func(d fieldAttributesOfT) fieldAttributesOfT { return d }).ToSlice(&nonNonMetaFilter)
	return fieldAttributesConfig{fieldAttributes: nonNonMetaFilter}
}

func appendCollectionNameWithBsonKeyIfNeeded(isPrimaryCollection bool, appendCollectionName bool, collectionName string, bsonKey string) string {
	if appendCollectionName && !isPrimaryCollection {
		return "$" + collectionName + "." + bsonKey
	} else {
		return "$" + bsonKey
	}
}

func arrayFilterBasedOnCollection(collectionName string, appendCollectionName bool, filterMapper []mongoDBFilterModel.MongoFilterMapper, fieldSetter fieldAttributesConfig) (bson.A, error) {
	var filterArray bson.A

	for _, fm := range filterMapper {

		if !checkIsValidConditionalOperator(fm.ConditionalOperator) {
			if key, ok := fm.Key.(string); ok {
				var fieldFilterConfigInterface interface{} = linq.From(fieldSetter.fieldAttributes).
					WhereT(func(d fieldAttributesOfT) bool { return d.jsonKey == key && d.collectionName == collectionName }).
					SelectT(func(d fieldAttributesOfT) fieldAttributesOfT { return d }).First()

				if fieldFilterConfigInterface == nil {
					continue
				}
				ffc := fieldFilterConfigInterface.(fieldAttributesOfT)
				var getCollectionBsonKey string = appendCollectionNameWithBsonKeyIfNeeded(ffc.isPrimaryCollection, appendCollectionName, ffc.collectionName, ffc.collectionBsonKey)
				switch fm.FilterOperator {
				case EQ_OPERATOR:
					var eqBson bson.D
					var eqBson_err error
					eqBson, eqBson_err = eqExprBsonD(getCollectionBsonKey, fm.Value, ffc.fieldType)
					if eqBson_err != nil {
						return nil, eqBson_err
					}
					filterArray = append(filterArray, eqBson)
				case LTE_OPERATOR:
					var ltEqBson bson.D
					var ltEqBson_err error
					ltEqBson, ltEqBson_err = ltEqExprBsonD(getCollectionBsonKey, fm.Value, ffc.fieldType)
					if ltEqBson_err != nil {
						return nil, ltEqBson_err
					}
					filterArray = append(filterArray, ltEqBson)
				case LT_OPERATOR:
					var ltBson bson.D
					var ltBson_err error
					ltBson, ltBson_err = ltExprBsonD(getCollectionBsonKey, fm.Value, ffc.fieldType)
					if ltBson_err != nil {
						return nil, ltBson_err
					}
					filterArray = append(filterArray, ltBson)
				case GTE_OPERATOR:
					var gtEqBson bson.D
					var gtEqBson_err error
					gtEqBson, gtEqBson_err = gtEqExprBsonD(getCollectionBsonKey, fm.Value, ffc.fieldType)
					if gtEqBson_err != nil {
						return nil, gtEqBson_err
					}
					filterArray = append(filterArray, gtEqBson)
				case GT_OPERATOR:
					var gtBson bson.D
					var gtBson_err error
					gtBson, gtBson_err = gtExprBsonD(getCollectionBsonKey, fm.Value, ffc.fieldType)
					if gtBson_err != nil {
						return nil, gtBson_err
					}
					filterArray = append(filterArray, gtBson)
				case REGEX_OPERATOR:
					var regexBson bson.D
					var regexBson_err error
					regexBson, regexBson_err = regexExprBsonD(getCollectionBsonKey, fm.Value)
					if regexBson_err != nil {
						return nil, regexBson_err
					}
					filterArray = append(filterArray, regexBson)
				case IN_OPERATOR:
					var inBson bson.D
					var inBson_err error
					inBson, inBson_err = inExprBsonD(getCollectionBsonKey, fm.Value, ffc.fieldType)
					if inBson_err != nil {
						return nil, inBson_err
					}
					filterArray = append(filterArray, inBson)
				default:
					return nil, errors.New(UNSUPPORTED_FILTER_OPERATOR + ": Unsupported filter operator")
				}
			} else {
				return nil, errors.New(UNSUPPORTED_TYPE + ": Key should be String type for " + fm.FilterOperator + " operator")
			}
		}
	}

	if len(filterArray) == 0 {
		return nil, nil
	}

	return filterArray, nil
}

func conditionalArrayFilterBasedOnOverAllCollection(filterMapper []mongoDBFilterModel.MongoFilterMapper, fieldSetter fieldAttributesConfig) (bson.A, error) {
	var conditionalAndArrayFilter bson.A

	for _, cf := range filterMapper {
		if checkIsValidConditionalOperator(cf.ConditionalOperator) {
			var conditionalArrayFilter bson.A

			var outKey []interface{}
			rv := reflect.ValueOf(cf.Key)
			for i := 0; i < rv.Len(); i++ {
				outKey = append(outKey, rv.Index(i).Interface())
			}

			for _, inKey := range outKey {
				if key, ok := inKey.(string); ok {
					var conditionalIndividualFilter bson.A
					var conditionalIndividualFilter_err error
					var fieldFilterConfig fieldAttributesOfT = linq.From(fieldSetter.fieldAttributes).
						WhereT(func(d fieldAttributesOfT) bool { return d.jsonKey == key }).
						SelectT(func(d fieldAttributesOfT) fieldAttributesOfT { return d }).First().(fieldAttributesOfT)

					if len(cf.FilterOperator) == 0 {
						var fieldType string = fieldFilterConfig.fieldType
						var getCollectionBsonKey string = appendCollectionNameWithBsonKeyIfNeeded(
							fieldFilterConfig.isPrimaryCollection, true, fieldFilterConfig.collectionName, fieldFilterConfig.collectionBsonKey)

						if strVal, ok := cf.Value.(string); ok {
							if fieldType == "int" || fieldType == "int32" || fieldType == "int64" {
								if val, err := strconv.ParseInt(strVal, 10, 64); err == nil {
									conditionalIndividualFilter = append(conditionalIndividualFilter, bson.D{{"$eq", bson.A{getCollectionBsonKey, val}}})
								}
							} else if fieldType == "float32" || fieldType == "float64" {
								if val, err := strconv.ParseFloat(strVal, 64); err == nil {
									conditionalIndividualFilter = append(conditionalIndividualFilter, bson.D{{"$eq", bson.A{getCollectionBsonKey, val}}})
								}
							} else if fieldType == "string" {
								conditionalIndividualFilter = append(conditionalIndividualFilter, bson.D{{"$regexMatch", bson.D{{"input", getCollectionBsonKey}, {"regex", strVal}, {"options", "i"}}}})
							}
						}
					} else {
						var conditionalFilterMapper []mongoDBFilterModel.MongoFilterMapper
						conditionalFilterMapper = append(conditionalFilterMapper, mongoDBFilterModel.MongoFilterMapper{
							Key:            key,
							FilterOperator: cf.FilterOperator,
							Value:          cf.Value})
						conditionalIndividualFilter, conditionalIndividualFilter_err = arrayFilterBasedOnCollection(fieldFilterConfig.collectionName, true, conditionalFilterMapper, fieldSetter)

						if conditionalIndividualFilter_err != nil {
							return nil, conditionalIndividualFilter_err
						}
					}

					if len(conditionalIndividualFilter) > 0 {
						conditionalArrayFilter = append(conditionalArrayFilter, conditionalIndividualFilter...)
					}
				}
			}

			if len(conditionalArrayFilter) > 0 {
				conditionalAndArrayFilter = append(conditionalAndArrayFilter, bson.D{{cf.ConditionalOperator, conditionalArrayFilter}})
			}
		}
	}

	if len(conditionalAndArrayFilter) == 0 {
		return nil, nil
	}
	return conditionalAndArrayFilter, nil
}

func isValidFilterBasedOnCollection(collectionName string, filterJsonKey string, fieldSetter fieldAttributesConfig) (bool, fieldAttributesOfT) {
	var filterFieldSort []fieldAttributesOfT
	var filterField fieldAttributesOfT

	linq.From(fieldSetter.fieldAttributes).
		WhereT(func(d fieldAttributesOfT) bool { return d.collectionName == collectionName }).
		SelectT(func(d fieldAttributesOfT) fieldAttributesOfT { return d }).ToSlice(&filterFieldSort)

	for _, field := range filterFieldSort {
		filterField = field
		if field.collectionName == collectionName && field.jsonKey == filterJsonKey {
			return true, filterField
		}
	}
	return false, filterField
}

func lookupLet(collectionName string, primaryCollection bool, joinCondition string) (bson.D, error) {
	var lookupLet bson.D

	joinSplit := toSliceWithNoSpace(joinCondition, "$and")
	for _, js := range joinSplit {

		if strings.Contains(js, " $eq ") {
			eqKeyValueSplit := toSliceWithNoSpace(js, "$eq")
			asEqKeyValueSplit := toSliceWithNoSpace(eqKeyValueSplit[0], "$as")
			letVariable := asEqKeyValueSplit[0]
			if !primaryCollection {
				letVariable = collectionName + "." + letVariable
			}
			lookupLet = append(lookupLet, bson.E{userDefinedVariableNameFormatter(eqKeyValueSplit[0]), "$" + letVariable})
		} else {
			return nil, errors.New(UNSUPPORTED_FILTER_OPERATOR + ": Unsupported filter operator on lookupLet")
		}
	}

	return lookupLet, nil
}

func lookupPipeline(joinCondition string) (bson.A, error) {
	var andFilterArray bson.A

	joinSplit := toSliceWithNoSpace(joinCondition, "$and")
	for _, js := range joinSplit {

		if strings.Contains(js, " $eq ") {
			eqKeyValueSplit := toSliceWithNoSpace(js, "$eq")
			andFilterArray = append(andFilterArray, bson.D{{"$eq", bson.A{"$$" + userDefinedVariableNameFormatter(eqKeyValueSplit[0]), "$" + eqKeyValueSplit[1]}}})
		} else {
			return nil, errors.New(UNSUPPORTED_FILTER_OPERATOR + ": Unsupported filter operator on lookupPipeline")
		}
	}

	return andFilterArray, nil
}

func getSortCollectionField(jsonkey string, primaryCollectionName string, fieldSetter fieldAttributesConfig) string {
	for _, fs := range fieldSetter.fieldAttributes {
		if jsonkey == fs.jsonKey {
			var sortKey string
			var collectionBsonKey string = fs.collectionBsonKey
			var sortBsonKey string = fs.sortBsonKey
			if len(sortBsonKey) > 0 {
				sortKey = sortBsonKey
			} else {
				sortKey = collectionBsonKey
			}
			if primaryCollectionName != fs.collectionName {
				sortKey = fs.collectionName + "." + sortKey
			}
			return sortKey
		}
	}
	return ""
}

func projectFieldAttributes(primaryCollectionName string, fieldSetter fieldAttributesConfig) bson.D {
	var projectFieldAttributes bson.D

	for _, fs := range fieldSetter.fieldAttributes {
		var collectionBsonKey string = fs.collectionBsonKey
		var bsonkey string = fs.bsonKey
		if primaryCollectionName != fs.collectionName {
			collectionBsonKey = fs.collectionName + "." + collectionBsonKey
		}
		collectionBsonKey = "$" + collectionBsonKey
		projectFieldAttributes = append(projectFieldAttributes, bson.E{bsonkey, collectionBsonKey})
	}

	return projectFieldAttributes
}

func checkAndSetFilterFieldAndValueIsValid(filter []mongoDBFilterModel.FilterParam, sort string, fieldSetter fieldAttributesConfig) ([]mongoDBFilterModel.MongoFilterMapper, error) {
	var flterMapper []mongoDBFilterModel.MongoFilterMapper

	for _, fs := range filter {
		var dataTypeError error

		if len(fs.ConditionalOperator) > 0 && !checkIsValidConditionalOperator(fs.ConditionalOperator) {
			return nil, errors.New(UNSUPPORTED_FILTER_OPERATOR + " Unsupported Conditional Operator " + fs.ConditionalOperator)
		}

		if (!checkIsValidConditionalOperator(fs.ConditionalOperator) || (checkIsValidConditionalOperator(fs.ConditionalOperator) && len(fs.FilterOperator) > 0)) &&
			!checkIsValidFilterOperator(fs.FilterOperator) {
			return nil, errors.New(UNSUPPORTED_FILTER_OPERATOR + " Unsupported Filter Operator " + fs.FilterOperator)
		}

		if checkIsValidConditionalOperator(fs.ConditionalOperator) {
			if fs.Key != nil && isSlice(fs.Key) {
				var outKey []interface{}
				rv := reflect.ValueOf(fs.Key)
				for i := 0; i < rv.Len(); i++ {
					outKey = append(outKey, rv.Index(i).Interface())
				}
				if len(outKey) == 0 {
					return nil, errors.New(UNSUPPORTED_TYPE + ": Key should not be empty for " + fs.ConditionalOperator + " Conditional operator")
				}

				for _, inKey := range outKey {
					if key, ok := inKey.(string); ok {
						if len(fs.FilterOperator) == 0 {
							var fieldFilterConfigInterface interface{} = fieldSetter.getFieldConfigByJsonKey(key)
							if fieldFilterConfigInterface == nil {
								return nil, errors.New(UNKNOWN_FIELD + ": " + key + " filter field is not available")
							}
						} else {
							dataTypeError = checkDataTypeOfField(key, fs.Value, fs.FilterOperator, fieldSetter)
							if dataTypeError != nil {
								break
							}
						}
					} else {
						return nil, errors.New(UNSUPPORTED_TYPE + ": Key should be String Array for " + fs.ConditionalOperator + " Conditional operator")
					}
				}

				if len(fs.FilterOperator) == 0 {
					if _, ok := fs.Value.(string); !ok {
						return nil, errors.New(UNSUPPORTED_TYPE + ": Value should be String for " + fs.ConditionalOperator + " Conditional operator if filter operator is not provided")
					}
				}
			} else {
				return nil, errors.New(UNSUPPORTED_TYPE + ": Key should be String Array for " + fs.ConditionalOperator + " Conditional operator")
			}
		} else {
			if key, ok := fs.Key.(string); ok {
				dataTypeError = checkDataTypeOfField(key, fs.Value, fs.FilterOperator, fieldSetter)
			} else {
				return nil, errors.New(UNSUPPORTED_TYPE + ": Key should be String type for " + fs.FilterOperator + " operator")
			}
		}

		if dataTypeError != nil {
			return nil, dataTypeError
		}

		flterMapper = append(flterMapper, mongoDBFilterModel.MongoFilterMapper{
			Key:                 fs.Key,
			Value:               fs.Value,
			FilterOperator:      fs.FilterOperator,
			ConditionalOperator: fs.ConditionalOperator})
	}

	if len(sort) != 0 {
		sortSplit := toSliceWithNoSpace(sort, ",")
		var sortFilterConfigInterface interface{} = fieldSetter.getFieldConfigByJsonKey(sortSplit[0])

		if sortFilterConfigInterface == nil {
			return nil, errors.New(UNKNOWN_FIELD + ": " + sortSplit[0] + " sort field is not available")
		}
	}

	return flterMapper, nil
}

func checkDataTypeOfField(key string, value interface{}, filterOperator string, fieldSetter fieldAttributesConfig) error {
	var dataTypeError error

	var fieldFilterConfigInterface interface{} = fieldSetter.getFieldConfigByJsonKey(key)
	if fieldFilterConfigInterface == nil {
		return errors.New(UNKNOWN_FIELD + ": " + key + " filter field is not available")
	}
	fieldConfig := fieldFilterConfigInterface.(fieldAttributesOfT)

	if filterOperator == IN_OPERATOR {
		if value != nil && isSlice(value) {
			var out []interface{}
			rv := reflect.ValueOf(value)
			for i := 0; i < rv.Len(); i++ {
				out = append(out, rv.Index(i).Interface())
			}
			if len(out) == 0 {
				return errors.New(UNSUPPORTED_TYPE + ": Value should not be empty for field " + key)
			}
			for _, ins := range out {
				dataTypeError = checkFieldValueDataType(key, ins, fieldConfig.fieldType)
				if dataTypeError != nil {
					break
				}
			}
		} else {
			return errors.New(UNSUPPORTED_TYPE + ": Value should be an Array for $in operator")
		}
	} else {
		dataTypeError = checkFieldValueDataType(key, value, fieldConfig.fieldType)
	}

	if dataTypeError != nil {
		return dataTypeError
	}
	return nil
}

func checkIsValidFilterOperator(searchOperator string) bool {
	if searchOperator == EQ_OPERATOR ||
		searchOperator == LT_OPERATOR || searchOperator == LTE_OPERATOR ||
		searchOperator == GT_OPERATOR || searchOperator == GTE_OPERATOR ||
		searchOperator == REGEX_OPERATOR || searchOperator == IN_OPERATOR {
		return true
	}
	return false
}
func checkIsValidConditionalOperator(conditionalOperator string) bool {
	if conditionalOperator == OR_OPERATOR || conditionalOperator == AND_OPERATOR {
		return true
	}
	return false
}

func checkFieldValueDataType(field string, value interface{}, fieldType string) error {
	if fieldType == "int" || fieldType == "int32" || fieldType == "int64" || fieldType == "float32" || fieldType == "float64" || fieldType == "*float64" {
		if _, ok := value.(float64); !ok {
			return errors.New(UNSUPPORTED_TYPE + ": Gievn value is invaild datatype for field '" + field + "', it should be in int/float")
		}
	} else if fieldType == "bool" {
		if _, ok := value.(bool); !ok {
			return errors.New(UNSUPPORTED_TYPE + ": Gievn value is invaild datatype for field '" + field + "', it should be in bool")
		}
	} else if fieldType == "primitive.ObjectID" || fieldType == "string" || fieldType == "time.Time" {
		if val, ok := value.(string); ok {
			if len(strings.TrimSpace(val)) == 0 {
				return errors.New(UNSUPPORTED_TYPE + ": Value should not be empty or contain whitespace for field '" + field + "'")
			}
			if fieldType == "time.Time" && !isTime(val) {
				return errors.New(UNSUPPORTED_TYPE + ": Gievn value is invaild datatype for field '" + field + "', it should be in (TZ) time format")
			}
		} else {
			return errors.New(UNSUPPORTED_TYPE + ": Gievn value is invaild datatype for field '" + field + "', it should be in string")
		}
	} else {
		return errors.New(UNSUPPORTED_TYPE + ": Gievn value is unsupported datatype for field '" + field)
	}
	return nil
}

func checkJoinFilterIsValid(index int, joinFilter mongoDBFilterModel.MongoJoinFilterConfig) error {

	if len(joinFilter.From) == 0 {
		return errors.New(fmt.Sprintf(INTERNAL_ERROR+": From field is not available at index %v", index))
	}

	if len(joinFilter.To) == 0 {
		return errors.New(fmt.Sprintf(INTERNAL_ERROR+": To field is not available at index %v", index))
	}

	if len(joinFilter.JoinCondition) == 0 {
		return errors.New(fmt.Sprintf(INTERNAL_ERROR+": JoinCondition field is not available at index %v", index))
	}

	return nil
}
