package mongoDBFilter

import (
	"errors"
	"fmt"
	"strings"
	"time"

	mongoDBFilterModel "github.com/RONZA5/mongoDBFilter/mongoDBFilterModel"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func MongoDataFilter[T any](
	database *mongo.Database,
	primaryCollectionName string,
	filterPayload mongoDBFilterModel.FilterPayload,
	joinFilter []mongoDBFilterModel.MongoJoinFilterConfig,
	filter []mongoDBFilterModel.FilterParam,
	timeFilter mongoDBFilterModel.TimeFilter,
	filterOption *mongoDBFilterModel.MongoDataFilterOption) (int64, []T, error) {
	return applyPipelineStagesOnFilterCommon[T, []T](
		database,
		primaryCollectionName,
		joinFilter,
		filter,
		timeFilter,
		filterPayload.Sort,
		filterPayload.PageNumber,
		filterPayload.PageSize,
		filterOption,
		nil,
		true,
		false)
}

func ApplyPipelineStagesOnFilterPagination[T any, A any](
	database *mongo.Database,
	primaryCollectionName string,
	filterPayload mongoDBFilterModel.FilterPayload,
	joinFilter []mongoDBFilterModel.MongoJoinFilterConfig,
	filter []mongoDBFilterModel.FilterParam,
	timeFilter mongoDBFilterModel.TimeFilter,
	filterOption *mongoDBFilterModel.MongoDataFilterOption,
	remainingStages mongo.Pipeline) (int64, []A, error) {

	return applyPipelineStagesOnFilterCommon[T, []A](
		database,
		primaryCollectionName,
		joinFilter,
		filter,
		timeFilter,
		filterPayload.Sort,
		filterPayload.PageNumber,
		filterPayload.PageSize,
		filterOption,
		remainingStages,
		false,
		false)
}

func ApplyPipelineStagesOnFilter[T any, A any](
	database *mongo.Database,
	primaryCollectionName string,
	joinFilter []mongoDBFilterModel.MongoJoinFilterConfig,
	filter []mongoDBFilterModel.FilterParam,
	timeFilter mongoDBFilterModel.TimeFilter,
	filterOption *mongoDBFilterModel.MongoDataFilterOption,
	remainingStages mongo.Pipeline) (A, error) {

	_, data, error := applyPipelineStagesOnFilterCommon[T, A](
		database,
		primaryCollectionName,
		joinFilter,
		filter,
		timeFilter,
		"",
		0,
		0,
		filterOption,
		remainingStages,
		false,
		true)
	return data, error
}

// return TotalRecords and Corresponding Pagination Data
func applyPipelineStagesOnFilterCommon[T any, A any](
	database *mongo.Database,
	primaryCollectionName string, joinFilter []mongoDBFilterModel.MongoJoinFilterConfig,
	filter []mongoDBFilterModel.FilterParam, timeFilter mongoDBFilterModel.TimeFilter,
	sort string, pageNumber int, pageSize int, filterOption *mongoDBFilterModel.MongoDataFilterOption, remainingStages mongo.Pipeline, isCommonFilter bool, isChartFilter bool) (int64, A, error) {

	var avoidPagination bool

	var result A
	var totalRecords int64 = 0
	var dateBasedCollection bool

	if filterOption != nil {
		avoidPagination = filterOption.AvoidPagination
		dateBasedCollection = filterOption.DateBasedCollection
	}

	fieldSetter, filterMapper, fieldSetter_err := vaildateAndSetFilterFields[T](primaryCollectionName, filter, sort)
	if fieldSetter_err != nil {
		return 0, result, fieldSetter_err
	}

	var primaryCollectionNameForDataBase string
	if dateBasedCollection {
		currentDateFormat := timeFilter.TimeStamp.Format("02_01_2006")
		primaryCollectionNameForDataBase = primaryCollectionName + "_" + currentDateFormat
	} else {
		primaryCollectionNameForDataBase = primaryCollectionName
	}
	collection := database.Collection(primaryCollectionNameForDataBase)

	var pipelineStages mongo.Pipeline
	var pipelineStages_err error
	pipelineStages, pipelineStages_err = getPipelineStagesBasedOnFilter[T](primaryCollectionName, joinFilter, filterMapper, fieldSetter, timeFilter, database, collection, filterOption)
	if pipelineStages_err != nil {
		if strings.Contains(pipelineStages_err.Error(), TIME_STAMP_NOT_FOUND) {
			return 0, result, nil
		}
		return 0, result, pipelineStages_err
	}

	var commonDataPipelineStages []bson.D

	if !isChartFilter {
		//sort
		if len(sort) != 0 {
			sortSplit := toSliceWithNoSpace(sort, ",")
			var sortOrder int = 1
			if len(sortSplit) == 2 && sortSplit[1] == "desc" {
				sortOrder = -1
			}
			sortField := bson.D{{getSortCollectionField(sortSplit[0], primaryCollectionName, fieldSetter), sortOrder}}
			sortStage := bson.D{{"$sort", sortField}}
			commonDataPipelineStages = append(commonDataPipelineStages, sortStage)
		}

		if !avoidPagination {
			if pageNumber != 0 && pageSize != 0 {
				//pagination - skip
				skip := (pageNumber - 1) * pageSize
				skipStage := bson.D{{"$skip", skip}}
				commonDataPipelineStages = append(commonDataPipelineStages, skipStage)

				//pagination - limit
				limitStage := bson.D{{"$limit", pageSize}}
				commonDataPipelineStages = append(commonDataPipelineStages, limitStage)
			} else {
				return 0, result, errors.New(MISSING_FIELD + ": PageNumber/PageSize are not provided")
			}
		}

	}

	if isCommonFilter {
		//projectStage
		var projectStage bson.D = bson.D{{"$project", projectFieldAttributes(primaryCollectionName, fieldSetter)}}
		commonDataPipelineStages = append(commonDataPipelineStages, projectStage)
	} else {
		commonDataPipelineStages = append(commonDataPipelineStages, remainingStages...)
	}

	var collection_Curr_err error

	if isChartFilter {
		pipelineStages = append(pipelineStages, commonDataPipelineStages...)
		fmt.Println(pipelineStages)
		result, collection_Curr_err = mongoFilterAggregator[A](collection, pipelineStages, "FinalResult")
		if collection_Curr_err != nil {
			return 0, result, collection_Curr_err
		}
	} else {
		var facetTotalRecordsPipelineStages []bson.D
		facetTotalRecordsPipelineStages = append(facetTotalRecordsPipelineStages, bson.D{{"$count", "count"}})

		var facetpipelineStages bson.D = bson.D{{"$facet", bson.D{{"totalRecords", facetTotalRecordsPipelineStages}, {"data", commonDataPipelineStages}}}}
		pipelineStages = append(pipelineStages, facetpipelineStages)
		fmt.Println(pipelineStages)

		var resultData mongoDBFilterModel.MongoDataFilterMapper[A]
		resultData, collection_Curr_err = mongoFilterAggregator[mongoDBFilterModel.MongoDataFilterMapper[A]](collection, pipelineStages, "FinalResult")
		if collection_Curr_err != nil {
			return 0, result, collection_Curr_err
		}

		if len(resultData.TotalRecords) > 0 {
			totalRecords = resultData.TotalRecords[0].Count
			result = resultData.Data
		}
	}

	return totalRecords, result, nil
}

func vaildateAndSetFilterFields[T any](primaryCollectionName string, filter []mongoDBFilterModel.FilterParam, sort string) (fieldAttributesConfig, []mongoDBFilterModel.MongoFilterMapper, error) {

	var fieldSetter fieldAttributesConfig

	//fieldSetter
	fieldAttributesConfigSetter, fieldAttributesConfigSetter_err := fieldAttributesConfigSetter[T](primaryCollectionName)
	if fieldAttributesConfigSetter_err != nil {
		return fieldSetter, nil, fieldAttributesConfigSetter_err
	}
	fieldSetter = fieldAttributesConfig{fieldAttributes: fieldAttributesConfigSetter}

	//checkFilterFieldIsValid
	filterMapper, filterFieldIsValid_err := checkAndSetFilterFieldAndValueIsValid(filter, sort, fieldSetter)
	if filterFieldIsValid_err != nil {
		return fieldSetter, nil, filterFieldIsValid_err
	}

	return fieldSetter, filterMapper, nil
}

func getPipelineStagesBasedOnFilter[T any](primaryCollectionName string, joinFilter []mongoDBFilterModel.MongoJoinFilterConfig,
	filterMapper []mongoDBFilterModel.MongoFilterMapper, fieldSetter fieldAttributesConfig, timeFilter mongoDBFilterModel.TimeFilter, database *mongo.Database, collection *mongo.Collection, filterOption *mongoDBFilterModel.MongoDataFilterOption) (mongo.Pipeline, error) {

	var intervalFilter bool
	var lessThanEqualToTimeStampFilter bool
	var nonTimeSeriesCollection bool
	var appendInPrimaryAndFilterArrayAsBsonD bson.D
	var appendRemainingStagesBeforePagination bool
	var appendRemainingStages mongo.Pipeline
	var dateBasedCollection bool

	if filterOption != nil {
		intervalFilter = filterOption.IntervalFilter
		nonTimeSeriesCollection = filterOption.NonTimeSeriesCollection
		appendInPrimaryAndFilterArrayAsBsonD = filterOption.AppendInPrimaryAndFilterArrayAsBsonD
		lessThanEqualToTimeStampFilter = filterOption.LessThanEqualToTimeStampFilter
		appendRemainingStagesBeforePagination = filterOption.AppendRemainingStagesBeforePagination
		appendRemainingStages = filterOption.AppendRemainingStages
		dateBasedCollection = filterOption.DateBasedCollection
	}

	if len(joinFilter) > 0 && primaryCollectionName != joinFilter[0].From {
		return nil, errors.New(INTERNAL_ERROR + ": Please provide same name for primary and initial join collection")
	}

	var pipelineStages mongo.Pipeline
	var primaryAndFilterArray bson.A
	var primaryMatchStage bson.D
	var getNonMetaFilterIfNeeded fieldAttributesConfig = fieldSetter

	//metaData
	metaFilterBson, metaFilterBson_err := metaDataFilter(primaryCollectionName, filterMapper, fieldSetter)
	if metaFilterBson_err != nil {
		return nil, metaFilterBson_err
	} else if metaFilterBson != nil {
		primaryAndFilterArray = append(primaryAndFilterArray, metaFilterBson)
		getNonMetaFilterIfNeeded = getNonMetaFilter(fieldSetter)
	}

	primaryArrayFilter, primaryArrayFilter_err := arrayFilterBasedOnCollection(primaryCollectionName, false, filterMapper, getNonMetaFilterIfNeeded)
	if primaryArrayFilter_err != nil {
		return nil, primaryArrayFilter_err
	}

	var filterTimeStamp time.Time
	var commonTimeStamp time.Time

	if timeFilter.CurrentTime {
		commonTimeStamp = time.Now().UTC()
	} else {
		commonTimeStamp = timeFilter.TimeStamp
	}

	if !intervalFilter && !lessThanEqualToTimeStampFilter {
		var timeStampPipelineStages mongo.Pipeline
		var timeStampStageMapperSlice []mongoDBFilterModel.TimeStampStageMapper
		var timeStamp_collection_Curr_err error
		var timeStampCollection *mongo.Collection = collection
		fmt.Println(timeStampCollection)
		var timeStampAndFilterArray bson.A

		if !nonTimeSeriesCollection {
			if metaFilterBson != nil {
				timeStampAndFilterArray = append(timeStampAndFilterArray, metaFilterBson)
			} else {
				timeStampAndFilterArray = append(timeStampAndFilterArray, primaryArrayFilter...)
			}
		}

		timeStampAndFilterArray = append(timeStampAndFilterArray, bson.D{{"$lte", bson.A{"$timeStamp", commonTimeStamp}}})
		timeStampMatchStage := bson.D{{"$match", bson.D{{"$expr", bson.D{{"$and", timeStampAndFilterArray}}}}}}
		timeStampPipelineStages = append(timeStampPipelineStages, timeStampMatchStage)
		timeStampGroupStage := bson.D{{"$group", bson.D{{"_id", "$timeStamp"}}}}
		timeStampPipelineStages = append(timeStampPipelineStages, timeStampGroupStage)
		timeStampProjectStage := bson.D{{"$project", bson.D{{"timeStamp", "$_id"}}}}
		timeStampPipelineStages = append(timeStampPipelineStages, timeStampProjectStage)
		timeStampSortField := bson.D{{"timeStamp", -1}}
		timeStampSortStage := bson.D{{"$sort", timeStampSortField}}
		timeStampPipelineStages = append(timeStampPipelineStages, timeStampSortStage)
		timeStampLimitStage := bson.D{{"$limit", 1}}
		timeStampPipelineStages = append(timeStampPipelineStages, timeStampLimitStage)
		timeStampStageMapperSlice, timeStamp_collection_Curr_err = mongoFilterAggregator[[]mongoDBFilterModel.TimeStampStageMapper](timeStampCollection, timeStampPipelineStages, "TimeStamp")
		if timeStamp_collection_Curr_err != nil {
			return nil, timeStamp_collection_Curr_err
		}

		if len(timeStampStageMapperSlice) == 0 {
			var timseStampNotFound string = TIME_STAMP_NOT_FOUND + ": TimeStamp is not available"
			timeStampPipelineStagesString := timseStampNotFound + " for Pipeline: " + fmt.Sprintf("%v", timeStampPipelineStages)
			fmt.Println(timeStampPipelineStagesString)
			return nil, errors.New(TIME_STAMP_NOT_FOUND)
		} else {
			filterTimeStamp = timeStampStageMapperSlice[0].TimeStamp
		}
	}

	var appendRemainingCondition bson.A
	if appendRemainingStagesBeforePagination {
		appendRemainingCondition = primaryArrayFilter
	} else {
		primaryAndFilterArray = append(primaryAndFilterArray, primaryArrayFilter...)
	}

	if intervalFilter {
		primaryAndFilterArray = append(primaryAndFilterArray, bson.D{{"$gte", bson.A{"$" + filterOption.IntervalFilterData.FieldName, filterOption.IntervalFilterData.StartTime}}})
		primaryAndFilterArray = append(primaryAndFilterArray, bson.D{{"$lte", bson.A{"$" + filterOption.IntervalFilterData.FieldName, filterOption.IntervalFilterData.EndTime}}})
	} else if lessThanEqualToTimeStampFilter {
		primaryAndFilterArray = append(primaryAndFilterArray, bson.D{{"$lte", bson.A{"$timeStamp", commonTimeStamp}}})
	} else {
		primaryAndFilterArray = append(primaryAndFilterArray, bson.D{{"$eq", bson.A{"$timeStamp", filterTimeStamp}}})
	}

	if appendInPrimaryAndFilterArrayAsBsonD != nil {
		primaryAndFilterArray = append(primaryAndFilterArray, appendInPrimaryAndFilterArrayAsBsonD)
	}

	primaryMatchStage = bson.D{{"$match", bson.D{{"$expr", bson.D{{"$and", primaryAndFilterArray}}}}}}
	pipelineStages = append(pipelineStages, primaryMatchStage)

	for index, joinFilterData := range joinFilter {
		//checkJoinFilterIsValid
		joinFilterIsValid_err := checkJoinFilterIsValid(index, joinFilterData)
		if joinFilterIsValid_err != nil {
			return nil, joinFilterIsValid_err
		}

		var fromIsAPrimaryCollection bool = false
		if primaryCollectionName == joinFilterData.From {
			fromIsAPrimaryCollection = true
		}

		//lookup
		var secondaryCollectionNameForJoin string
		if dateBasedCollection {
			currentDateFormat := timeFilter.TimeStamp.Format("02_01_2006")
			secondaryCollectionNameForJoin = joinFilterData.To + "_" + currentDateFormat
		} else {
			secondaryCollectionNameForJoin = joinFilterData.To
		}
		var lookupFrom bson.E = bson.E{"from", secondaryCollectionNameForJoin}
		lookupLetBsonD, lookupLetBsonD_err := lookupLet(joinFilterData.From, fromIsAPrimaryCollection, joinFilterData.JoinCondition)
		if lookupLetBsonD_err != nil {
			return nil, lookupLetBsonD_err
		}
		var lookupLet bson.E = bson.E{"let", lookupLetBsonD}
		lookupPipelineArrayFilter, lookupPipelineArrayFilter_err := lookupPipeline(joinFilterData.JoinCondition)
		if lookupPipelineArrayFilter_err != nil {
			return nil, lookupPipelineArrayFilter_err
		}
		var secondaryAndFilterArray bson.A
		secondaryAndFilterArray, secondaryAndFilterArray_err := arrayFilterBasedOnCollection(joinFilterData.To, false, filterMapper, fieldSetter)
		if secondaryAndFilterArray_err != nil {
			return nil, secondaryAndFilterArray_err
		}
		if secondaryAndFilterArray != nil {
			lookupPipelineArrayFilter = append(lookupPipelineArrayFilter, secondaryAndFilterArray...)
		}
		if joinFilterData.IsSecondaryCollection {
			lookupPipelineArrayFilter = append(lookupPipelineArrayFilter, bson.D{{"$eq", bson.A{"$timeStamp", filterTimeStamp}}})
		}
		var lookupPipeline bson.E = bson.E{"pipeline", bson.A{bson.D{{"$match", bson.D{{"$expr", bson.D{{"$and", lookupPipelineArrayFilter}}}}}}}}
		var lookupAs bson.E = bson.E{"as", joinFilterData.To}
		var lookupPipelineStage bson.D = bson.D{{"$lookup", bson.D{lookupFrom, lookupLet, lookupPipeline, lookupAs}}}
		pipelineStages = append(pipelineStages, lookupPipelineStage)

		if !joinFilterData.AvoidUnwind {
			//unwind
			var unwindStage bson.D = bson.D{{"$unwind", bson.D{{"path", "$" + joinFilterData.To}}}}
			pipelineStages = append(pipelineStages, unwindStage)
		}
	}

	if appendRemainingStagesBeforePagination {
		pipelineStages = append(pipelineStages, appendRemainingStages...)
		primaryAndFilterArray = append(primaryAndFilterArray, primaryArrayFilter...)

		if len(appendRemainingCondition) > 0 {
			appendRemainingMatchCondition := bson.D{{"$match", bson.D{{"$expr", bson.D{{"$and", appendRemainingCondition}}}}}}
			pipelineStages = append(pipelineStages, appendRemainingMatchCondition)
		}

	}

	//conditionalFilterBasedOnOverAllCollection
	var conditionalAndArrayFilter bson.A
	var conditionalAndArrayFilter_err error
	conditionalAndArrayFilter, conditionalAndArrayFilter_err = conditionalArrayFilterBasedOnOverAllCollection(filterMapper, fieldSetter)
	if conditionalAndArrayFilter_err != nil {
		return nil, conditionalAndArrayFilter_err
	}
	if conditionalAndArrayFilter != nil {
		conditionalFilterStage := bson.D{{"$match", bson.D{{"$expr", bson.D{{"$and", conditionalAndArrayFilter}}}}}}
		pipelineStages = append(pipelineStages, conditionalFilterStage)
	}
	return pipelineStages, nil
}
