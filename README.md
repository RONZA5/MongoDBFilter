**<h1>MongoDBFilter (GoLang)</h1>**

This module is purely for filtering time series collection to retrieving data based on given filter from api payload.<br/><br/>
It contain three main functions:
1) **MongoDataFilter** - It's a common filter used to do all filter operation.
2) **ApplyPipelineStagesOnFilterPagination** - It's a advanced version of MongoDataFilter, additionally append project pipeline, in the way we want the output.
3) **ApplyPipelineStagesOnFilter** - It's a advanced version of MongoDataFilter, additionally append project pipeline, in the way we want the output but pagination is not supported.

The following filter operation can be perform:
1) $eq<br/>
2) $lt<br/>
3) $lte<br/>
4) $gt<br/>
5) $gte<br/>
6) $regex<br/>
7) $in

The following conditional operation can be perform:
1) $or<br/>
2) $and

Supported Data Type: string, int, float, bool, primitive.ObjectID, time.Time

Additional Functionalities:
1) Avoid Pagination<br/>
2) Time-Interval Filter<br/>
3) Date Based Collection Filter

**<h3>Example</h3>**
<b>Note:</b> When CurrentTime set as true in TimeFilter then filter use current utc time, otherwise it use given TimeStamp.

Sample Time-Series Collection Data:
```
[
	{
		"timeStamp": {
			"$date": "2023-06-01T00:00:00.000Z"
		},
		"metaData": {
			"employeeId": 1000
		},
		"employeeName": "ronza5",
		"employeeAge": 24,
		"employeeDesignation": "devloper",
		"_id": {
			"$oid": "666ad5f11b2025f484aa8f7a"
		}
	},
	{
		"timeStamp": {
			"$date": "2023-06-01T00:00:00.000Z"
		},
		"metaData": {
			"employeeId": 1001
		},
		"employeeName": "gasron5",
		"employeeAge": 25,
		"employeeDesignation": "manager",
		"_id": {
			"$oid": "666ad5f11b2025f484aa8f7b"
		}
	},
	{
		"timeStamp": {
			"$date": "2024-06-01T00:00:00.000Z"
		},
		"metaData": {
			"employeeId": 1000
		},
		"employeeName": "ronza5",
		"employeeAge": 25,
		"employeeDesignation": "manager",
		"_id": {
			"$oid": "666ad5f11b2025f484aa8f7c"
		}
	},
	{
		"timeStamp": {
			"$date": "2024-06-01T00:00:00.000Z"
		},
		"metaData": {
			"employeeId": 1001
		},
		"employeeName": "gasron5",
		"employeeAge": 26,
		"employeeDesignation": "sr. manager",
		"_id": {
			"$oid": "666ad5f11b2025f484aa8f7d"
		}
	}
]
```
Filter From API:<br/>
```
"filter": [
        {
            "key": "name",
            "filterOperator": "$eq",
            "value": "ronza5"
        },
        {
            "key": "designation",
            "filterOperator": "$in",
            "value": ["devloper,"manager"]
        },
        {
            "key": ["name", "designation"],
            "conditionalOperator": "$or",
            "filterOperator": "$eq",
            "value": "developer"
        }
    ]
```

Code:

```
package example

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongoDBFilter "github.com/RONZA5/mongoDBFilter"
	mongoDBFilterModel "github.com/RONZA5/mongoDBFilter/mongoDBFilterModel"
)

type EmployeeFilter struct {
	//metaData and timeStamp Field
	Id        int    `json:"id" collectionName:"<Collection Name>" collectionBsonKey:"metaData.employeeId" bson:"id"`
	TimeStamp time.Time `json:"timeStamp" collectionName:"<Collection Name>" collectionBsonKey:"timeStamp" bson:"timeStamp"`

	//normal Fields
	Name        string `json:"name" collectionName:"<Collection Name>" collectionBsonKey:"employeeName" bson:"name"`
	Age         int    `json:"age" collectionName:"<Collection Name>" collectionBsonKey:"employeeAge" bson:"age"`
	Designation string `json:"designation" collectionName:"<Collection Name>" collectionBsonKey:"employeeDesignation" bson:"designation"`
}

type EmployeeListModel struct {
	Id          int    `json:"id" bson:"employeeId"`
	TimeStamp   time.Time `json:"timeStamp" bson:"timeStamp"`
	Name        string    `json:"name" bson:"employeeName"`
	Age         int       `json:"age" bson:"employeeAge"`
	Designation string    `json:"designation" bson:"employeeDesignation"`
}

func main() {
	Client, _ := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27017"))
	database := Client.Database("<DB name>")

	filterPayload := mongoDBFilterModel.FilterPayload{
		Sort:       "employeeName,asc",
		PageNumber: 1,
		PageSize:   10,
	}

	timeFilter := mongoDBFilterModel.TimeFilter{
		CurrentTime: true,
	}

	var filter []mongoDBFilterModel.FilterParam = [Filter From API]
	//additionally append age filter
	filter = append(filter, mongoDBFilterModel.FilterParam{
		Key:            "age",
		FilterOperator: mongoDBFilter.GTE_OPERATOR,
		Value:          25,
	})

	totalElements, data, _ := mongoDBFilter.ApplyPipelineStagesOnFilterPagination[EmployeeFilter, EmployeeListModel](
		database,
		"<Collection Name>",
		filterPayload,
		nil,
		filter,
		timeFilter,
		nil,
		EmployeeDetailsProjectPipeline()) //additionally append project pipeline, in the way we want the output
}

func EmployeeDetailsProjectPipeline() mongo.Pipeline {
	employeeDetailsProjectPipeline := mongo.Pipeline{bson.D{
		{
			Key: "$project",
			Value: bson.D{
				{Key: "employeeId", Value: "$metaData.employeeId"},
				{Key: "employeeName", Value: 1},
				{Key: "employeeAge", Value: 1},
				{Key: "employeeDesignation", Value: 1},
			}}}}
	return employeeDetailsProjectPipeline
}
```
