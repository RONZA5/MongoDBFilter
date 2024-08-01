package mongoDBFilter

import (
	"context"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func mongoFilterAggregator[A any](collection *mongo.Collection, pipelineStages mongo.Pipeline, functionalityType string) (A, error) {
	var result A
	curr, curr_err := collection.Aggregate(context.TODO(), pipelineStages, mongoFilterAggregateOption())
	if curr_err != nil {
		errorFormatForCurr := "Error on Currsor for FunctionalityType: " + functionalityType + " for Pipeline: " + fmt.Sprintf("%v", pipelineStages)
		fmt.Println(errorFormatForCurr)
		fmt.Println(curr_err)
		return result, curr_err
	}

	var mapp_err error
	mapp_err = curr.All(context.TODO(), &result)
	if mapp_err != nil && strings.Contains(mapp_err.Error(), "results argument must be a pointer to a slice, but was a pointer to struct") {
		curr.Next(context.TODO())
		mapp_err = curr.Decode(&result)
	}

	if mapp_err != nil && mapp_err.Error() != "EOF" {
		errorFormatForMapper := "Error on Mapper Data for FunctionalityType: " + functionalityType + " for Pipeline: " + fmt.Sprintf("%v", pipelineStages)
		fmt.Println(errorFormatForMapper)
		fmt.Println(mapp_err)
		return result, mapp_err
	}

	return result, nil
}

func mongoFilterAggregateOption() *options.AggregateOptions {
	var collation options.Collation
	collation.Locale = "en"
	collation.Strength = 2
	opt := options.Aggregate().SetAllowDiskUse(true)
	//.SetCollation(&collation)
	return opt
}
