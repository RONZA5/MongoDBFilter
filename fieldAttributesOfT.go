package mongoDBFilter

import (
	"errors"
	"reflect"

	. "github.com/ahmetb/go-linq/v3"
)

type fieldAttributesOfT struct {
	fieldName           string
	fieldType           string
	collectionName      string
	collectionBsonKey   string
	sortBsonKey         string
	bsonKey             string
	jsonKey             string
	isPrimaryCollection bool
	metaDataFilter      bool
	metaDataBsonKey     string
}

type fieldAttributesConfig struct {
	fieldAttributes []fieldAttributesOfT
}

func (data fieldAttributesConfig) getFieldConfigByJsonKey(jKey string) interface{} {
	return From(data.fieldAttributes).
		WhereT(func(d fieldAttributesOfT) bool { return d.jsonKey == jKey }).
		SelectT(func(d fieldAttributesOfT) fieldAttributesOfT { return d }).First()
}

func fieldAttributesConfigSetter[T any](primaryCollectionName string) ([]fieldAttributesOfT, error) {
	var modelType T
	modelTypeOf := reflect.TypeOf(modelType)

	var fieldAttributesOfType []fieldAttributesOfT
	for i := 0; i < modelTypeOf.NumField(); i++ {
		field := modelTypeOf.Field(i)

		if len(field.Tag.Get("json")) == 0 {
			return nil, errors.New("json not defined for field " + field.Name)
		}

		if len(field.Tag.Get("collectionName")) == 0 {
			return nil, errors.New("collectionName not defined for field " + field.Name)
		}

		if len(field.Tag.Get("collectionBsonKey")) == 0 {
			return nil, errors.New("collectionBsonKey not defined for field " + field.Name)
		}

		if len(field.Tag.Get("bson")) == 0 {
			return nil, errors.New("bson not defined for field " + field.Name)
		}

		var metaDataFilter bool
		var metaDataBsonKey string
		bsonSplit := toSliceWithNoSpace(field.Tag.Get("collectionBsonKey"), ".")
		if bsonSplit[0] == "metaData" {
			metaDataFilter = true
			metaDataBsonKey = bsonSplit[1]
		}

		var isPrimaryCollection bool
		if primaryCollectionName == field.Tag.Get("collectionName") {
			isPrimaryCollection = true
		}

		fieldAttributesOfType = append(fieldAttributesOfType, fieldAttributesOfT{
			fieldName:           field.Name,
			fieldType:           field.Type.String(),
			collectionName:      field.Tag.Get("collectionName"),
			collectionBsonKey:   field.Tag.Get("collectionBsonKey"),
			sortBsonKey:         field.Tag.Get("sortBsonKey"),
			bsonKey:             field.Tag.Get("bson"),
			jsonKey:             field.Tag.Get("json"),
			isPrimaryCollection: isPrimaryCollection,
			metaDataFilter:      metaDataFilter,
			metaDataBsonKey:     metaDataBsonKey})
	}
	return fieldAttributesOfType, nil
}
