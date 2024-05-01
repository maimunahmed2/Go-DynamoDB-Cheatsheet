package utils

import (
	"main/types"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func PrepareTableName(tableName string, isNotAwsString ...bool) *string {
	if len(isNotAwsString) > 0 && isNotAwsString[0] {
		name := "Dev.Inkspire.." + tableName
		return &name
	}

	return aws.String("Dev.Inkspire.." + tableName)
}

func PrepareConditionKey(dbKeys types.DBKeys) map[string]*dynamodb.AttributeValue {
	if dbKeys.SortKey != nil {
		return map[string]*dynamodb.AttributeValue{
			dbKeys.PartitionKey.Name: {
				S: aws.String(dbKeys.PartitionKey.Value),
			},
			dbKeys.SortKey.Name: {
				S: aws.String(dbKeys.SortKey.Value),
			},
		}
	}

	return map[string]*dynamodb.AttributeValue{
		dbKeys.PartitionKey.Name: {
			S: aws.String(dbKeys.PartitionKey.Value),
		},
	}
}

// func PrepareDBKeyCondition(dbKeys types.DBKeys) (map[string]*dynamodb.AttributeValue) {
// 	var keyCondition expression.KeyConditionBuilder
// 	keyCondition = expression.KeyEqual(expression.Key(dbKeys.PartitionKey.Name), expression.Value(dbKeys.PartitionKey.Value))
// 	if dbKeys.SortKey != nil {
// 		keyCondition = keyCondition.And(expression.KeyEqual(expression.Key(dbKeys.SortKey.Name), expression.Value(dbKeys.SortKey.Value)))
// 	}

// 	return keyCondition
// }

func SplitDataIntoBatches(data []*map[string]*dynamodb.AttributeValue, batchSize int) [][]*map[string]*dynamodb.AttributeValue {
	var batches [][]*map[string]*dynamodb.AttributeValue
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}
		batches = append(batches, data[i:end])
	}
	return batches
}
