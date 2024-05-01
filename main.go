package main

import (
	"encoding/json"
	"fmt"
	"log"
	"main/types"
	"main/utils"
	"net/http"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

// TODO: Check invalid type
var svc *dynamodb.DynamoDB

func init() {
	// Initialize AWS session once
	//For development (local machine setup)
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Endpoint: aws.String("http://localhost:8000"), // Endpoint URL for DynamoDB Local
		},
	}))
	//For production
	// sess := session.Must(session.NewSessionWithOptions(session.Options{
	// 	SharedConfigState: session.SharedConfigEnable,
	// }))

	// Create DynamoDB client
	svc = dynamodb.New(sess)
}

func createTable(tableName string, partitionKey string, sortKey ...string) {
	var input *dynamodb.CreateTableInput

	if len(sortKey) > 0 {
		input = &dynamodb.CreateTableInput{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String(partitionKey),
					AttributeType: aws.String("S"),
				},
				{
					AttributeName: aws.String(sortKey[0]),
					AttributeType: aws.String("S"),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String(partitionKey),
					KeyType:       aws.String("HASH"),
				},
				{
					AttributeName: aws.String(sortKey[0]),
					KeyType:       aws.String("RANGE"),
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(10),
				WriteCapacityUnits: aws.Int64(10),
			},
			TableName: utils.PrepareTableName(tableName),
		}
	} else {
		input = &dynamodb.CreateTableInput{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String(partitionKey),
					AttributeType: aws.String("S"),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String(partitionKey),
					KeyType:       aws.String("HASH"),
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(10),
				WriteCapacityUnits: aws.Int64(10),
			},
			TableName: utils.PrepareTableName(tableName),
		}
	}

	_, err := svc.CreateTable(input)
	if err != nil {
		log.Fatalf("Got error calling CreateTable: %s", err)
	}

	fmt.Println("Created the table", tableName)
}
func createTableEndpoint(w http.ResponseWriter, r *http.Request) {
	createTable("movies", "title")
}

func deleteTable(tableName string) {
	_, err := svc.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: utils.PrepareTableName(tableName),
	})
	if err != nil {
		log.Fatalf("Error deleting table: %v", err)
	}

	fmt.Println("Deleted the table", tableName)
}
func deleteTableEndpoint(w http.ResponseWriter, r *http.Request) {
	deleteTable("movies")
}

func batchWriteItems(tableName string, data []*map[string]*dynamodb.AttributeValue) {
	batchSize := 25
	batches := utils.SplitDataIntoBatches(data, batchSize)
	for _, batch := range batches {
		putRequests := make([]*dynamodb.WriteRequest, len(batch))
		for index, singleData := range batch {
			putRequests[index] = &dynamodb.WriteRequest{
				PutRequest: &dynamodb.PutRequest{
					Item: *singleData,
				},
			}
		}

		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				*utils.PrepareTableName(tableName): putRequests,
			},
		}

		result, err := svc.BatchWriteItem(input)
		if err != nil {
			panic("failed to batch write items, " + err.Error())
		}

		fmt.Println("Batch write operation result:", result)
	}
}

type apiResponse struct {
	Results []struct {
		Name struct {
			Title string `json:"title"`
			First string `json:"first"`
			Last  string `json:"last"`
		} `json:"name"`
		Email string `json:"email"`
	} `json:"results"`
}

func batchWriteItemsEndpoint(w http.ResponseWriter, r *http.Request) {
	var data []*map[string]*dynamodb.AttributeValue
	for i := 0; i < 50; i++ {
		resp, err := http.Get("https://randomuser.me/api/")
		if err != nil {
			fmt.Println("Error fetching data from API:", err)
			return
		}
		defer resp.Body.Close()

		var apiResponse apiResponse
		err = json.NewDecoder(resp.Body).Decode(&apiResponse)
		if err != nil {
			fmt.Println("Error decoding API response:", err)
			return
		}

		// Generate random data item
		title := apiResponse.Results[0].Name.First + " " + apiResponse.Results[0].Name.Last
		titleWithUUID := fmt.Sprintf("%s-%s", title, uuid.New())
		email := apiResponse.Results[0].Email

		// Transform data item into DynamoDB attribute values
		dynamoDBItem := map[string]*dynamodb.AttributeValue{
			"title": {S: aws.String(titleWithUUID)},
			"email": {S: aws.String(email)},
		}

		// Append data item to the data slice
		data = append(data, &dynamoDBItem)
	}

	batchWriteItems("movies", data)
}

func createItem(tableName string, tableItem interface{}) {
	av, err := dynamodbattribute.MarshalMap(tableItem)
	if err != nil {
		log.Fatalf("Got error marshalling new movie item: %s", err)
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: utils.PrepareTableName(tableName),
	}
	_, err = svc.PutItem(input)
	if err != nil {
		log.Fatalf("Got error calling PutItem: %s", err)
	}

	fmt.Println("Added successfully")
}
func createItemEndpoint(w http.ResponseWriter, r *http.Request) {
	var Item = struct {
		Year   int     `json:"year"`
		Title  string  `json:"title"`
		Plot   string  `json:"plot"`
		Rating float64 `json:"rating"`
	}{
		Year:   2015,
		Title:  "The Big New Movie",
		Plot:   "Nothing happens at all.",
		Rating: 0.0,
	}

	createItem("movies", Item)
}

func getItem(tableName string, varToSetDataTo interface{}, conditionKey map[string]*dynamodb.AttributeValue) {
	movieName := "The Big New Movie"

	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: utils.PrepareTableName(tableName),
		Key:       conditionKey,
	})
	if err != nil {
		log.Fatalf("Got error calling GetItem: %s", err)
	}

	if result.Item == nil {
		msg := "Could not find '" + movieName + "'"
		log.Fatalf(msg)
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &varToSetDataTo)
	if err != nil {
		log.Fatalf("Failed to unmarshal Record, %v", err)
	}
}
func getItemEndpoint(w http.ResponseWriter, r *http.Request) {
	var Item struct {
		Year   int
		Title  string
		Plot   string
		Rating float64
	}
	getItemKey := types.DBKeys{
		PartitionKey: types.DBkeyType{
			Name:  "title",
			Value: "The Big New Movie",
		},
	}

	getItem("movies", &Item, utils.PrepareConditionKey(getItemKey))
	data, err := json.Marshal(Item)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(data))
}

func queryItem(tableName string, out *[]interface{}, expression expression.Expression) {
	input := &dynamodb.QueryInput{
		TableName:                 utils.PrepareTableName(tableName),
		KeyConditionExpression:    expression.KeyCondition(),
		ProjectionExpression:      expression.Projection(),
		FilterExpression:          expression.Filter(),
		ExpressionAttributeNames:  expression.Names(),
		ExpressionAttributeValues: expression.Values(),
	}

	result, err := svc.Query(input)
	if err != nil {
		log.Fatal(err)
	}

	var items []interface{}
	for _, item := range result.Items {
		var d interface{}
		if err := dynamodbattribute.UnmarshalMap(item, &d); err != nil {
			log.Fatal(err)
		}
		// Append to items slice
		items = append(items, d)
	}

	*out = items
}
func queryItemEndpoint(w http.ResponseWriter, r *http.Request) {
	var ItemArr []interface{}

	queryKey := types.DBKeys{
		PartitionKey: types.DBkeyType{
			Name:  "title",
			Value: "Ilaria Andre-2bc79336-cac8-4aff-be43-f84730bed326",
		},
	}

	exprBuilder := expression.NewBuilder()
	exprBuilder = exprBuilder.WithKeyCondition(expression.KeyEqual(expression.Key(queryKey.PartitionKey.Name), expression.Value(queryKey.PartitionKey.Value)))
	expr, err := exprBuilder.Build()
	if err != nil {
		log.Fatal(err)
	}

	queryItem("movies", &ItemArr, expr)
	fmt.Println(ItemArr)
}

// varToSetDataTo is a pointer.
// TODO: Assumed cant send varToSetData with custom typed data.
func getAllItemsUsingScan(tableName string, varToSetDataTo *[]interface{}) {
	var lastEvaluatedKey map[string]*dynamodb.AttributeValue
	var data []interface{}

	sliceValue := reflect.ValueOf(varToSetDataTo)
	if sliceValue.Kind() != reflect.Ptr || sliceValue.Elem().Kind() != reflect.Slice {
		log.Fatal("varToSetDataTo must be a pointer to a slice")
	}

	for {
		// Prepare input parameters for the Scan operation
		input := &dynamodb.ScanInput{
			TableName:         utils.PrepareTableName(tableName), // Change to your table name
			ExclusiveStartKey: lastEvaluatedKey,
		}

		// Execute the Scan operation
		result, err := svc.Scan(input)
		if err != nil {
			log.Fatalf("Error scanning table: %v", err)
		}

		// Unmarshal the items returned by the Scan operation
		var batch []interface{}
		if err := dynamodbattribute.UnmarshalListOfMaps(result.Items, &batch); err != nil {
			log.Fatalf("Error unmarshaling items: %v", err)
		}

		// Add the items to the movies slice
		data = append(data, batch...)
		// sliceValue.Elem().Set(reflect.AppendSlice(sliceValue.Elem(), reflect.ValueOf(batch)))

		// Check if there are more items to retrieve
		lastEvaluatedKey = result.LastEvaluatedKey
		if lastEvaluatedKey == nil {
			break
		}
	}

	*varToSetDataTo = data
}
func scanEndpoint(w http.ResponseWriter, r *http.Request) {
	var item []interface{}
	getAllItemsUsingScan("movies", &item)
	data, err := json.Marshal(item)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(data))

}

// func update() {

// }
// func updateItemEndPoint(w http.ResponseWriter, r *http.Request) {

// }
func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file:", err)
		return
	}

	http.HandleFunc("/createTable", createTableEndpoint)
	http.HandleFunc("/deleteTable", deleteTableEndpoint)
	http.HandleFunc("/createItem", createItemEndpoint)
	http.HandleFunc("/batchCreate", batchWriteItemsEndpoint)
	http.HandleFunc("/getItem", getItemEndpoint)
	http.HandleFunc("/scan", scanEndpoint)
	http.HandleFunc("/query", queryItemEndpoint)
	// http.HandleFunc("/createTable", DynamoCreateTable)
	// http.HandleFunc("/createTableItem", DynamoCreateTableItem)
	// http.HandleFunc("/listTables", DynamoListTables)
	// http.HandleFunc("/listItems", DynamoListTableItems)
	// http.HandleFunc("/updateItem", DynamoUpdateItem)
	// http.HandleFunc("/deleteItem", DynamoDeleteItem)
	// http.HandleFunc("/getItemWithScan", DynamoGetItemWithScan)
	// http.HandleFunc("/getAllItems", DynamoGetAllItems)

	fmt.Println("Server listening on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
