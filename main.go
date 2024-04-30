package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/joho/godotenv"
)

//TODO: Check invalid type
var svc *dynamodb.DynamoDB

func init() {
    // Initialize AWS session once
    sess := session.Must(session.NewSessionWithOptions(session.Options{
        SharedConfigState: session.SharedConfigEnable,
    }))

    // Create DynamoDB client
    svc = dynamodb.New(sess)
}

func DynamoCreateTable(w http.ResponseWriter, r *http.Request) {
    fmt.Println("Started")
    // Create table Movies
    tableName := "Movies"

    // Define the input for CreateTable operation
    input := &dynamodb.CreateTableInput{
        AttributeDefinitions: []*dynamodb.AttributeDefinition{
            {
                AttributeName: aws.String("Year"),
                AttributeType: aws.String("N"),
            },
            {
                AttributeName: aws.String("Title"),
                AttributeType: aws.String("S"),
            },
        },
        KeySchema: []*dynamodb.KeySchemaElement{
            {
                AttributeName: aws.String("Year"),
                KeyType:       aws.String("HASH"),
            },
            {
                AttributeName: aws.String("Title"),
                KeyType:       aws.String("RANGE"),
            },
        },
        ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
            ReadCapacityUnits:  aws.Int64(10),
            WriteCapacityUnits: aws.Int64(10),
        },
        TableName: aws.String(tableName),
    }

    _, err := svc.CreateTable(input)
    if err != nil {
        log.Fatalf("Got error calling CreateTable: %s", err)
    }

    fmt.Println("Created the table", tableName)
}

type Item struct {
    Year   int
    Title  string
    Plot   string
    Rating float64
}

func DynamoCreateTableItem(w http.ResponseWriter, r *http.Request) {
    raw, err := os.ReadFile("./movie_data.json")
    if err != nil {
        log.Fatalf("Got error reading file: %s", err)
    }

    var items []Item
    json.Unmarshal(raw, &items)

    tableName := "Movies"

    for _, item := range items {
        av, err := dynamodbattribute.MarshalMap(item)
        if err != nil {
            log.Fatalf("Got error marshalling map: %s", err)
        }

        // Create item in table Movies
        input := &dynamodb.PutItemInput{
            Item:      av,
            TableName: aws.String(tableName),
        }

        _, err = svc.PutItem(input)
        if err != nil {
            log.Fatalf("Got error calling PutItem: %s", err)
        }

        year := strconv.Itoa(item.Year)
        fmt.Println("Successfully added '" + item.Title + "' (" + year + ") to table " + tableName)
    }

    data := map[string]interface{}{
        "success": 1,
        "message": "Successfully added table items.",
		"items": items,
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(data)
}

func DynamoListTables(w http.ResponseWriter, r *http.Request) {
	input := &dynamodb.ListTablesInput{}

	result, err := svc.ListTables(input)
	if err != nil {
		return
	}
	respJson := map[string][]*string {
		"Tables": result.TableNames,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(respJson)
}

// Uses DynamoDB GetItem
func DynamoListTableItems(w http.ResponseWriter, r *http.Request) {
	tableName := "Movies"
	movieName := "The Big New Movies"
	movieYear := "2015"

	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Year": {
				N: aws.String(movieYear),
			},
			"Title": {
				S: aws.String(movieName),
			},
		},
	})
	if err != nil {
		log.Fatalf("Got error calling GetItem: %s", err)
	}
	
	item := Item{}

	if result.Item == nil {
		msg := "Could not find '" + movieName + "'"
		fmt.Println(msg)
		return
	}
	

	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	if err != nil {
	    panic(fmt.Sprintf("Failed to unmarshal Record, %v", err))
	}

	fmt.Println("Found item:")
	fmt.Println("Year:  ", item.Year)
	fmt.Println("Title: ", item.Title)
	fmt.Println("Plot:  ", item.Plot)
	fmt.Println("Rating:", item.Rating)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(item)
}


func DynamoUpdateItem(w http.ResponseWriter, r *http.Request) {
	tableName := "Movies"
	movieName := "The Big New Movie"
	movieYear := "2015"
	movieRating := "5.0"

	input := &dynamodb.UpdateItemInput{
	    ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
	        ":r": {
	            N: aws.String(movieRating),
	        },
	    },
	    TableName: aws.String(tableName),
	    Key: map[string]*dynamodb.AttributeValue{
	        "Year": {
	            N: aws.String(movieYear),
	        },
	        "Title": {
	            S: aws.String(movieName),
	        },
	    },
	    ReturnValues:     aws.String("ALL_NEW"),
	    UpdateExpression: aws.String("set Rating = :r"),
	}

	res, err := svc.UpdateItem(input)
	if err != nil {
	    log.Fatalf("Got error calling UpdateItem: %s", err)
	}
	
	fmt.Println("Successfully updated '" + movieName + "' (" + movieYear + ") rating to " + movieRating)

	fmt.Println(res.Attributes)
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func DynamoDeleteItem(w http.ResponseWriter, r *http.Request) {
	tableName := "Movies"
	movieName := "The Big New Movie"
	movieYear := "2015"
	
	input := &dynamodb.DeleteItemInput{
	    Key: map[string]*dynamodb.AttributeValue{
	        "Year": {
	            N: aws.String(movieYear),
	        },
	        "Title": {
	            S: aws.String(movieName),
	        },
	    },
	    TableName: aws.String(tableName),
	}
	
	_, err := svc.DeleteItem(input)
	if err != nil {
	    log.Fatalf("Got error calling DeleteItem: %s", err)
	}

	fmt.Println("Deleted '" + movieName + "' (" + movieYear + ") from table " + tableName)
}

// Uses DynamoDB GetItem
func DynamoGetItemWithScan(w http.ResponseWriter, r *http.Request) {
	tableName := "Movies"
	minRating := 4.0
	year := 2013

	filt := expression.Name("Year").Equal(expression.Value(year))
	filt2 := expression.Name("Rating").GreaterThan(expression.Value(minRating))

	proj := expression.NamesList(expression.Name("Title"), expression.Name("Year"), expression.Name("Rating"))

	expr, err := expression.NewBuilder().WithFilter(filt).WithFilter(filt2).WithProjection(proj).Build()
	if err != nil {
	    log.Fatalf("Got error building expression: %s", err)
	}

	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(tableName),
	}

	result, err := svc.Scan(params)
	if err != nil {
	    log.Fatalf("Query API call failed: %s", err)
	}

	fmt.Println(result)
}

func DynamoGetAllItems(w http.ResponseWriter, r *http.Request) {
	tableName := "Movies"

	params := &dynamodb.ScanInput{
		TableName:                 aws.String(tableName),
	}

	result, err := svc.Scan(params)
	if err != nil {
	    log.Fatalf("Query API call failed: %s", err)
	}

	jsonData := map[string]interface{}{
		"result":result.Items,
	}
	json.NewEncoder(w).Encode(jsonData)
}

func main() {
    err := godotenv.Load()
    if err != nil {
        log.Fatal("Error loading .env file:", err)
        return
    }

    http.HandleFunc("/createTable", DynamoCreateTable)
    http.HandleFunc("/createTableItem", DynamoCreateTableItem)
    http.HandleFunc("/listTables", DynamoListTables)
	http.HandleFunc("/listItems", DynamoListTableItems)
	http.HandleFunc("/updateItem", DynamoUpdateItem)
	http.HandleFunc("/deleteItem", DynamoDeleteItem)
	http.HandleFunc("/getItemWithScan", DynamoGetItemWithScan)
	http.HandleFunc("/getAllItems", DynamoGetAllItems)

    fmt.Println("Server listening on port 8080...")
    log.Fatal(http.ListenAndServe(":8080", nil))
}
