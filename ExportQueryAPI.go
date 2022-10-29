package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emrserverless"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type RequestBody struct {
	Query       string `json:"query"`
	Destination string `json:"destination"`
}

type SuccessResponse struct {
	Id        string `json:"id"`
	JobId     string `json:"jobId"`
	RequestId string `json:"RequestId"`
	JobStatus string `json:"jobStatus"`
}

type FailureResponse struct {
	Id      string `json:"id"`
	Message string `json:"message"`
}

var db *sql.DB

func init() {
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	databaseName := os.Getenv("DB_NAME")

	connection := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host,
		port,
		user,
		password,
		databaseName,
	)
	db, _ = sql.Open("postgres", connection)
}

func main() {
	lambda.Start(HandleRequest)
}

func HandleRequest(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	apiResponse := events.APIGatewayProxyResponse{}
	id := uuid.New().String()

	var body RequestBody
	json.Unmarshal([]byte(request.Body), &body)

	vaultId := request.PathParameters["vaultID"]
	token := request.Headers["Authorization"]
	requestId := SkyflowValidation(token, body.Query, vaultId, id)

	if requestId == "" {
		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: "Failed on Skyflow Validation",
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusBadRequest

		return apiResponse, nil
	}

	log.Printf("%v-> Triggering Spark job with args, query: %v, destination: %v", id, body.Query, body.Destination)
	jobId, err := TriggerEMRJob(body.Query, body.Destination, id)
	if err != nil {
		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: fmt.Sprintf("Failed to trigger Spark job with error: %v\n", err.Error()),
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusBadRequest

		return apiResponse, nil
	}

	jobStatus := "Initiated"
	LogJob(id, jobId, jobStatus, requestId, body.Query, body.Destination)

	responseBody, _ := json.Marshal(SuccessResponse{
		Id:        id,
		JobId:     jobId,
		JobStatus: jobStatus,
		RequestId: requestId,
	})

	apiResponse.Body = string(responseBody)
	apiResponse.StatusCode = http.StatusOK

	return apiResponse, nil
}

func SkyflowValidation(token string, query string, vaultId string, id string) string {
	if len(query) == 0 {
		log.Printf("%v-> Got invalid query: %v\n", id, query)
		return ""
	}
	payloadBody, _ := json.Marshal(map[string]string{
		"query": query,
	})
	payload := bytes.NewBuffer(payloadBody)

	client := &http.Client{Timeout: 1 * time.Minute}
	var vaultUrl = os.Getenv("VAULT_URL")
	var url = vaultUrl + vaultId + "/query"

	request, _ := http.NewRequest("POST", url, payload)
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Authorization", "Bearer "+token)

	response, err := client.Do(request)
	if err != nil {
		log.Printf("%v-> Got error on Skyflow Validation request: %v\n", id, err.Error())
		return ""
	}

	if response.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(response.Body)
		defer response.Body.Close()
		log.Printf("%v-> Got status on Skyflow Validation: %v\n", id, response.StatusCode)
		log.Printf("%v-> Got response on Skyflow Validation: %v\n", id, string(responseBody))
		return ""
	}
	return response.Header.Get("x-request-id")
}

func TriggerEMRJob(query string, destination string, id string) (string, error) {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("REGION")),
	})

	EntryPointArguments := []*string{aws.String(query), aws.String(destination), aws.String(id)}
	ApplicationId := aws.String(os.Getenv("APPLICATION_ID"))
	ExecutionRoleArn := aws.String(os.Getenv("EXECUTION_ROLE_ARN"))
	SparkSubmitParameters := aws.String(os.Getenv("SPARK_SUBMIT_PARAMETERS"))
	EntryPoint := aws.String(os.Getenv("ENTRYPOINT"))
	LogUri := aws.String(os.Getenv("LOG_URI"))

	service := emrserverless.New(sess)

	params := &emrserverless.StartJobRunInput{
		ApplicationId:    ApplicationId,
		ExecutionRoleArn: ExecutionRoleArn,
		JobDriver: &emrserverless.JobDriver{
			SparkSubmit: &emrserverless.SparkSubmit{
				EntryPoint:            EntryPoint,
				EntryPointArguments:   EntryPointArguments,
				SparkSubmitParameters: SparkSubmitParameters,
			},
		},
		ConfigurationOverrides: &emrserverless.ConfigurationOverrides{
			MonitoringConfiguration: &emrserverless.MonitoringConfiguration{
				S3MonitoringConfiguration: &emrserverless.S3MonitoringConfiguration{
					LogUri: LogUri,
				},
			},
		},
	}

	jobRunOutput, err := service.StartJobRun(params)

	if err != nil {
		log.Printf("%v-> Failed to trigger EMR Job with error: %v\n", id, err.Error())
		return "", err
	}

	return *jobRunOutput.JobRunId, nil
}

func LogJob(id string, jobId string, jobStatus string, requestId string, query string, destination string) {

	statement := `insert into "emr_job_details"("id", "jobid", "jobstatus", "requestid", "query", "destination", "createdat") values($1, $2, $3, $4, $5, $6, $7)`

	log.Printf("%v-> Inserting record for jobId: %v & requestId:%v\n", id, jobId, requestId)
	_, err := db.Exec(statement, id, jobId, jobStatus, requestId, query, destination, time.Now())

	if err != nil {
		log.Printf("%v-> Failed to insert record for jobId: %v with error: %v\n", requestId, jobId, err.Error())
		return
	}

	defer db.Close()

	log.Printf("%v-> Successfully logged jobId: %v & requestId:%v\n", id, jobId, requestId)
}
