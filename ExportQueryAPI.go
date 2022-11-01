package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
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
	RequestId string `json:"requestId"`
	JobStatus string `json:"jobStatus"`
}

type FailureResponse struct {
	Id      string `json:"id"`
	Message string `json:"message"`
}

type SkyflowAuthorizationResponse struct {
	RequestId    string `json:"requestId"`
	StatusCode   int    `json:"statusCode"`
	ResponseBody string `json:"responseBody"`
	Error        string `json:"error"`
}

var db *sql.DB
var region *string
var applicationId *string
var executionRoleArn *string
var sparkSubmitParameters *string
var entryPoint *string
var logUri *string
var vaultUrl string
var service *emrserverless.EMRServerless
var secrets *string

func init() {
	applicationId = aws.String(os.Getenv("APPLICATION_ID"))
	executionRoleArn = aws.String(os.Getenv("EXECUTION_ROLE_ARN"))
	sparkSubmitParameters = aws.String(os.Getenv("SPARK_SUBMIT_PARAMETERS"))
	entryPoint = aws.String(os.Getenv("ENTRYPOINT"))
	logUri = aws.String(os.Getenv("LOG_URI"))
	secrets = aws.String(os.Getenv("SECRETS"))
	region = aws.String(os.Getenv("REGION"))

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

	vaultUrl = os.Getenv("VAULT_URL")

	sess, _ := session.NewSession(&aws.Config{
		Region: region,
	})
	service = emrserverless.New(sess)
}

func main() {
	lambda.Start(HandleRequest)
}

func HandleRequest(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	apiResponse := events.APIGatewayProxyResponse{}
	id := uuid.New().String()

	log.Printf("%v-> Initiated with id: %v\n", id, id)

	clientIpAddress, IpPresent := request.Headers["CF-Connecting-IP"]
	if IpPresent {
		log.Printf("%v-> Client IP address: %v", id, clientIpAddress)
	}

	var body RequestBody
	json.Unmarshal([]byte(request.Body), &body)

	vaultId := request.PathParameters["vaultID"]
	token := request.Headers["Authorization"]
	authResponse := SkyflowAuthorization(token, body.Query, vaultId, id)

	if authResponse.Error != "" {
		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: authResponse.Error,
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = authResponse.StatusCode

		return apiResponse, nil
	}

	if authResponse.StatusCode != http.StatusOK {
		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: authResponse.ResponseBody,
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = authResponse.StatusCode

		return apiResponse, nil
	}

	log.Printf("%v-> Sucessfully Authorized", id)

	log.Printf("%v-> Triggering Spark job with args, query: %v, destination: %v", id, body.Query, body.Destination)

	jobId, err := TriggerEMRJob(body.Query, body.Destination, id)
	if err != nil {
		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: fmt.Sprintf("Failed to trigger Spark job with error: %v\n", err.Error()),
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusInternalServerError

		return apiResponse, nil
	}

	jobStatus := "INITIATED"

	logJobError := LogJob(id, jobId, jobStatus, authResponse.RequestId, body.Query, body.Destination)
	if logJobError != nil {
		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: fmt.Sprintf("Failed to log job with error: %v\n", logJobError.Error()),
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusInternalServerError

		return apiResponse, nil
	}

	responseBody, _ := json.Marshal(SuccessResponse{
		Id:        id,
		JobId:     jobId,
		JobStatus: jobStatus,
		RequestId: authResponse.RequestId,
	})

	apiResponse.Body = string(responseBody)
	apiResponse.StatusCode = http.StatusOK

	return apiResponse, nil
}

func SkyflowAuthorization(token string, query string, vaultId string, id string) SkyflowAuthorizationResponse {
	var authResponse SkyflowAuthorizationResponse

	log.Printf("%v-> Initiating SkyflowAuthorization", id)

	if len(query) == 0 {
		log.Printf("%v-> Got invalid query: %v\n", id, query)

		authResponse.StatusCode = http.StatusUnauthorized
		authResponse.Error = errors.New("Invalid Query").Error()
		return authResponse
	}
	payloadBody, _ := json.Marshal(map[string]string{
		"query": query,
	})
	payload := bytes.NewBuffer(payloadBody)

	client := &http.Client{Timeout: 1 * time.Minute}
	var url = vaultUrl + "/v1/vaults/" + vaultId + "/query"

	log.Printf("%v-> Initiating Skyflow Request for Authorization", id)

	request, _ := http.NewRequest("POST", url, payload)
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Authorization", "Bearer "+token)

	response, err := client.Do(request)
	if err != nil {
		log.Printf("%v-> Got error on Skyflow Authorization: %v\n", id, err.Error())

		authResponse.StatusCode = http.StatusInternalServerError
		authResponse.Error = err.Error()
		return authResponse
	}

	responseBody, _ := io.ReadAll(response.Body)
	defer response.Body.Close()

	authResponse.RequestId = response.Header.Get("x-request-id")
	authResponse.StatusCode = response.StatusCode
	authResponse.ResponseBody = string(responseBody)

	if response.StatusCode != http.StatusOK {
		log.Printf("%v-> Unable/Fail to call Skyflow API status code:%v and message:%v", id, response.StatusCode, string(responseBody))
	}

	return authResponse
}

func TriggerEMRJob(query string, destination string, id string) (string, error) {
	entryPointArguments := []*string{aws.String(query), aws.String(destination), aws.String(id), secrets, region}

	log.Printf("%v-> Initiating TriggerEMRJob", id)

	params := &emrserverless.StartJobRunInput{
		ApplicationId:    applicationId,
		ExecutionRoleArn: executionRoleArn,
		JobDriver: &emrserverless.JobDriver{
			SparkSubmit: &emrserverless.SparkSubmit{
				EntryPoint:            entryPoint,
				EntryPointArguments:   entryPointArguments,
				SparkSubmitParameters: sparkSubmitParameters,
			},
		},
		ConfigurationOverrides: &emrserverless.ConfigurationOverrides{
			MonitoringConfiguration: &emrserverless.MonitoringConfiguration{
				S3MonitoringConfiguration: &emrserverless.S3MonitoringConfiguration{
					LogUri: logUri,
				},
			},
		},
	}

	log.Printf("%v-> Submitting EMR job", id)

	jobRunOutput, err := service.StartJobRun(params)

	if err != nil {
		log.Printf("%v-> Failed to trigger EMR Job with error: %v\n", id, err.Error())
		return "", err
	}

	log.Printf("%v-> Successfully submitted EMR Job", id)

	return *jobRunOutput.JobRunId, nil
}

func LogJob(id string, jobId string, jobStatus string, requestId string, query string, destination string) error {

	log.Printf("%v-> Initiating LogJob", id)

	statement := `INSERT INTO "emr_job_details"("id", "jobid", "jobstatus", "requestid", "query", "destination", "createdat") VALUES($1, $2, $3, $4, $5, $6, $7)`

	log.Printf("%v-> Inserting record for jobId: %v & requestId:%v\n", id, jobId, requestId)
	_, err := db.Exec(statement, id, jobId, jobStatus, requestId, query, destination, time.Now())

	if err != nil {
		log.Printf("%v-> Failed to insert record for jobId: %v with error: %v\n", requestId, jobId, err.Error())
		return err
	}

	log.Printf("%v-> Successfully logged jobId: %v & requestId:%v\n", id, jobId, requestId)
	return err
}
