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
	_ "github.com/lib/pq"
)

type RequestBody struct {
	Query       string `json:"query"`
	Destination string `json:"destination"`
}

type SuccessResponse struct {
	JobId     string `json:"jobId"`
	JobStatus string `json:"jobStatus"`
}

type FailureResponse struct {
	Message string `json:message`
}

func Validate(token string, query string, vaultId string) string {
	if len(query) == 0 {
		log.Printf("Got invalid query: %v\n", query)
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
		log.Printf("Got error on Skyflow Validation request: %v\n", err.Error())
		return ""
	}

	if response.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(response.Body)
		defer response.Body.Close()
		log.Printf("Got status on Skyflow Validation: %v\n", response.StatusCode)
		log.Printf("Got response on Skyflow Validation: %v\n", string(responseBody))
		return ""
	}
	return response.Header.Get("x-request-id")
}

func TriggerEMRJob(query string, destination string, requestId string) (string, error) {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("REGION")),
	})

	EntryPointArguments := []*string{aws.String(query), aws.String(destination), aws.String(requestId)}
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
		log.Printf("%v-> Failed to trigger EMR Job with error: %v\n", requestId, err.Error())
		return "", err
	}

	return *jobRunOutput.JobRunId, nil
}

func LogJob(jobId string, requestId string) {
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
	db, _ := sql.Open("postgres", connection)

	statement := `insert into "job_details"("job_id", "job_status", "start_time") values($1, $2, $3)`
	
	log.Printf("%v-> Inserting record for jobId: %v\n", requestId, jobId)
	_, err := db.Exec(statement, jobId, "Initiated", time.Now())

	if err != nil {
		log.Printf("%v-> Failed to insert record for jobId: %v with error: %v\n", requestId, jobId, err.Error())
		return
	}

	defer db.Close()

	log.Printf("%v-> Successfully logged jobId: %v\n", requestId, jobId)
}

func HandleRequest(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	apiResponse := events.APIGatewayProxyResponse{}

	var body RequestBody
	json.Unmarshal([]byte(request.Body), &body)

	vaultId := request.PathParameters["vaultID"]
	token := request.Headers["Authorization"]
	requestId := Validate(token, body.Query, vaultId)

	if requestId == "" {
		responseBody, _ := json.Marshal(FailureResponse{
			Message: "Failed on Skyflow Validation",
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusBadRequest

		return apiResponse, nil
	}
	
	log.Printf("%v-> Triggering Spark job with args, query: %v, destination: %v, requestId: %v\n", requestId, body.Query, body.Destination, requestId)
	jobId, err := TriggerEMRJob(body.Query, body.Destination, requestId)
	if err != nil {
		responseBody, _ := json.Marshal(FailureResponse{
			Message: fmt.Sprintf("%v-> Failed to trigger Spark job with error: %v\n", requestId, err.Error()),
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusBadRequest

		return apiResponse, nil
	}

	LogJob(jobId, requestId)

	responseBody, _ := json.Marshal(SuccessResponse{
		JobId:     jobId,
		JobStatus: "Initiated",
	})

	apiResponse.Body = string(responseBody)
	apiResponse.StatusCode = http.StatusOK

	return apiResponse, nil
}

func main() {
	lambda.Start(HandleRequest)
}
