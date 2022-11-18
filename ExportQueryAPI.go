package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emrserverless"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"github.com/golang-jwt/jwt"
)

type RequestBody struct {
	Query             string `json:"query"`
	Destination       string `json:"destination"`
	CrossBucketRegion string `json:"region"`
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

var id string
var logger *log.Entry
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
var validVaultIds []string
var re *regexp.Regexp
var source string

func init() {
	log.SetFormatter(&log.JSONFormatter{})

	applicationId = aws.String(os.Getenv("APPLICATION_ID"))
	executionRoleArn = aws.String(os.Getenv("EXECUTION_ROLE_ARN"))
	sparkSubmitParameters = aws.String(os.Getenv("SPARK_SUBMIT_PARAMETERS"))
	entryPoint = aws.String(os.Getenv("ENTRYPOINT"))
	logUri = aws.String(os.Getenv("LOG_URI"))
	secrets = aws.String(os.Getenv("SECRETS"))
	region = aws.String(os.Getenv("REGION"))

	re = regexp.MustCompile(`^s3://([^/]+)/(.*?([^/]+)/?)$`)

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

	validVaultIds = strings.Split(os.Getenv("VALID_VAULT_IDS"), ",")

	source = "ExportQueryAPI"
}

func main() {
	lambda.Start(HandleRequest)
}

func HandleRequest(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	id = uuid.New().String()

	logger = log.WithFields(log.Fields{
		"queryId": id,
		"source": source,
	})

	apiResponse := events.APIGatewayProxyResponse{}

	logger.Info(fmt.Sprintf("Initiated %v", source))

	clientIpAddress := strings.Split(request.Headers["X-Forwarded-For"], ",")[0]
	logger.Info(fmt.Sprintf("Client IP address: %v", clientIpAddress))

	logger = logger.WithFields(log.Fields{
		"clientIp": clientIpAddress,
	})

	var body RequestBody
	body.CrossBucketRegion = *region
	json.Unmarshal([]byte(request.Body), &body)

	vaultId := request.PathParameters["vaultID"]
	token := request.Headers["Authorization"]

	if !re.Match([]byte(body.Destination)) {
		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: "Invalid s3 destination path.",
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusBadRequest

		return apiResponse, nil
	}

	authSchemeValidation := ValidateAuthScheme(token)
	if !authSchemeValidation {
		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: "Auth Scheme not supported",
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusUnauthorized

		return apiResponse, nil
	}

	jti, err := ExtractJTI(token)
	if err != nil {
		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: fmt.Sprintf("Failed to extract jti with error: %v", err.Error()),
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusForbidden

		return apiResponse, nil
	}

	logger = logger.WithFields(log.Fields{
		"jti": jti,
	})

	validVaultIdValidation := ValidateVaultId(vaultId)
	if !validVaultIdValidation {
		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: "Invalid Vault ID",
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusForbidden

		return apiResponse, nil
	}

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

	logger = logger.WithFields(log.Fields{
		"skyflowRequestId": authResponse.RequestId,
		"query": body.Query,
		"destinationBucket": body.Destination,
		"region": body.CrossBucketRegion,
	})

	logger.Info("Sucessfully Authorized")

	logger.Info(fmt.Sprintf("Triggering Spark job with args, query: %v, destination: %v", body.Query, body.Destination))

	jobId, err := TriggerEMRJob(body.Query, id)
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

	logJobError := LogJob(id, jobId, jobStatus, authResponse.RequestId, body.Query, body.Destination, body.CrossBucketRegion, jti, clientIpAddress)
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

func ValidateAuthScheme(token string) bool {
	logger.Info("Initiating ValidateAuthScheme")

	authScheme := strings.Split(token, " ")[0]

	if authScheme != "Bearer" {
		return false
	}
	return true
}

func ExtractJTI(authToken string) (string, error) {
	logger.Info("Initiating ExtractJTI")

	tokenString := strings.Split(authToken, " ")[1]

	logger.Info("Initiating token parsing")
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		logger.Error(fmt.Sprintf("Got error: %v in token parsing", err.Error()))
		return "", err
	}

	logger.Info("Successfully parsed token")
	claims := token.Claims.(jwt.MapClaims)
	jti := claims["jti"].(string)

	return jti, nil
}

func ValidateVaultId(vaultId string) bool {
	logger.Info("Initiating ValidateVaultId")

	for _, validVaultId := range validVaultIds {
		if vaultId == validVaultId {
			return true
		}
	}
	return false
}

func SkyflowAuthorization(token string, query string, vaultId string, id string) SkyflowAuthorizationResponse {
	var authResponse SkyflowAuthorizationResponse

	logger.Info("Initiating SkyflowAuthorization")

	if len(query) == 0 {
		logger.Error("Got invalid query")

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

	logger.Info("Initiating Skyflow Request for Authorization")

	request, _ := http.NewRequest("POST", url, payload)
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Authorization", token)

	response, err := client.Do(request)
	if err != nil {
		logger.Error(fmt.Sprintf("Got error on Skyflow Authorization: %v", err.Error()))

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
		logger.Error("Unable/Fail to call Skyflow API status code:%v and message", response.StatusCode, string(responseBody))
	}

	return authResponse
}

func TriggerEMRJob(query string, id string) (string, error) {
	entryPointArguments := []*string{aws.String(query), aws.String(id), secrets, region}

	logger.Info("Initiating TriggerEMRJob")

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

	logger.Info("Submitting EMR job")

	jobRunOutput, err := service.StartJobRun(params)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to trigger EMR Job with error: %v", err.Error()))
		return "", err
	}

	logger.Info("Successfully submitted EMR Job")

	return *jobRunOutput.JobRunId, nil
}

func LogJob(id string, jobId string, jobStatus string, requestId string, query string, destination string, cross_bucket_region string, jti string, clientIp string) error {

	logger.Info("Initiating LogJob")

	statement := `INSERT INTO "emr_job_details"("id", "jobid", "jobstatus", "requestid", "query", "destination", "createdat", "cross_bucket_region", "jti", "client_ip") VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

	logger.Info(fmt.Sprintf("Inserting record for jobId: %v & requestId:%v", jobId, requestId))
	_, err := db.Exec(statement, id, jobId, jobStatus, requestId, query, destination, time.Now(), cross_bucket_region, jti, clientIp)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to insert record for jobId: %v with error: %v", jobId, err.Error()))
		return err
	}

	logger.Info(fmt.Sprintf("Successfully logged jobId: %v & requestId:%v", jobId, requestId))
	return err
}
