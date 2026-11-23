package gateway

import (
	"errors"
	"net/http"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	status  int
}

func (e *apiError) Error() string { return e.Code }

var (
	invalid       = &apiError{"INVALID_ARGUMENT", "Invalid request", http.StatusBadRequest}
	unauthorized  = &apiError{"UNAUTHORIZED", "Invalid bearer token", http.StatusUnauthorized}
	notFound      = &apiError{"NOT_FOUND", "Not found", http.StatusNotFound}
	limitExceeded = &apiError{"LIMIT_EXCEEDED", "Size limit exceeded", http.StatusRequestEntityTooLarge}
	overloaded    = &apiError{"OVERLOADED", "Admission limit reached", http.StatusServiceUnavailable}
	unavailable   = &apiError{"UNAVAILABLE", "Service unavailable", http.StatusServiceUnavailable}
	internalError = &apiError{"INTERNAL_ERROR", "Internal error", http.StatusInternalServerError}
	staleOwner    = &apiError{"UNAVAILABLE", "Owner changed", http.StatusServiceUnavailable}
)

var wireErrors = map[pb.ErrorCode]struct {
	grpc   codes.Code
	public *apiError
}{
	pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT:      {codes.InvalidArgument, invalid},
	pb.ErrorCode_ERROR_CODE_UNAUTHORIZED:          {codes.Unauthenticated, unauthorized},
	pb.ErrorCode_ERROR_CODE_NOT_FOUND:             {codes.NotFound, notFound},
	pb.ErrorCode_ERROR_CODE_OBJECT_ALREADY_EXISTS: {codes.AlreadyExists, &apiError{"OBJECT_ALREADY_EXISTS", "Object already exists", 409}},
	pb.ErrorCode_ERROR_CODE_CODE_VERSION_CONFLICT: {codes.AlreadyExists, &apiError{"CODE_VERSION_CONFLICT", "Code version conflict", 409}},
	pb.ErrorCode_ERROR_CODE_REQUEST_ID_CONFLICT:   {codes.AlreadyExists, &apiError{"REQUEST_ID_CONFLICT", "Request ID conflict", 409}},
	pb.ErrorCode_ERROR_CODE_LIMIT_EXCEEDED:        {codes.ResourceExhausted, limitExceeded},
	pb.ErrorCode_ERROR_CODE_OVERLOADED:            {codes.ResourceExhausted, overloaded},
	pb.ErrorCode_ERROR_CODE_UNAVAILABLE:           {codes.Unavailable, unavailable},
	pb.ErrorCode_ERROR_CODE_EXECUTION_TIMEOUT:     {codes.DeadlineExceeded, &apiError{"EXECUTION_TIMEOUT", "Execution timed out", 504}},
	pb.ErrorCode_ERROR_CODE_INTERNAL_ERROR:        {codes.Internal, internalError},
	pb.ErrorCode_ERROR_CODE_STALE_OWNER:           {codes.FailedPrecondition, staleOwner},
	pb.ErrorCode_ERROR_CODE_STALE_CONTROLLER:      {codes.FailedPrecondition, unavailable},
}

func publicError(err error) *apiError {
	var e *apiError
	if errors.As(err, &e) {
		return e
	}
	s := status.Convert(err)
	for _, detail := range s.Details() {
		if d, ok := detail.(*pb.ErrorDetail); ok {
			mapping, known := wireErrors[d.Code]
			if known && mapping.grpc == s.Code() {
				return mapping.public
			}
			return unavailable
		}
	}
	// A transport deadline alone cannot confirm that execution timed out.
	return unavailable
}
