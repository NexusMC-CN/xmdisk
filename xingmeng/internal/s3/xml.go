package s3

import (
	"encoding/xml"
	"errors"
	"net/http"

	"xingmeng/internal/errs"
)

const xmlNs = "http://s3.amazonaws.com/doc/2006-03-01/"

// ---- D017 错误映射：sentinel ↔ S3 Code/Status 穷举表 ----

type errMapping struct {
	Code   string
	Status int
}

var errMap = map[error]errMapping{
	errs.ErrBucketNotFound:     {Code: "NoSuchBucket", Status: 404},
	errs.ErrKeyNotFound:        {Code: "NoSuchKey", Status: 404},
	errs.ErrUploadNotFound:     {Code: "NoSuchUpload", Status: 404},
	errs.ErrBucketExists:       {Code: "BucketAlreadyExists", Status: 409},
	errs.ErrIsDirectory:        {Code: "NoSuchKey", Status: 404},
	errs.ErrIsFile:             {Code: "InvalidRequest", Status: 400},
	errs.ErrDirNotEmpty:        {Code: "BucketNotEmpty", Status: 409},
	errs.ErrQuotaExceeded:      {Code: "QuotaExceeded", Status: 507},
	errs.ErrReadOnly:           {Code: "ReadOnly", Status: 503},
	errs.ErrEntityTooLarge:     {Code: "EntityTooLarge", Status: 400},
	errs.ErrRangeInvalid:       {Code: "InvalidRange", Status: 416},
	errs.ErrPreconditionFailed: {Code: "PreconditionFailed", Status: 412},
	errs.ErrNotModified:        {Code: "NotModified", Status: 304},
	errs.ErrPermissionDenied:   {Code: "AccessDenied", Status: 403},
	errs.ErrInvalidArgument:    {Code: "InvalidArgument", Status: 400},
	errs.ErrStaleEpoch:         {Code: "InternalError", Status: 500},
	errs.ErrInternal:           {Code: "InternalError", Status: 500},
}

// sentinelList 是需要映射覆盖的完整清单；init 检查一对一（漏配拒绝启动，99-D017）。
var sentinelList = []error{
	errs.ErrBucketNotFound, errs.ErrKeyNotFound, errs.ErrUploadNotFound,
	errs.ErrBucketExists, errs.ErrIsDirectory, errs.ErrIsFile, errs.ErrDirNotEmpty,
	errs.ErrQuotaExceeded, errs.ErrReadOnly, errs.ErrEntityTooLarge, errs.ErrRangeInvalid,
	errs.ErrPreconditionFailed, errs.ErrNotModified, errs.ErrPermissionDenied,
	errs.ErrInvalidArgument, errs.ErrStaleEpoch, errs.ErrInternal,
}

// authErrCodes：验签失败细分（不能进 sentinel 表：errSigMismatch 等是派生错误）。
func mapAuthError(err error) errMapping {
	switch {
	case err == errSigMismatch:
		return errMapping{Code: "SignatureDoesNotMatch", Status: 403}
	case err == errBadAccessKey:
		return errMapping{Code: "InvalidAccessKeyId", Status: 403}
	default:
		return errMap[errs.ErrPermissionDenied]
	}
}

func mapError(err error) errMapping {
	for _, s := range sentinelList {
		if errors.Is(err, s) {
			return errMap[s]
		}
	}
	return errMap[errs.ErrInternal]
}

// ---- XML 载荷 ----

type errorXML struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

func writeXMLError(w http.ResponseWriter, status int, code, msg, resource, reqID string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(errorXML{Code: code, Message: msg, Resource: resource, RequestID: reqID})
}

func writeXML(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(v)
}

type listResultV2 struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	Xmlns                 string   `xml:"xmlns,attr"`
	Name                  string   `xml:"Name"`
	Prefix                string   `xml:"Prefix"`
	Delimiter             string   `xml:"Delimiter"`
	MaxKeys               int      `xml:"MaxKeys"`
	KeyCount              int      `xml:"KeyCount"`
	IsTruncated           bool     `xml:"IsTruncated"`
	ContinuationToken     string   `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
	StartAfter            string   `xml:"StartAfter,omitempty"`
	Contents              []objectXML
	CommonPrefixes        []commonPrefixXML `xml:"CommonPrefixes"`
}

type listResultV1 struct {
	XMLName        xml.Name `xml:"ListBucketResult"`
	Xmlns          string   `xml:"xmlns,attr"`
	Name           string   `xml:"Name"`
	Prefix         string   `xml:"Prefix"`
	Delimiter      string   `xml:"Delimiter"`
	MaxKeys        int      `xml:"MaxKeys"`
	IsTruncated    bool     `xml:"IsTruncated"`
	Marker         string   `xml:"Marker"`
	NextMarker     string   `xml:"NextMarker,omitempty"`
	Contents       []objectXML
	CommonPrefixes []commonPrefixXML `xml:"CommonPrefixes"`
}

type objectXML struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type commonPrefixXML struct {
	Prefix string `xml:"Prefix"`
}

type deleteRequestXML struct {
	XMLName xml.Name `xml:"Delete"`
	Quiet   bool     `xml:"Quiet"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

type deleteResponseXML struct {
	XMLName xml.Name `xml:"DeleteResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Deleted []struct {
		Key string `xml:"Key"`
	} `xml:"Deleted"`
	Error []struct {
		Key     string `xml:"Key"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
}

type initiateMultipartXML struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartRequestXML struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	} `xml:"Part"`
}

type completeMultipartXML struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type listUploadsXML struct {
	XMLName xml.Name `xml:"ListMultipartUploadsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Bucket  string   `xml:"Bucket"`
	Upload  []uploadXML
}

type uploadXML struct {
	Key       string `xml:"Key"`
	UploadID  string `xml:"UploadId"`
	Initiated string `xml:"Initiated"`
}

type listPartsXML struct {
	XMLName  xml.Name `xml:"ListPartsResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
	Part     []partXML
}

type partXML struct {
	PartNumber   int    `xml:"PartNumber"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	LastModified string `xml:"LastModified"`
}
