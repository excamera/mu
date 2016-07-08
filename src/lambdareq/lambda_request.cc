#include "awsv4_sig.hh"
#include "lambda_request.hh"

#include <cassert>
#include <ctime>
#include <map>
#include <sstream>

using namespace std;

static const string BASEURI_ = "/2015-03-31/functions/";
static const string INVOKE_ = "/invocations";
static const string REGION_ = "us-east-1";
static const string SERVICE_ = "lambda";
static const string HOST_ = "lambda.us-east-1.amazonaws.com";

LambdaRequest::LambdaRequest(const string &fn_name,
                             const string &secret,
                             const string &akid,
                             const string &payload,
                             const string &qualifier,
                             InvocationType type)
    : request_date_ (x_amz_date_(time(0)))
    , request_uri_ (BASEURI_ + fn_name + INVOKE_)
    , query_string_ ()
    , secret_ (secret)
    , akid_ (akid)
    , payload_ (payload)
    , headers_ ({{"x-amz-invocation-type", "RequestResponse"}
               , {"host", HOST_ }
               , {"content-type", "application/x-amz-json-1.0"}
               , {"x-amz-date", request_date_}
               , {"content-length", to_string(payload.length())}
                 })
{
    // invocation type header update, if necessary
    switch (type) {
        case InvocationType::Event:
            headers_["x-amz-invocation-type"] = "Event";
            break;
        case InvocationType::DryRun:
            headers_["x-amz-invocation-type"] = "DryRun";
            break;
        default:
            break;
    }

    // canonical qualifier, if we need one
    if (qualifier.length() > 0) {
        query_string_ = "Qualifier=" + qualifier;
    }

    AWSv4Sig::sign_request(secret_, akid_, REGION_, SERVICE_, request_uri_,
                           request_date_, query_string_, payload_, headers_);
}

string
LambdaRequest::x_amz_date_(const time_t &t) {
    char sbuf[17];
    strftime(sbuf, 17, "%Y%m%dT%H%M%SZ", gmtime(&t));
    return string(sbuf, 16);
}

HTTPRequest
LambdaRequest::to_http_request(void) const {
    HTTPRequest req;

    // construct request
    {
        stringstream sreq;
        sreq << "POST " << request_uri_ << '?' << query_string_ << " HTTP/1.1";
        req.set_first_line(sreq.str());
    }

    // headers
    for (const auto &hd: headers_) {
        req.add_header(HTTPHeader(hd.first, hd.second));
    }
    req.done_with_headers();

    // payload
    req.read_in_body(payload_);
    assert( req.state() == COMPLETE );

    return req;
}
