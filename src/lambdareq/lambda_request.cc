#include "awsv4_sig.hh"
#include "lambda_request.hh"

#include <ctime>
#include <map>
#include <sstream>

using namespace std;

LambdaRequest::LambdaRequest(string fn_name,
                             string secret,
                             string akid,
                             string payload,
                             string qualifier,
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

