#include "awsv4_sig.hh"
#include "lambda_request.hh"

#include <cassert>
#include <ctime>
#include <map>
#include <sstream>
#include <utility>

using namespace std;

static const string BASEURI_ = "/2015-03-31/functions/";

/*
 * LambdaListFnVersions - list all versions of a given function
 */

LambdaListFnVersions::LambdaListFnVersions(const string &secret,
                                           const string &akid,
                                           const string &fn_name,
                                           const string &region)
    : LambdaRequest ( {BASEURI_ + fn_name + "/versions"}
                    , {}
                    , secret
                    , akid
                    , {}
                    , region
                    , {}
                    , false
                    )
{
    add_authorization();
}

/*
 * LambdaListFunctions - list available lambda functions
 */

LambdaListFunctions::LambdaListFunctions(const string &secret,
                                         const string &akid,
                                         const string &region)
    : LambdaRequest ( string(BASEURI_)
                    , {}
                    , secret
                    , akid
                    , {}
                    , region
                    , {}
                    , false
                    )
{
    add_authorization();
}

/*
 * LambdaInvocation - invoke a lambda function
 */

LambdaInvocation::LambdaInvocation(const string &secret,
                                   const string &akid,
                                   const string &fn_name,
                                   const string &payload,
                                   const string &qualifier,
                                   const InvocationType type,
                                   const string &region)
    : LambdaRequest ( {BASEURI_ + fn_name + "/invocations"}
                    , {}
                    , secret
                    , akid
                    , payload
                    , region
                    , { {"x-amz-invocation-type", "RequestResponse"}
                      , {"content-type", "application/x-amz-json-1.0"}
                      , {"content-length", to_string(payload.length())}
                      }
                    , true
                    )
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

    add_authorization();
}

/*
 * LambdaRequest
 */

static const string SERVICE_ = "lambda";

LambdaRequest::LambdaRequest (string &&uri,
                              const string &query,
                              const string &secret,
                              const string &akid,
                              const string &payload,
                              const string &region,
                              map<string, string> &&headers,
                              bool post)
    : request_date_ (x_amz_date_(time(0)))
    , request_uri_ (uri)
    , query_string_ (query)
    , secret_ (secret)
    , akid_ (akid)
    , payload_ (payload)
    , method_ (post ? "POST" : "GET")
    , region_ (region)
    , headers_ (headers)
{
    // insert basic headers
    headers_["x-amz-date"] = request_date_;
    headers_["host"] = SERVICE_ + "." + region_ + ".amazonaws.com";
}

string
LambdaRequest::x_amz_date_(const time_t &t) {
    char sbuf[17];
    strftime(sbuf, 17, "%Y%m%dT%H%M%SZ", gmtime(&t));
    return string(sbuf, 16);
}

void
LambdaRequest::add_authorization(void) {
    AWSv4Sig::sign_request(method_, secret_, akid_, region_, SERVICE_, request_uri_,
                           request_date_, query_string_, payload_, headers_);
}

HTTPRequest
LambdaRequest::to_http_request(void) const {
    HTTPRequest req;

    // construct request
    {
        stringstream sreq;
        sreq << method_ << ' ' << request_uri_ << '?' << query_string_ << " HTTP/1.1";
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
