#pragma once
#include <ctime>
#include <map>
#include <string>
#include "http_request.hh"

class LambdaRequest {
  private:
    static std::string x_amz_date_(const std::time_t &t);

  protected:
    std::string request_date_;
    std::string request_uri_;
    std::string query_string_;
    std::string secret_;
    std::string akid_;
    std::string payload_;
    std::string method_;
    std::string region_;
    std::map<std::string, std::string> headers_;
    void add_authorization(void);

  public:
    HTTPRequest to_http_request(void) const;
    LambdaRequest(std::string &&uri,
                  const std::string &query,
                  const std::string &secret,
                  const std::string &akid,
                  const std::string &payload,
                  const std::string &region,
                  std::map<std::string, std::string> &&headers,
                  bool post = true);
};

class LambdaInvocation : public LambdaRequest {
  public:
    enum class InvocationType { Event, RequestResponse, DryRun };

    LambdaInvocation(const std::string &secret,
                     const std::string &akid,
                     const std::string &fn_name,
                     const std::string &payload = "{}",
                     const std::string &qualifier = "",
                     const InvocationType type = InvocationType::RequestResponse,
                     const std::string &region = "us-east-1");
};

class LambdaListFunctions : public LambdaRequest {
  public:
    LambdaListFunctions(const std::string &secret,
                        const std::string &akid,
                        const std::string &region = "us-east-1");
};

class LambdaListFnVersions : public LambdaRequest {
  public:
    LambdaListFnVersions(const std::string &secret,
                         const std::string &akid,
                         const std::string &fn_name,
                         const std::string &region = "us-east-1");
};
