#pragma once
#include <ctime>
#include <map>
#include <string>

class LambdaRequest {
  private:
    std::string request_date_;
    std::string request_uri_;
    std::string query_string_;
    std::string secret_;
    std::string akid_;
    std::string payload_;
    std::map<std::string, std::string> headers_;

    static std::string x_amz_date_(const std::time_t &t);

    static constexpr char BASEURI_[] = "/2015-03-31/functions/";
    static constexpr char INVOKE_[] = "/invocations";
    static constexpr char REGION_[] = "us-east-1";
    static constexpr char SERVICE_[] = "lambda";
    static constexpr char HOST_[] = "lambda.us-east-1.amazonaws.com";

  public:
    enum class InvocationType { Event, RequestResponse, DryRun };

    LambdaRequest(const std::string fn_name,
                  const std::string secret,
                  const std::string akid,
                  const std::string payload = "{}",
                  const std::string qualifier = "",
                  const InvocationType type = InvocationType::RequestResponse);
};
