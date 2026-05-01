#pragma once

#include <map>
#include <string>

namespace sapetl {

struct HttpResponse {
    long status_code{};
    std::string body;
};

class HttpClient {
  public:
    HttpClient();
    ~HttpClient();

    HttpResponse get(
        const std::string& url,
        const std::map<std::string, std::string>& headers = {}) const;

    HttpResponse post(
        const std::string& url,
        const std::string& body,
        const std::map<std::string, std::string>& headers = {}) const;
};

} // namespace sapetl
