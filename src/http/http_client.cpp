#include "http/http_client.hpp"

#include <curl/curl.h>

#include <memory>
#include <stdexcept>

namespace sapetl {
namespace {

size_t write_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* output = static_cast<std::string*>(userdata);
    output->append(ptr, size * nmemb);
    return size * nmemb;
}

HttpResponse perform_request(
    const std::string& url,
    const char* method,
    const std::string* body,
    const std::map<std::string, std::string>& headers) {

    std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl(curl_easy_init(), &curl_easy_cleanup);
    if (!curl) {
        throw std::runtime_error("curl_easy_init failed");
    }

    std::string response_body;
    curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl.get(), CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(curl.get(), CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, &write_callback);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &response_body);
    curl_easy_setopt(curl.get(), CURLOPT_USERAGENT, "sap-etl/0.1.0");
    curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT, 0L);

    struct curl_slist* header_list = nullptr;
    for (const auto& [key, value] : headers) {
        const auto full = key + ": " + value;
        header_list = curl_slist_append(header_list, full.c_str());
    }
    std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> scoped_headers(header_list, &curl_slist_free_all);
    if (header_list != nullptr) {
        curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, header_list);
    }

    if (body != nullptr) {
        curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDS, body->c_str());
        curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(body->size()));
    }

    const auto rc = curl_easy_perform(curl.get());
    if (rc != CURLE_OK) {
        throw std::runtime_error(std::string("HTTP request failed: ") + curl_easy_strerror(rc));
    }

    long status_code = 0;
    curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &status_code);
    return {status_code, std::move(response_body)};
}

} // namespace

HttpClient::HttpClient() {
    curl_global_init(CURL_GLOBAL_DEFAULT);
}

HttpClient::~HttpClient() {
    curl_global_cleanup();
}

HttpResponse HttpClient::get(
    const std::string& url,
    const std::map<std::string, std::string>& headers) const {
    return perform_request(url, "GET", nullptr, headers);
}

HttpResponse HttpClient::post(
    const std::string& url,
    const std::string& body,
    const std::map<std::string, std::string>& headers) const {
    return perform_request(url, "POST", &body, headers);
}

} // namespace sapetl
