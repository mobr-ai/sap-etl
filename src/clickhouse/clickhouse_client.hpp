#pragma once

#include "http/http_client.hpp"

#include <string>
#include <vector>

namespace sapetl {

class ClickHouseClient {
  public:
    ClickHouseClient(std::string base_url, std::string database);

    std::vector<std::vector<std::string>> query_tsv(const std::string& sql) const;
    bool table_exists(const std::string& table_name) const;
    std::vector<std::string> table_columns(const std::string& table_name) const;

  private:
    std::string base_url_;
    std::string database_;
    HttpClient http_;
};

} // namespace sapetl
