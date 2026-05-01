#include "clickhouse/clickhouse_client.hpp"

#include "util/strings.hpp"

#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <unordered_set>

namespace sapetl {
namespace {

std::vector<std::vector<std::string>> parse_tsv(const std::string& raw) {
    std::vector<std::vector<std::string>> rows;
    std::istringstream input(raw);
    std::string line;
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        rows.push_back(split(line, '\t'));
    }
    return rows;
}

} // namespace

ClickHouseClient::ClickHouseClient(std::string base_url, std::string database)
    : base_url_(std::move(base_url)), database_(std::move(database)) {}

std::vector<std::vector<std::string>> ClickHouseClient::query_tsv(const std::string& sql) const {
    const std::string url =
        base_url_ + "/?database=" + url_encode(database_) + "&default_format=TabSeparatedRaw";
    const auto response = http_.post(
        url,
        sql,
        {
            {"Content-Type", "text/plain; charset=utf-8"},
            {"Accept", "text/tab-separated-values"}
        });

    if (response.status_code < 200 || response.status_code >= 300) {
        throw std::runtime_error(
            "ClickHouse query failed with HTTP " + std::to_string(response.status_code) +
            ": " + response.body);
    }
    return parse_tsv(response.body);
}

bool ClickHouseClient::table_exists(const std::string& table_name) const {
    const auto rows = query_tsv(
        "SELECT count() FROM system.tables "
        "WHERE database = '" + database_ + "' AND name = '" + table_name + "'");
    if (rows.empty() || rows.front().empty()) {
        return false;
    }
    return rows.front().front() != "0";
}

std::vector<std::string> ClickHouseClient::table_columns(const std::string& table_name) const {
    const auto rows = query_tsv(
        "SELECT name FROM system.columns "
        "WHERE database = '" + database_ + "' AND table = '" + table_name + "' ORDER BY position");
    std::vector<std::string> columns;
    columns.reserve(rows.size());
    for (const auto& row : rows) {
        if (!row.empty()) {
            columns.push_back(row.front());
        }
    }
    return columns;
}

} // namespace sapetl
