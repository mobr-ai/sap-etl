#include "source/clickhouse_jetstreamer_source.hpp"

#include "util/strings.hpp"

#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <unordered_set>

namespace sapetl {
namespace {

std::uint64_t parse_u64(const std::string& raw) {
    return raw.empty() ? 0ULL : std::stoull(raw);
}

std::uint32_t parse_u32(const std::string& raw) {
    return raw.empty() ? 0U : static_cast<std::uint32_t>(std::stoul(raw));
}

bool parse_bool(const std::string& raw) {
    return raw == "1" || to_lower(raw) == "true";
}

std::optional<std::uint64_t> parse_optional_u64(const std::string& raw) {
    if (raw.empty() || raw == "\\N") {
        return std::nullopt;
    }
    return std::stoull(raw);
}

std::optional<std::string> parse_optional_string(const std::string& raw) {
    if (raw.empty() || raw == "\\N") {
        return std::nullopt;
    }
    return raw;
}

std::string range_filter(const Range& slots) {
    return "(slot BETWEEN " + std::to_string(slots.begin) + " AND " + std::to_string(slots.end) + ")";
}

} // namespace

ClickHouseJetstreamerSource::ClickHouseJetstreamerSource(ClickHouseClient client)
    : client_(std::move(client)) {}

bool ClickHouseJetstreamerSource::has_table(const std::string& name) const {
    return client_.table_exists(name);
}

bool ClickHouseJetstreamerSource::table_has_column(const std::string& table, const std::string& column) const {
    const auto columns = client_.table_columns(table);
    return std::find(columns.begin(), columns.end(), column) != columns.end();
}

std::vector<ProgramInvocationRow> ClickHouseJetstreamerSource::fetch_program_invocations(const Range& slots) const {
    if (!has_table("program_invocations")) {
        return {};
    }

    const auto rows = client_.query_tsv(
        "SELECT slot, toUnixTimestamp(timestamp), hex(program_id), is_vote, count, "
        "error_count, min_cus, max_cus, total_cus "
        "FROM program_invocations WHERE " + range_filter(slots) + " ORDER BY slot, program_id, is_vote");

    std::vector<ProgramInvocationRow> out;
    out.reserve(rows.size());
    for (const auto& row : rows) {
        if (row.size() != 9) {
            throw std::runtime_error("unexpected program_invocations row width");
        }
        out.push_back({
            parse_u64(row[0]),
            parse_u64(row[1]),
            row[2],
            parse_bool(row[3]),
            parse_u32(row[4]),
            parse_u32(row[5]),
            parse_u32(row[6]),
            parse_u32(row[7]),
            parse_u32(row[8]),
        });
    }
    return out;
}

std::vector<SlotInstructionRow> ClickHouseJetstreamerSource::fetch_slot_instructions(const Range& slots) const {
    if (!has_table("slot_instructions")) {
        return {};
    }

    const auto rows = client_.query_tsv(
        "SELECT slot, toUnixTimestamp(timestamp), vote_instruction_count, non_vote_instruction_count, "
        "vote_transaction_count, non_vote_transaction_count "
        "FROM slot_instructions WHERE " + range_filter(slots) + " ORDER BY slot");

    std::vector<SlotInstructionRow> out;
    out.reserve(rows.size());
    for (const auto& row : rows) {
        if (row.size() != 6) {
            throw std::runtime_error("unexpected slot_instructions row width");
        }
        out.push_back({
            parse_u64(row[0]),
            parse_u64(row[1]),
            parse_u64(row[2]),
            parse_u64(row[3]),
            parse_u32(row[4]),
            parse_u32(row[5]),
        });
    }
    return out;
}

std::vector<SlotStatusRow> ClickHouseJetstreamerSource::fetch_slot_statuses(const Range& slots) const {
    if (!has_table("jetstreamer_slot_status")) {
        return {};
    }

    std::vector<std::string> selects{"slot"};
    const auto has_block_time = table_has_column("jetstreamer_slot_status", "block_time");
    const auto has_slot_status = table_has_column("jetstreamer_slot_status", "slot_status");
    const auto has_status = table_has_column("jetstreamer_slot_status", "status");
    const auto has_block_height = table_has_column("jetstreamer_slot_status", "block_height");
    const auto has_leader = table_has_column("jetstreamer_slot_status", "leader");

    selects.push_back(has_block_time ? "toUnixTimestamp(block_time)" : "NULL");
    if (has_slot_status) {
        selects.push_back("slot_status");
    } else if (has_status) {
        selects.push_back("status");
    } else {
        selects.push_back("NULL");
    }
    selects.push_back(has_block_height ? "block_height" : "NULL");
    selects.push_back(has_leader ? "leader" : "NULL");

    std::ostringstream sql;
    sql << "SELECT ";
    for (std::size_t i = 0; i < selects.size(); ++i) {
        if (i != 0) {
            sql << ", ";
        }
        sql << selects[i];
    }
    sql << " FROM jetstreamer_slot_status WHERE " << range_filter(slots) << " ORDER BY slot";

    const auto rows = client_.query_tsv(sql.str());

    std::vector<SlotStatusRow> out;
    out.reserve(rows.size());
    for (const auto& row : rows) {
        if (row.size() != 5) {
            throw std::runtime_error("unexpected jetstreamer_slot_status row width");
        }
        out.push_back({
            parse_u64(row[0]),
            parse_optional_u64(row[1]),
            parse_optional_string(row[2]),
            parse_optional_u64(row[3]),
            parse_optional_string(row[4]),
        });
    }
    return out;
}

} // namespace sapetl
