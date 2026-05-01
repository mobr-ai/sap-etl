#include "model/selection.hpp"

#include "util/strings.hpp"

#include <algorithm>
#include <limits>
#include <sstream>
#include <stdexcept>

namespace sapetl {
namespace {

std::uint64_t parse_uint64(const std::string& token) {
    if (token.empty()) {
        throw std::invalid_argument("empty numeric token");
    }
    std::size_t consumed = 0;
    const auto value = std::stoull(token, &consumed, 10);
    if (consumed != token.size()) {
        throw std::invalid_argument("invalid numeric token: " + token);
    }
    return value;
}

std::uint64_t pow2_u64(std::uint64_t n) {
    if (n >= 63) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    return 1ULL << n;
}

std::pair<std::uint64_t, std::uint64_t> epoch_bounds(
    const std::uint64_t epoch,
    const EpochScheduleConfig& schedule) {

    if (!schedule.enabled) {
        throw std::invalid_argument("epoch schedule is not enabled");
    }
    if (schedule.slots_per_epoch == 0) {
        throw std::invalid_argument("slots_per_epoch must be > 0");
    }

    if (!schedule.warmup || epoch >= schedule.first_normal_epoch) {
        const auto start = schedule.first_normal_slot +
                           (epoch - schedule.first_normal_epoch) * schedule.slots_per_epoch;
        return {start, start + schedule.slots_per_epoch - 1};
    }

    std::uint64_t start = 0;
    for (std::uint64_t e = 0; e < epoch; ++e) {
        start += schedule.minimum_slots_per_epoch * pow2_u64(e);
    }
    const auto length = schedule.minimum_slots_per_epoch * pow2_u64(epoch);
    return {start, start + length - 1};
}

} // namespace

std::vector<Range> parse_range_list(const std::string& value) {
    std::vector<Range> ranges;
    for (const auto& raw : split(value, ',')) {
        const auto token = trim(raw);
        if (token.empty()) {
            continue;
        }
        const auto dash = token.find('-');
        if (dash == std::string::npos) {
            const auto slot = parse_uint64(token);
            ranges.push_back({slot, slot});
            continue;
        }
        const auto start = parse_uint64(trim(token.substr(0, dash)));
        const auto end = parse_uint64(trim(token.substr(dash + 1)));
        if (end < start) {
            throw std::invalid_argument("range end is smaller than start: " + token);
        }
        ranges.push_back({start, end});
    }
    if (ranges.empty()) {
        throw std::invalid_argument("no slots were provided");
    }
    return merge_ranges(std::move(ranges));
}

std::vector<std::uint64_t> parse_identifier_list(const std::string& value) {
    std::vector<std::uint64_t> identifiers;
    for (const auto& raw : split(value, ',')) {
        const auto token = trim(raw);
        if (!token.empty()) {
            identifiers.push_back(parse_uint64(token));
        }
    }
    if (identifiers.empty()) {
        throw std::invalid_argument("no identifiers were provided");
    }
    return identifiers;
}

std::vector<Range> expand_epochs_to_slots(
    const std::vector<std::uint64_t>& epochs,
    const EpochScheduleConfig& schedule) {
    std::vector<Range> ranges;
    ranges.reserve(epochs.size());
    for (const auto epoch : epochs) {
        const auto [begin, end] = epoch_bounds(epoch, schedule);
        ranges.push_back({begin, end});
    }
    return merge_ranges(std::move(ranges));
}

std::string ranges_to_sql_predicate(const std::vector<Range>& ranges, const std::string& column_name) {
    if (ranges.empty()) {
        return "1 = 0";
    }
    std::ostringstream out;
    bool first = true;
    for (const auto& range : ranges) {
        if (!first) {
            out << " OR ";
        }
        first = false;
        if (range.begin == range.end) {
            out << "(" << column_name << " = " << range.begin << ")";
        } else {
            out << "(" << column_name << " BETWEEN " << range.begin << " AND " << range.end << ")";
        }
    }
    return out.str();
}

std::vector<Range> merge_ranges(std::vector<Range> ranges) {
    std::sort(ranges.begin(), ranges.end(), [](const Range& lhs, const Range& rhs) {
        return lhs.begin < rhs.begin || (lhs.begin == rhs.begin && lhs.end < rhs.end);
    });

    std::vector<Range> merged;
    for (const auto& range : ranges) {
        if (merged.empty() || range.begin > merged.back().end + 1) {
            merged.push_back(range);
        } else {
            merged.back().end = std::max(merged.back().end, range.end);
        }
    }
    return merged;
}

std::vector<Range> split_ranges_by_size(const std::vector<Range>& ranges, const std::uint64_t chunk_size) {
    if (chunk_size == 0) {
        throw std::invalid_argument("chunk_size must be > 0");
    }
    std::vector<Range> chunks;
    for (const auto& range : ranges) {
        std::uint64_t current = range.begin;
        while (current <= range.end) {
            const auto max_end = std::min(range.end, current + chunk_size - 1);
            chunks.push_back({current, max_end});
            if (max_end == std::numeric_limits<std::uint64_t>::max()) {
                break;
            }
            current = max_end + 1;
        }
    }
    return chunks;
}

} // namespace sapetl
