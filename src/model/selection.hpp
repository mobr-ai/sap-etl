#pragma once

#include "model/types.hpp"

#include <string>
#include <vector>

namespace sapetl {

std::vector<Range> parse_range_list(const std::string& value);
std::vector<std::uint64_t> parse_identifier_list(const std::string& value);
std::vector<Range> expand_epochs_to_slots(
    const std::vector<std::uint64_t>& epochs,
    const EpochScheduleConfig& schedule);

std::string ranges_to_sql_predicate(const std::vector<Range>& ranges, const std::string& column_name);
std::vector<Range> merge_ranges(std::vector<Range> ranges);
std::vector<Range> split_ranges_by_size(const std::vector<Range>& ranges, std::uint64_t chunk_size);

} // namespace sapetl
