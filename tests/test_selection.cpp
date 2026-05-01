#include "test_framework.hpp"
#include "model/selection.hpp"

TEST(selection_merges_ranges) {
    const auto merged = sapetl::parse_range_list("1-3,4,10-12");
    REQUIRE(merged.size() == 2);
    REQUIRE(merged[0].begin == 1);
    REQUIRE(merged[0].end == 4);
}

TEST(selection_builds_sql_predicate) {
    const auto ranges = sapetl::parse_range_list("1-2,4");
    const auto predicate = sapetl::ranges_to_sql_predicate(ranges, "slot");
    REQUIRE(predicate.find("slot BETWEEN 1 AND 2") != std::string::npos);
    REQUIRE(predicate.find("slot = 4") != std::string::npos);
}
