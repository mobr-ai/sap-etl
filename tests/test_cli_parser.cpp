#include "test_framework.hpp"
#include "config/cli_parser.hpp"
#include "model/types.hpp"

#include <vector>
#include <string>

TEST(cli_parser_defaults_to_ttl_slots) {
    const auto config = sapetl::parse_cli_args({"--scope", "slots", "--slots", "1-3,5"});
    REQUIRE(config.load_type == sapetl::LoadType::Ttl);
    REQUIRE(config.scope == sapetl::ScopeType::Slots);
    REQUIRE(config.ranges.size() == 2);
    REQUIRE(config.ranges[0].begin == 1);
    REQUIRE(config.ranges[0].end == 3);
}

TEST(cli_parser_expands_epochs) {
    const auto config = sapetl::parse_cli_args({
        "--scope", "epochs",
        "--epochs", "0,1",
        "--slots-per-epoch", "8192",
        "--first-normal-epoch", "8",
        "--first-normal-slot", "8160",
        "--warmup", "true"
    });
    REQUIRE(config.scope == sapetl::ScopeType::Epochs);
    REQUIRE(!config.ranges.empty());
    REQUIRE(config.ranges[0].begin == 0);
}
