#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace sapetl {

enum class LoadType {
    Ttl,
    Qlever
};

enum class ScopeType {
    Slots,
    Epochs
};

struct Range {
    std::uint64_t begin{};
    std::uint64_t end{}; // inclusive
};

struct EpochScheduleConfig {
    bool enabled{false};
    std::uint64_t slots_per_epoch{0};
    std::uint64_t first_normal_epoch{0};
    std::uint64_t first_normal_slot{0};
    bool warmup{true};
    std::uint64_t minimum_slots_per_epoch{32};
};

struct ProgramInvocationRow {
    std::uint64_t slot{};
    std::uint64_t timestamp{};
    std::string program_id_hex;
    bool is_vote{};
    std::uint32_t count{};
    std::uint32_t error_count{};
    std::uint32_t min_cus{};
    std::uint32_t max_cus{};
    std::uint32_t total_cus{};
};

struct SlotInstructionRow {
    std::uint64_t slot{};
    std::uint64_t timestamp{};
    std::uint64_t vote_instruction_count{};
    std::uint64_t non_vote_instruction_count{};
    std::uint32_t vote_transaction_count{};
    std::uint32_t non_vote_transaction_count{};
};

struct SlotStatusRow {
    std::uint64_t slot{};
    std::optional<std::uint64_t> block_time;
    std::optional<std::string> slot_status;
    std::optional<std::uint64_t> block_height;
    std::optional<std::string> leader;
};

struct Triple {
    std::string subject;
    std::string predicate;
    std::string object;
};

struct AppConfig {
    LoadType load_type{LoadType::Ttl};
    ScopeType scope{ScopeType::Slots};
    std::vector<Range> ranges;
    std::vector<std::uint64_t> identifiers;
    EpochScheduleConfig epoch_schedule{};

    std::string clickhouse_url{"http://127.0.0.1:8123"};
    std::string clickhouse_database{"default"};
    std::string qlever_update_url{"http://127.0.0.1:7001"};
    std::string output_dir{"./out"};
    std::string output_prefix{"sap-etl"};
    std::string checkpoint_dir{"./checkpoints"};
    std::size_t ttl_max_bytes{50ULL * 1024ULL * 1024ULL * 1024ULL};
    std::size_t source_chunk_slots{10000};
    std::size_t loader_batch_triples{20000};
    std::size_t qlever_update_max_bytes{8ULL * 1024ULL * 1024ULL};
    std::size_t worker_threads{0};
    bool resume{true};
    bool overwrite{false};
};

} // namespace sapetl
