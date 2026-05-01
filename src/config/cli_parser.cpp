#include "config/cli_parser.hpp"

#include "model/selection.hpp"
#include "util/strings.hpp"

#include <stdexcept>
#include <thread>
#include <unordered_map>

namespace sapetl {
namespace {

std::size_t parse_size(const std::string& raw) {
    const auto value = trim(raw);
    if (value.empty()) {
        throw std::invalid_argument("empty size value");
    }
    std::size_t idx = 0;
    const auto number = std::stoull(value, &idx, 10);
    const auto unit = to_lower(trim(value.substr(idx)));
    if (unit.empty() || unit == "b") {
        return number;
    }
    if (unit == "kb") {
        return number * 1000ULL;
    }
    if (unit == "mb") {
        return number * 1000ULL * 1000ULL;
    }
    if (unit == "gb") {
        return number * 1000ULL * 1000ULL * 1000ULL;
    }
    if (unit == "kib") {
        return number * 1024ULL;
    }
    if (unit == "mib") {
        return number * 1024ULL * 1024ULL;
    }
    if (unit == "gib") {
        return number * 1024ULL * 1024ULL * 1024ULL;
    }
    throw std::invalid_argument("unsupported size suffix: " + raw);
}

bool parse_bool(const std::string& raw) {
    const auto value = to_lower(trim(raw));
    return value == "1" || value == "true" || value == "yes" || value == "on";
}

std::uint64_t parse_u64(const std::string& raw) {
    std::size_t consumed = 0;
    const auto value = std::stoull(raw, &consumed, 10);
    if (consumed != raw.size()) {
        throw std::invalid_argument("invalid integer: " + raw);
    }
    return value;
}

} // namespace

AppConfig parse_cli_args(const std::vector<std::string>& args) {
    AppConfig config;
    config.worker_threads = std::max<std::size_t>(1, std::thread::hardware_concurrency());

    std::unordered_map<std::string, std::string> options;
    for (std::size_t i = 0; i < args.size(); ++i) {
        const auto& token = args[i];
        if (!token.starts_with("--")) {
            throw std::invalid_argument("unexpected positional argument: " + token);
        }
        const auto key = token.substr(2);
        if (key == "help") {
            throw std::invalid_argument(usage_text());
        }
        if (i + 1 >= args.size()) {
            throw std::invalid_argument("missing value for argument: " + token);
        }
        options[key] = args[++i];
    }

    if (const auto it = options.find("load-type"); it != options.end()) {
        const auto value = to_lower(it->second);
        if (value == "ttl") {
            config.load_type = LoadType::Ttl;
        } else if (value == "qlever") {
            config.load_type = LoadType::Qlever;
        } else {
            throw std::invalid_argument("unsupported load type: " + it->second);
        }
    }

    if (const auto it = options.find("scope"); it != options.end()) {
        const auto value = to_lower(it->second);
        if (value == "slots") {
            config.scope = ScopeType::Slots;
        } else if (value == "epochs") {
            config.scope = ScopeType::Epochs;
        } else {
            throw std::invalid_argument("unsupported scope: " + it->second);
        }
    }

    if (const auto it = options.find("clickhouse-url"); it != options.end()) {
        config.clickhouse_url = it->second;
    }
    if (const auto it = options.find("clickhouse-database"); it != options.end()) {
        config.clickhouse_database = it->second;
    }
    if (const auto it = options.find("qlever-update-url"); it != options.end()) {
        config.qlever_update_url = it->second;
    }
    if (const auto it = options.find("output-dir"); it != options.end()) {
        config.output_dir = it->second;
    }
    if (const auto it = options.find("output-prefix"); it != options.end()) {
        config.output_prefix = it->second;
    }
    if (const auto it = options.find("checkpoint-dir"); it != options.end()) {
        config.checkpoint_dir = it->second;
    }
    if (const auto it = options.find("ttl-max-size"); it != options.end()) {
        config.ttl_max_bytes = parse_size(it->second);
    }
    if (const auto it = options.find("source-chunk-slots"); it != options.end()) {
        config.source_chunk_slots = parse_u64(it->second);
    }
    if (const auto it = options.find("loader-batch-triples"); it != options.end()) {
        config.loader_batch_triples = parse_u64(it->second);
    }
    if (const auto it = options.find("qlever-update-max-size"); it != options.end()) {
        config.qlever_update_max_bytes = parse_size(it->second);
    }
    if (const auto it = options.find("worker-threads"); it != options.end()) {
        config.worker_threads = parse_u64(it->second);
    }
    if (const auto it = options.find("resume"); it != options.end()) {
        config.resume = parse_bool(it->second);
    }
    if (const auto it = options.find("overwrite"); it != options.end()) {
        config.overwrite = parse_bool(it->second);
    }

    if (const auto it = options.find("slots-per-epoch"); it != options.end()) {
        config.epoch_schedule.enabled = true;
        config.epoch_schedule.slots_per_epoch = parse_u64(it->second);
    }
    if (const auto it = options.find("first-normal-epoch"); it != options.end()) {
        config.epoch_schedule.enabled = true;
        config.epoch_schedule.first_normal_epoch = parse_u64(it->second);
    }
    if (const auto it = options.find("first-normal-slot"); it != options.end()) {
        config.epoch_schedule.enabled = true;
        config.epoch_schedule.first_normal_slot = parse_u64(it->second);
    }
    if (const auto it = options.find("warmup"); it != options.end()) {
        config.epoch_schedule.enabled = true;
        config.epoch_schedule.warmup = parse_bool(it->second);
    }
    if (const auto it = options.find("minimum-slots-per-epoch"); it != options.end()) {
        config.epoch_schedule.enabled = true;
        config.epoch_schedule.minimum_slots_per_epoch = parse_u64(it->second);
    }

    if (config.scope == ScopeType::Slots) {
        const auto found = options.find("slots");
        if (found == options.end()) {
            throw std::invalid_argument("missing required --slots option");
        }
        config.ranges = parse_range_list(found->second);
    } else {
        const auto found = options.find("epochs");
        if (found == options.end()) {
            throw std::invalid_argument("missing required --epochs option");
        }
        config.identifiers = parse_identifier_list(found->second);
        if (!config.epoch_schedule.enabled) {
            throw std::invalid_argument(
                "epoch scope requires an explicit epoch schedule "
                "(--slots-per-epoch, --first-normal-epoch, --first-normal-slot, --warmup)");
        }
        config.ranges = expand_epochs_to_slots(config.identifiers, config.epoch_schedule);
    }

    if (config.ttl_max_bytes == 0) {
        throw std::invalid_argument("ttl max size must be > 0");
    }
    if (config.source_chunk_slots == 0) {
        throw std::invalid_argument("source chunk size must be > 0");
    }
    if (config.loader_batch_triples == 0) {
        throw std::invalid_argument("loader batch size must be > 0");
    }
    if (config.worker_threads == 0) {
        config.worker_threads = 1;
    }

    return config;
}

std::string usage_text() {
    return
        "Usage:\n"
        "  sap-etl --scope slots --slots 358560000-358560010[,358560100] [options]\n"
        "  sap-etl --scope epochs --epochs 800,801 --slots-per-epoch ... [options]\n\n"
        "Options:\n"
        "  --load-type ttl|qlever                Output mode (default: ttl)\n"
        "  --scope slots|epochs                  Input selection scope (default: slots)\n"
        "  --slots LIST                          Slot list/ranges, e.g. 1,2,10-20\n"
        "  --epochs LIST                         Epoch list, e.g. 800,801\n"
        "  --clickhouse-url URL                  ClickHouse HTTP endpoint\n"
        "  --clickhouse-database NAME            ClickHouse database (default: default)\n"
        "  --qlever-update-url URL               QLever SPARQL update endpoint\n"
        "  --output-dir PATH                     Output directory for TTL files\n"
        "  --output-prefix NAME                  Prefix for TTL parts\n"
        "  --checkpoint-dir PATH                 Checkpoint directory\n"
        "  --ttl-max-size SIZE                   Max size per TTL file (default: 50GiB)\n"
        "  --source-chunk-slots N                Slot chunk size per source query\n"
        "  --loader-batch-triples N              Flush triples in batches\n"
        "  --qlever-update-max-size SIZE         Max SPARQL update payload size\n"
        "  --worker-threads N                    Transformation worker threads\n"
        "  --resume true|false                   Resume from checkpoints (default: true)\n"
        "  --overwrite true|false                Overwrite existing TTL outputs (default: false)\n"
        "  --slots-per-epoch N                   Epoch schedule: slots per normal epoch\n"
        "  --first-normal-epoch N                Epoch schedule: first normal epoch\n"
        "  --first-normal-slot N                 Epoch schedule: first normal slot\n"
        "  --warmup true|false                   Epoch schedule warmup flag\n"
        "  --minimum-slots-per-epoch N           Warmup minimum slots per epoch\n";
}

} // namespace sapetl
