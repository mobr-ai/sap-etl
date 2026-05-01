#include "app/application.hpp"

#include "model/selection.hpp"

#include <algorithm>
#include <future>
#include <iostream>
#include <mutex>
#include <thread>

namespace sapetl {
namespace {

template <typename Row, typename MapperFn>
std::vector<Triple> map_rows_parallel(
    const std::vector<Row>& rows,
    const std::size_t thread_count,
    MapperFn&& mapper) {

    if (rows.empty()) {
        return {};
    }

    const auto workers = std::max<std::size_t>(1, std::min(thread_count, rows.size()));
    const auto chunk = (rows.size() + workers - 1) / workers;

    std::vector<std::future<std::vector<Triple>>> futures;
    futures.reserve(workers);

    for (std::size_t worker = 0; worker < workers; ++worker) {
        const auto begin = worker * chunk;
        if (begin >= rows.size()) {
            break;
        }
        const auto end = std::min(rows.size(), begin + chunk);
        futures.push_back(std::async(std::launch::async, [begin, end, &rows, &mapper]() {
            thread_local std::vector<Triple> buffer;
            buffer.clear();
            for (std::size_t i = begin; i < end; ++i) {
                auto triples = mapper(rows[i]);
                buffer.insert(buffer.end(), triples.begin(), triples.end());
            }
            return buffer;
        }));
    }

    std::vector<Triple> merged;
    for (auto& future : futures) {
        auto part = future.get();
        merged.insert(merged.end(),
                      std::make_move_iterator(part.begin()),
                      std::make_move_iterator(part.end()));
    }
    return merged;
}

} // namespace

Application::Application(
    AppConfig config,
    std::unique_ptr<JetstreamerSource> source,
    std::unique_ptr<Loader> loader)
    : config_(std::move(config)),
      source_(std::move(source)),
      loader_(std::move(loader)),
      checkpoints_(config_.checkpoint_dir) {}

int Application::run() {
    if (!config_.resume) {
        checkpoints_.clear();
    }

    loader_->begin();

    const auto chunks = split_ranges_by_size(config_.ranges, config_.source_chunk_slots);

    for (const auto& chunk : chunks) {
        auto statuses = source_->fetch_slot_statuses(chunk);
        auto status_triples = map_rows_parallel(statuses, config_.worker_threads, [this](const SlotStatusRow& row) {
            return mapper_.map_slot_status(row);
        });
        loader_->append(status_triples);

        auto programs = source_->fetch_program_invocations(chunk);
        auto program_triples =
            map_rows_parallel(programs, config_.worker_threads, [this](const ProgramInvocationRow& row) {
                return mapper_.map_program_invocation(row);
            });
        loader_->append(program_triples);

        auto instructions = source_->fetch_slot_instructions(chunk);
        auto instruction_triples =
            map_rows_parallel(instructions, config_.worker_threads, [this](const SlotInstructionRow& row) {
                return mapper_.map_slot_instruction(row);
            });
        loader_->append(instruction_triples);

        checkpoints_.save_slot("global", chunk.end);
        std::cout << "Processed slots " << chunk.begin << "-" << chunk.end << '\n';
    }

    loader_->finish();
    return 0;
}

} // namespace sapetl
