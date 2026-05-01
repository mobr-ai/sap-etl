#include "checkpoint/checkpoint_store.hpp"

#include "util/filesystem.hpp"

#include <filesystem>
#include <optional>
#include <stdexcept>

namespace sapetl {

CheckpointStore::CheckpointStore(std::filesystem::path root) : root_(std::move(root)) {
    ensure_directory(root_);
}

std::filesystem::path CheckpointStore::path_for(const std::string& phase) const {
    return root_ / (phase + ".checkpoint");
}

std::optional<std::uint64_t> CheckpointStore::load_slot(const std::string& phase) const {
    const auto path = path_for(phase);
    if (!std::filesystem::exists(path)) {
        return std::nullopt;
    }
    const auto raw = read_text_file(path);
    std::size_t consumed = 0;
    const auto value = std::stoull(raw, &consumed, 10);
    return value;
}

void CheckpointStore::save_slot(const std::string& phase, const std::uint64_t slot) const {
    write_text_file(path_for(phase), std::to_string(slot));
}

void CheckpointStore::clear() const {
    std::error_code ec;
    std::filesystem::remove_all(root_, ec);
    if (ec) {
        throw std::runtime_error("failed to clear checkpoints: " + ec.message());
    }
    ensure_directory(root_);
}

} // namespace sapetl
