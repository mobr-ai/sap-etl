#pragma once

#include <filesystem>
#include <optional>
#include <string>

namespace sapetl {

class CheckpointStore {
  public:
    explicit CheckpointStore(std::filesystem::path root);

    std::optional<std::uint64_t> load_slot(const std::string& phase) const;
    void save_slot(const std::string& phase, std::uint64_t slot) const;
    void clear() const;

  private:
    std::filesystem::path path_for(const std::string& phase) const;

    std::filesystem::path root_;
};

} // namespace sapetl
