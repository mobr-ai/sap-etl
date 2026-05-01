#pragma once

#include "load/loader.hpp"

#include <filesystem>
#include <fstream>
#include <string>

namespace sapetl {

class TtlLoader final : public Loader {
  public:
    TtlLoader(
        std::filesystem::path output_dir,
        std::string prefix,
        std::size_t max_bytes_per_file,
        bool overwrite);

    void begin() override;
    void append(const std::vector<Triple>& triples) override;
    void finish() override;

  private:
    void open_next_file();
    void write_prefixes();
    void write_line(const std::string& line);

    std::filesystem::path output_dir_;
    std::string prefix_;
    std::size_t max_bytes_per_file_;
    bool overwrite_;
    std::size_t current_bytes_{0};
    std::size_t file_index_{0};
    std::ofstream current_;
};

} // namespace sapetl
