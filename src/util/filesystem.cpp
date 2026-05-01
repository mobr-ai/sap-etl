#include "util/filesystem.hpp"

#include <fstream>
#include <sstream>
#include <stdexcept>

namespace sapetl {

void ensure_directory(const std::filesystem::path& path) {
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    if (ec) {
        throw std::runtime_error("failed to create directory '" + path.string() + "': " + ec.message());
    }
}

std::string read_text_file(const std::filesystem::path& path) {
    std::ifstream input(path);
    if (!input) {
        throw std::runtime_error("failed to open file for reading: " + path.string());
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

void write_text_file(const std::filesystem::path& path, const std::string& content) {
    std::ofstream output(path, std::ios::trunc);
    if (!output) {
        throw std::runtime_error("failed to open file for writing: " + path.string());
    }
    output << content;
    if (!output.good()) {
        throw std::runtime_error("failed to write file: " + path.string());
    }
}

} // namespace sapetl
