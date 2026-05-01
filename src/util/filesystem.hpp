#pragma once

#include <filesystem>
#include <string>

namespace sapetl {

void ensure_directory(const std::filesystem::path& path);
std::string read_text_file(const std::filesystem::path& path);
void write_text_file(const std::filesystem::path& path, const std::string& content);

} // namespace sapetl
