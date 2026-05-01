#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace sapetl {

std::string trim(std::string_view input);
std::vector<std::string> split(std::string_view input, char delimiter);
std::string to_lower(std::string value);
std::string escape_turtle_string(const std::string& value);
std::string url_encode(const std::string& value);

} // namespace sapetl
