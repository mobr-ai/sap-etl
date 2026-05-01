#pragma once

#include "model/types.hpp"

#include <vector>
#include <string>

namespace sapetl {

AppConfig parse_cli_args(const std::vector<std::string>& args);
std::string usage_text();

} // namespace sapetl
