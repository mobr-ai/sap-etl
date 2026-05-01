#include "util/strings.hpp"

#include <cctype>
#include <iomanip>
#include <sstream>

namespace sapetl {

std::string trim(std::string_view input) {
    std::size_t start = 0;
    while (start < input.size() && std::isspace(static_cast<unsigned char>(input[start])) != 0) {
        ++start;
    }
    std::size_t end = input.size();
    while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1])) != 0) {
        --end;
    }
    return std::string(input.substr(start, end - start));
}

std::vector<std::string> split(std::string_view input, const char delimiter) {
    std::vector<std::string> parts;
    std::size_t start = 0;
    while (start <= input.size()) {
        const auto pos = input.find(delimiter, start);
        if (pos == std::string_view::npos) {
            parts.emplace_back(input.substr(start));
            break;
        }
        parts.emplace_back(input.substr(start, pos - start));
        start = pos + 1;
    }
    return parts;
}

std::string to_lower(std::string value) {
    for (auto& ch : value) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return value;
}

std::string escape_turtle_string(const std::string& value) {
    std::ostringstream out;
    for (const char ch : value) {
        switch (ch) {
            case '\\': out << "\\\\"; break;
            case '"': out << "\\\""; break;
            case '\n': out << "\\n"; break;
            case '\r': out << "\\r"; break;
            case '\t': out << "\\t"; break;
            default: out << ch; break;
        }
    }
    return out.str();
}

std::string url_encode(const std::string& value) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex << std::uppercase;
    for (const unsigned char ch : value) {
        if (std::isalnum(ch) || ch == '-' || ch == '_' || ch == '.' || ch == '~') {
            escaped << static_cast<char>(ch);
        } else {
            escaped << '%' << std::setw(2) << static_cast<int>(ch);
        }
    }
    return escaped.str();
}

} // namespace sapetl
