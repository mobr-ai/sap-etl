#include "load/ttl_loader.hpp"

#include "util/filesystem.hpp"

#include <filesystem>
#include <stdexcept>

namespace sapetl {
namespace {

std::string triple_to_ttl(const Triple& triple) {
    return triple.subject + " " + triple.predicate + " " + triple.object + " .\n";
}

} // namespace

TtlLoader::TtlLoader(
    std::filesystem::path output_dir,
    std::string prefix,
    const std::size_t max_bytes_per_file,
    const bool overwrite)
    : output_dir_(std::move(output_dir)),
      prefix_(std::move(prefix)),
      max_bytes_per_file_(max_bytes_per_file),
      overwrite_(overwrite) {}

void TtlLoader::begin() {
    ensure_directory(output_dir_);
    if (overwrite_) {
        for (const auto& entry : std::filesystem::directory_iterator(output_dir_)) {
            if (entry.path().filename().string().starts_with(prefix_) && entry.path().extension() == ".ttl") {
                std::filesystem::remove(entry.path());
            }
        }
    }
    open_next_file();
}

void TtlLoader::append(const std::vector<Triple>& triples) {
    for (const auto& triple : triples) {
        const auto line = triple_to_ttl(triple);
        if (current_bytes_ + line.size() > max_bytes_per_file_ && current_bytes_ > 0) {
            open_next_file();
        }
        write_line(line);
    }
}

void TtlLoader::finish() {
    if (current_.is_open()) {
        current_.flush();
        current_.close();
    }
}

void TtlLoader::open_next_file() {
    if (current_.is_open()) {
        current_.flush();
        current_.close();
    }
    const auto path = output_dir_ / (prefix_ + "-" + std::to_string(file_index_++) + ".ttl");
    current_.open(path, std::ios::trunc);
    if (!current_) {
        throw std::runtime_error("failed to open TTL output: " + path.string());
    }
    current_bytes_ = 0;
    write_prefixes();
}

void TtlLoader::write_prefixes() {
    write_line("@prefix so:    <https://mobr.ai/solana/ontology#> .\n");
    write_line("@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n");
    write_line("@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .\n");
    write_line("@prefix owl:   <http://www.w3.org/2002/07/owl#> .\n");
    write_line("@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .\n");
    write_line("@prefix dct:   <http://purl.org/dc/terms/> .\n");
    write_line("@prefix prov:  <http://www.w3.org/ns/prov#> .\n\n");
}

void TtlLoader::write_line(const std::string& line) {
    current_ << line;
    if (!current_.good()) {
        throw std::runtime_error("failed to write TTL output");
    }
    current_bytes_ += line.size();
}

} // namespace sapetl
