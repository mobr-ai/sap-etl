#include "load/qlever_loader.hpp"

#include <sstream>
#include <stdexcept>

namespace sapetl {
namespace {

std::string triples_to_update(const std::vector<Triple>& triples) {
    std::ostringstream update;
    update
        << "PREFIX so: <https://mobr.ai/solana/ontology#>\n"
        << "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        << "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
        << "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"
        << "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
        << "PREFIX dct: <http://purl.org/dc/terms/>\n"
        << "PREFIX prov: <http://www.w3.org/ns/prov#>\n"
        << "INSERT DATA {\n";
    for (const auto& triple : triples) {
        update << "  " << triple.subject << " " << triple.predicate << " " << triple.object << " .\n";
    }
    update << "}\n";
    return update.str();
}

} // namespace

QleverLoader::QleverLoader(std::string update_url, const std::size_t max_payload_bytes)
    : update_url_(std::move(update_url)), max_payload_bytes_(max_payload_bytes) {}

void QleverLoader::begin() {}

void QleverLoader::append(const std::vector<Triple>& triples) {
    for (const auto& triple : triples) {
        pending_.push_back(triple);
        if (triples_to_update(pending_).size() >= max_payload_bytes_) {
            flush();
        }
    }
}

void QleverLoader::finish() {
    flush();
}

void QleverLoader::flush() {
    if (pending_.empty()) {
        return;
    }
    const auto body = triples_to_update(pending_);
    const auto response = http_.post(
        update_url_,
        body,
        {
            {"Content-Type", "application/sparql-update; charset=utf-8"},
            {"Accept", "*/*"}
        });
    if (response.status_code < 200 || response.status_code >= 300) {
        throw std::runtime_error(
            "QLever update failed with HTTP " + std::to_string(response.status_code) +
            ": " + response.body);
    }
    pending_.clear();
}

} // namespace sapetl
