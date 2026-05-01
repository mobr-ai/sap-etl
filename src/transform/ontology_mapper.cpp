#include "transform/ontology_mapper.hpp"

#include "util/strings.hpp"

namespace sapetl {
namespace {

const std::string kResourceBase = "https://mobr.ai/solana/resource/";
const std::string kSo = "https://mobr.ai/solana/ontology#";

Triple iri_triple(const std::string& s, const std::string& p, const std::string& o) {
    return {s, p, o};
}

Triple lit_triple(const std::string& s, const std::string& p, const std::string& o) {
    return {s, p, o};
}

} // namespace

std::vector<Triple> OntologyMapper::ontology_prefixes() const {
    return {};
}

std::string OntologyMapper::slot_uri(const std::uint64_t slot) {
    return "<" + kResourceBase + "slot/" + std::to_string(slot) + ">";
}

std::string OntologyMapper::block_uri(const std::uint64_t slot) {
    return "<" + kResourceBase + "block/" + std::to_string(slot) + ">";
}

std::string OntologyMapper::program_uri_from_hex(const std::string& hex) {
    return "<" + kResourceBase + "program/" + to_lower(hex) + ">";
}

std::string OntologyMapper::literal_integer(const std::uint64_t value) {
    return "\"" + std::to_string(value) + "\"^^xsd:decimal";
}

std::string OntologyMapper::literal_long(const std::uint64_t value) {
    return "\"" + std::to_string(value) + "\"^^xsd:decimal";
}

std::string OntologyMapper::literal_string(const std::string& value) {
    return "\"" + escape_turtle_string(value) + "\"";
}

std::string OntologyMapper::literal_bool(const bool value) {
    return value
        ? "\"true\"^^xsd:boolean"
        : "\"false\"^^xsd:boolean";
}

std::vector<Triple> OntologyMapper::map_program_invocation(const ProgramInvocationRow& row) const {
    std::vector<Triple> triples;
    const auto slot = slot_uri(row.slot);
    const auto block = block_uri(row.slot);
    const auto program = program_uri_from_hex(row.program_id_hex);

    triples.push_back(iri_triple(slot, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<" + kSo + "Slot>"));
    triples.push_back(lit_triple(slot, "<" + kSo + "slotNumber>", literal_integer(row.slot)));
    triples.push_back(lit_triple(slot, "<" + kSo + "slotStatus>", literal_string("Produced")));

    triples.push_back(iri_triple(block, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<" + kSo + "Block>"));
    triples.push_back(iri_triple(slot, "<" + kSo + "hasBlock>", block));
    triples.push_back(iri_triple(block, "<" + kSo + "occupiesSlot>", slot));
    if (row.timestamp != 0) {
        triples.push_back(lit_triple(block, "<" + kSo + "blockTimeUnix>", literal_long(row.timestamp)));
    }

    triples.push_back(iri_triple(program, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<" + kSo + "Program>"));
    triples.push_back(lit_triple(program, "<" + kSo + "ownerProgramId>", literal_string(row.program_id_hex)));

    const auto invocation = "<" + kResourceBase + "programInvocation/" + std::to_string(row.slot) + "/" +
                            to_lower(row.program_id_hex) + "/" + (row.is_vote ? "vote" : "non-vote") + ">";
    triples.push_back(iri_triple(invocation, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<" + kSo + "Instruction>"));
    triples.push_back(iri_triple(invocation, "<" + kSo + "invokesProgram>", program));
    triples.push_back(iri_triple(block, "<" + kSo + "containsTransaction>", invocation));
    triples.push_back(lit_triple(invocation, "<" + kSo + "instructionIndex>", literal_integer(0)));
    triples.push_back(lit_triple(invocation, "<" + kSo + "computeUnitsConsumed>", literal_integer(row.total_cus)));
    triples.push_back(lit_triple(invocation, "<" + kSo + "success>", literal_bool(row.error_count == 0)));
    triples.push_back(lit_triple(invocation, "<" + kSo + "isVoteTransaction>", literal_bool(row.is_vote)));

    return triples;
}

std::vector<Triple> OntologyMapper::map_slot_instruction(const SlotInstructionRow& row) const {
    std::vector<Triple> triples;
    const auto slot = slot_uri(row.slot);
    const auto block = block_uri(row.slot);

    triples.push_back(iri_triple(slot, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<" + kSo + "Slot>"));
    triples.push_back(lit_triple(slot, "<" + kSo + "slotNumber>", literal_integer(row.slot)));
    triples.push_back(lit_triple(slot, "<" + kSo + "slotStatus>", literal_string("Produced")));

    triples.push_back(iri_triple(block, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<" + kSo + "Block>"));
    triples.push_back(iri_triple(slot, "<" + kSo + "hasBlock>", block));
    triples.push_back(iri_triple(block, "<" + kSo + "occupiesSlot>", slot));
    if (row.timestamp != 0) {
        triples.push_back(lit_triple(block, "<" + kSo + "blockTimeUnix>", literal_long(row.timestamp)));
    }

    const auto metrics = "<" + kResourceBase + "slotMetrics/" + std::to_string(row.slot) + ">";
    triples.push_back(iri_triple(metrics, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<" + kSo + "LedgerEvent>"));
    triples.push_back(iri_triple(block, "<" + kSo + "containsEntry>", metrics));
    triples.push_back(lit_triple(metrics, "<" + kSo + "entryIndex>", literal_integer(0)));
    triples.push_back(lit_triple(metrics, "<" + kSo + "base58Value>", literal_string(
        "voteInstructions=" + std::to_string(row.vote_instruction_count) +
        ";nonVoteInstructions=" + std::to_string(row.non_vote_instruction_count) +
        ";voteTransactions=" + std::to_string(row.vote_transaction_count) +
        ";nonVoteTransactions=" + std::to_string(row.non_vote_transaction_count))));

    return triples;
}

std::vector<Triple> OntologyMapper::map_slot_status(const SlotStatusRow& row) const {
    std::vector<Triple> triples;
    const auto slot = slot_uri(row.slot);
    triples.push_back(iri_triple(slot, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<" + kSo + "Slot>"));
    triples.push_back(lit_triple(slot, "<" + kSo + "slotNumber>", literal_integer(row.slot)));
    if (row.slot_status.has_value()) {
        triples.push_back(lit_triple(slot, "<" + kSo + "slotStatus>", literal_string(*row.slot_status)));
    }
    if (row.block_time.has_value()) {
        const auto block = block_uri(row.slot);
        triples.push_back(iri_triple(block, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<" + kSo + "Block>"));
        triples.push_back(iri_triple(slot, "<" + kSo + "hasBlock>", block));
        triples.push_back(iri_triple(block, "<" + kSo + "occupiesSlot>", slot));
        triples.push_back(lit_triple(block, "<" + kSo + "blockTimeUnix>", literal_long(*row.block_time)));
        if (row.block_height.has_value()) {
            triples.push_back(lit_triple(block, "<" + kSo + "blockHeight>", literal_integer(*row.block_height)));
        }
        if (row.leader.has_value()) {
            const auto leader = "<" + kResourceBase + "validator/" + escape_turtle_string(*row.leader) + ">";
            triples.push_back(iri_triple(leader, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<" + kSo + "Validator>"));
            triples.push_back(iri_triple(slot, "<" + kSo + "hasLeader>", leader));
            triples.push_back(iri_triple(block, "<" + kSo + "producedBy>", leader));
        }
    }
    return triples;
}

} // namespace sapetl
