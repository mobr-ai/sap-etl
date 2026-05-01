#pragma once

#include "model/types.hpp"

#include <vector>

namespace sapetl {

class OntologyMapper {
  public:
    std::vector<Triple> map_program_invocation(const ProgramInvocationRow& row) const;
    std::vector<Triple> map_slot_instruction(const SlotInstructionRow& row) const;
    std::vector<Triple> map_slot_status(const SlotStatusRow& row) const;
    std::vector<Triple> ontology_prefixes() const;

  private:
    static std::string slot_uri(std::uint64_t slot);
    static std::string block_uri(std::uint64_t slot);
    static std::string program_uri_from_hex(const std::string& hex);
    static std::string literal_integer(std::uint64_t value);
    static std::string literal_long(std::uint64_t value);
    static std::string literal_string(const std::string& value);
    static std::string literal_bool(bool value);
};

} // namespace sapetl
