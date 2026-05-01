#include "test_framework.hpp"
#include "transform/ontology_mapper.hpp"

TEST(ontology_mapper_maps_program_invocation) {
    sapetl::OntologyMapper mapper;
    sapetl::ProgramInvocationRow row{};
    row.slot = 42;
    row.timestamp = 1234;
    row.program_id_hex = "ABCD";
    row.is_vote = false;
    row.total_cus = 99;

    const auto triples = mapper.map_program_invocation(row);
    REQUIRE(!triples.empty());

    bool saw_program = false;
    bool saw_slot = false;
    for (const auto& triple : triples) {
        if (triple.subject.find("program/abcd") != std::string::npos) {
            saw_program = true;
        }
        if (triple.subject.find("slot/42") != std::string::npos) {
            saw_slot = true;
        }
    }
    REQUIRE(saw_program);
    REQUIRE(saw_slot);
}
