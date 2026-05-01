#pragma once

#include "model/types.hpp"

#include <vector>

namespace sapetl {

class JetstreamerSource {
  public:
    virtual ~JetstreamerSource() = default;

    virtual std::vector<ProgramInvocationRow> fetch_program_invocations(const Range& slots) const = 0;
    virtual std::vector<SlotInstructionRow> fetch_slot_instructions(const Range& slots) const = 0;
    virtual std::vector<SlotStatusRow> fetch_slot_statuses(const Range& slots) const = 0;
};

} // namespace sapetl
