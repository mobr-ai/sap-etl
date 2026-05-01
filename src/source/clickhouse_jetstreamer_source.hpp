#pragma once

#include "clickhouse/clickhouse_client.hpp"
#include "source/source.hpp"

#include <unordered_set>

namespace sapetl {

class ClickHouseJetstreamerSource final : public JetstreamerSource {
  public:
    explicit ClickHouseJetstreamerSource(ClickHouseClient client);

    std::vector<ProgramInvocationRow> fetch_program_invocations(const Range& slots) const override;
    std::vector<SlotInstructionRow> fetch_slot_instructions(const Range& slots) const override;
    std::vector<SlotStatusRow> fetch_slot_statuses(const Range& slots) const override;

  private:
    bool has_table(const std::string& name) const;
    bool table_has_column(const std::string& table, const std::string& column) const;

    ClickHouseClient client_;
};

} // namespace sapetl
