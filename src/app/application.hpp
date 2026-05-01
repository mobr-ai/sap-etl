#pragma once

#include "checkpoint/checkpoint_store.hpp"
#include "load/loader.hpp"
#include "model/types.hpp"
#include "source/source.hpp"
#include "transform/ontology_mapper.hpp"

#include <memory>

namespace sapetl {

class Application {
  public:
    Application(
        AppConfig config,
        std::unique_ptr<JetstreamerSource> source,
        std::unique_ptr<Loader> loader);

    int run();

  private:
    template <typename Rows, typename MapperFn>
    void process_phase(
        const std::string& phase_name,
        const std::vector<Range>& chunks,
        const Rows& rows,
        MapperFn&& mapper);

    AppConfig config_;
    std::unique_ptr<JetstreamerSource> source_;
    std::unique_ptr<Loader> loader_;
    CheckpointStore checkpoints_;
    OntologyMapper mapper_;
};

} // namespace sapetl
