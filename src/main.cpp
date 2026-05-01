#include "app/application.hpp"
#include "clickhouse/clickhouse_client.hpp"
#include "config/cli_parser.hpp"
#include "load/qlever_loader.hpp"
#include "load/ttl_loader.hpp"
#include "source/clickhouse_jetstreamer_source.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

int main(int argc, char** argv) {
    try {
        std::vector<std::string> args;
        args.reserve(static_cast<std::size_t>(argc > 0 ? argc - 1 : 0));
        for (int i = 1; i < argc; ++i) {
            args.emplace_back(argv[i]);
        }

        const auto config = sapetl::parse_cli_args(args);

        auto source = std::make_unique<sapetl::ClickHouseJetstreamerSource>(
            sapetl::ClickHouseClient(config.clickhouse_url, config.clickhouse_database));

        std::unique_ptr<sapetl::Loader> loader;
        if (config.load_type == sapetl::LoadType::Ttl) {
            loader = std::make_unique<sapetl::TtlLoader>(
                config.output_dir,
                config.output_prefix,
                config.ttl_max_bytes,
                config.overwrite);
        } else {
            loader = std::make_unique<sapetl::QleverLoader>(
                config.qlever_update_url,
                config.qlever_update_max_bytes);
        }

        sapetl::Application app(config, std::move(source), std::move(loader));
        return app.run();
    } catch (const std::exception& ex) {
        std::cerr << "sap-etl error: " << ex.what() << '\n';
        return 1;
    }
}
