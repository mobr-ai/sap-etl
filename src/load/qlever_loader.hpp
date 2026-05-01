#pragma once

#include "http/http_client.hpp"
#include "load/loader.hpp"

#include <string>
#include <vector>

namespace sapetl {

class QleverLoader final : public Loader {
  public:
    QleverLoader(std::string update_url, std::size_t max_payload_bytes);

    void begin() override;
    void append(const std::vector<Triple>& triples) override;
    void finish() override;

  private:
    void flush();

    std::string update_url_;
    std::size_t max_payload_bytes_;
    HttpClient http_;
    std::vector<Triple> pending_;
};

} // namespace sapetl
