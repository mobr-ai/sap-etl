#pragma once

#include "model/types.hpp"

#include <vector>

namespace sapetl {

class Loader {
  public:
    virtual ~Loader() = default;
    virtual void begin() = 0;
    virtual void append(const std::vector<Triple>& triples) = 0;
    virtual void finish() = 0;
};

} // namespace sapetl
