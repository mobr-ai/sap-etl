#include "test_framework.hpp"

std::vector<TestCase>& registry() {
    static std::vector<TestCase> value;
    return value;
}

Register::Register(std::string name, TestFn fn) {
    registry().push_back({std::move(name), std::move(fn)});
}

int main() {
    std::size_t failures = 0;
    for (const auto& test : registry()) {
        try {
            test.fn();
            std::cout << "[PASS] " << test.name << '\n';
        } catch (const std::exception& ex) {
            ++failures;
            std::cerr << "[FAIL] " << test.name << ": " << ex.what() << '\n';
        }
    }
    return failures == 0 ? 0 : 1;
}
