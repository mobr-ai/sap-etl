#pragma once

#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

using TestFn = std::function<void()>;

struct TestCase {
    std::string name;
    TestFn fn;
};

std::vector<TestCase>& registry();

struct Register {
    Register(std::string name, TestFn fn);
};

#define TEST(name) \
    void name(); \
    static Register reg_##name(#name, name); \
    void name()

#define REQUIRE(cond) \
    do { \
        if (!(cond)) { \
            throw std::runtime_error("requirement failed: " #cond); \
        } \
    } while (false)
