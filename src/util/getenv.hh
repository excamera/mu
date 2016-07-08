#pragma once
#include <cstdlib>
#include <string>
#include <stdexcept>

std::string
safe_getenv(const std::string &key) {
    const char *const value = getenv(key.c_str());
    if (!value) {
        throw std::runtime_error("missing environment variable: " + key);
    }

    return value;
}
