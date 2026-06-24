#pragma once
/**
 * config.hpp  –  Zero-dependency INI config parser (hand-rolled).
 *
 * Format rules
 *   [section]            – starts a new section
 *   key = value          – key/value pair; value is everything after '='
 *                          up to the first ; or # comment character,
 *                          with leading/trailing whitespace stripped
 *   ; comment / # comment – ignored anywhere on a line
 *   blank lines          – ignored
 *
 * No Boost, no third-party libs – pure C++ stdlib.
 */
#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

class Config {
public:
    explicit Config(const std::string &path) {
        std::ifstream f(path);
        if (!f.is_open())
            throw std::runtime_error("Cannot open config: " + path);

        std::string line, section;
        int lineno = 0;
        while (std::getline(f, line)) {
            ++lineno;
            line = trim(line);
            if (line.empty() || line[0] == ';' || line[0] == '#') continue;

            // [section]
            if (line[0] == '[') {
                auto end = line.find(']');
                if (end == std::string::npos)
                    throw std::runtime_error(
                        path + ":" + std::to_string(lineno)
                        + " – unclosed '['");
                section = trim(line.substr(1, end - 1));
                continue;
            }

            // key = value
            auto eq = line.find('=');
            if (eq == std::string::npos) continue;      // no '=' → skip

            std::string key = trim(line.substr(0, eq));
            std::string val = trim(strip_comment(line.substr(eq + 1)));

            if (key.empty()) continue;
            store_[section.empty() ? key : section + "." + key] = val;
        }
    }

    // ── Typed accessors ────────────────────────────────────────────────────
    std::string get_str(const std::string &k,
                        const std::string &def = "") const {
        auto it = store_.find(k);
        return it == store_.end() ? def : it->second;
    }

    int get_int(const std::string &k, int def = 0) const {
        auto it = store_.find(k);
        if (it == store_.end() || it->second.empty()) return def;
        try { return std::stoi(it->second); } catch (...) { return def; }
    }

    double get_double(const std::string &k, double def = 0.0) const {
        auto it = store_.find(k);
        if (it == store_.end() || it->second.empty()) return def;
        try { return std::stod(it->second); } catch (...) { return def; }
    }

    bool get_bool(const std::string &k, bool def = false) const {
        auto it = store_.find(k);
        if (it == store_.end() || it->second.empty()) return def;
        std::string v = it->second;
        std::transform(v.begin(), v.end(), v.begin(), ::tolower);
        return v == "true" || v == "1" || v == "yes";
    }

    // Dump all parsed keys (useful for debugging)
    void dump() const {
        for (auto &kv : store_)
            std::cout << "  " << kv.first << " = " << kv.second << "\n";
    }

    // Returns all key/value pairs belonging to one section, with the
    // "section." prefix stripped off the keys. Needed for sections with
    // an arbitrary, unknown-in-advance number of entries (e.g. a list of
    // Pi backends under [pis], where each Pi is its own key).
    std::vector<std::pair<std::string, std::string>>
    get_section(const std::string &section) const {
        std::vector<std::pair<std::string, std::string>> result;
        const std::string prefix = section + ".";
        for (auto &kv : store_) {
            if (kv.first.size() > prefix.size() &&
                kv.first.compare(0, prefix.size(), prefix) == 0) {
                result.emplace_back(kv.first.substr(prefix.size()), kv.second);
            }
        }
        return result;
    }

private:
    std::unordered_map<std::string, std::string> store_;

    static std::string trim(const std::string &s) {
        const char *ws = " \t\r\n";
        size_t a = s.find_first_not_of(ws);
        if (a == std::string::npos) return "";
        size_t b = s.find_last_not_of(ws);
        return s.substr(a, b - a + 1);
    }

    // Strip inline comment (; or #) — but only outside a quoted section
    // INI values are normally unquoted, so we just scan for ; / #
    static std::string strip_comment(const std::string &s) {
        bool in_q = false;
        for (size_t i = 0; i < s.size(); ++i) {
            if (s[i] == '"') in_q = !in_q;
            if (!in_q && (s[i] == ';' || s[i] == '#'))
                return s.substr(0, i);
        }
        return s;
    }
};
