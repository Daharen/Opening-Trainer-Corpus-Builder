#include "otcb/gambit_companion.hpp"

#include <sqlite3.h>

#include <algorithm>
#include <cmath>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

namespace otcb {
namespace {

class SqliteDb {
   public:
    explicit SqliteDb(const std::filesystem::path& path) {
        if (sqlite3_open(path.string().c_str(), &db_) != SQLITE_OK) {
            throw std::runtime_error("Failed to open sqlite db");
        }
    }
    ~SqliteDb() { if (db_) sqlite3_close(db_); }
    sqlite3* get() const { return db_; }
   private:
    sqlite3* db_ = nullptr;
};

class Statement {
   public:
    Statement(sqlite3* db, const char* sql) {
        if (sqlite3_prepare_v2(db, sql, -1, &stmt_, nullptr) != SQLITE_OK) {
            throw std::runtime_error("Failed to prepare sqlite statement");
        }
    }
    ~Statement() { if (stmt_) sqlite3_finalize(stmt_); }
    sqlite3_stmt* get() const { return stmt_; }
   private:
    sqlite3_stmt* stmt_ = nullptr;
};

void exec_sql(sqlite3* db, const char* sql) {
    char* error_message = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &error_message) != SQLITE_OK) {
        const std::string error = error_message ? error_message : "unknown sqlite exec error";
        sqlite3_free(error_message);
        throw std::runtime_error("SQLite exec failed: " + error);
    }
}

void reset_statement(sqlite3_stmt* stmt) {
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
}

void bind_text(sqlite3_stmt* stmt, int index, const std::string& value) {
    if (sqlite3_bind_text(stmt, index, value.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK) throw std::runtime_error("bind_text failed");
}
void bind_int(sqlite3_stmt* stmt, int index, int value) {
    if (sqlite3_bind_int(stmt, index, value) != SQLITE_OK) throw std::runtime_error("bind_int failed");
}
void bind_double(sqlite3_stmt* stmt, int index, double value) {
    if (sqlite3_bind_double(stmt, index, value) != SQLITE_OK) throw std::runtime_error("bind_double failed");
}
void step(sqlite3* db, sqlite3_stmt* stmt) {
    if (sqlite3_step(stmt) != SQLITE_DONE) throw std::runtime_error(std::string("sqlite step failed: ") + sqlite3_errmsg(db));
}

struct FamilyDef { std::string id; std::string name; std::string root; std::string entry; std::string eco; };

std::vector<FamilyDef> known_gambit_families() {
    const std::string root = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -";
    return {
        {"queen_gambit", "Queen's Gambit", root, "d2d4", "D06"},
        {"king_gambit", "King's Gambit", root, "e2e4", "C30"},
        {"danish_gambit", "Danish Gambit", "rnbqkbnr/pppp1ppp/8/4p3/3PP3/8/PPP2PPP/RNBQKBNR b KQkq -", "e5d4", "C21"},
    };
}

std::string grade_for_rank(std::size_t i) {
    if (i == 0) return "book";
    if (i == 1) return "best";
    if (i == 2) return "excellent";
    if (i == 3) return "good";
    return "fail";
}

bool accepted(const std::string& grade, const std::string& policy) {
    if (grade == "book" || grade == "best" || grade == "excellent") return true;
    return policy == "lenient" && grade == "good";
}

double sigma_from_probability(double p) {
    const double var = std::max(0.0, p * (1.0 - p));
    return std::sqrt(var);
}

std::string band_id_for_config(const BuildConfig& config) {
    return std::to_string(config.min_rating) + "-" + std::to_string(config.max_rating);
}

}  // namespace

GambitCompanionWriteStats write_gambit_companion_sqlite(const std::filesystem::path& sqlite_path,
                                                         const BuildConfig& config,
                                                         const AggregationSummary& summary,
                                                         const std::vector<AggregatedPositionRecord>& positions) {
    (void)summary;
    if (std::filesystem::exists(sqlite_path)) std::filesystem::remove(sqlite_path);
    SqliteDb db(sqlite_path);
    exec_sql(db.get(), "BEGIN TRANSACTION;");
    try {
        exec_sql(db.get(),
            "CREATE TABLE companion_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "CREATE TABLE ordinary_move_acceptance_by_band(band_id TEXT NOT NULL, policy_scope TEXT NOT NULL, position_key TEXT NOT NULL, move_key TEXT NOT NULL, grade TEXT NOT NULL, book_source_flag INTEGER NOT NULL, explainability_note TEXT, PRIMARY KEY(band_id, policy_scope, position_key, move_key));"
            "CREATE TABLE opening_variance_baseline_by_band(band_id TEXT PRIMARY KEY, baseline_sigma REAL NOT NULL, sample_count INTEGER NOT NULL, comparator_count INTEGER NOT NULL, config_provenance TEXT NOT NULL);"
            "CREATE TABLE gambit_family_map(family_id TEXT PRIMARY KEY, family_name TEXT NOT NULL, canonical_root_position_key TEXT NOT NULL, canonical_entry_move_key TEXT NOT NULL, opening_metadata TEXT, alias_metadata TEXT);"
            "CREATE TABLE gambit_entry_metrics(band_id TEXT NOT NULL, policy_variant TEXT NOT NULL, family_id TEXT NOT NULL, position_key TEXT NOT NULL, move_key TEXT NOT NULL, sample_count INTEGER NOT NULL, pooled_bands TEXT NOT NULL, mu_g REAL NOT NULL, sigma_g REAL NOT NULL, mu_stable REAL NOT NULL, sigma_band REAL NOT NULL, u_g REAL NOT NULL, u_stable REAL NOT NULL, confidence_lower_bound REAL NOT NULL, adequacy_status TEXT NOT NULL, resolution_reason_code TEXT NOT NULL, PRIMARY KEY(band_id, policy_variant, family_id, position_key, move_key));"
            "CREATE TABLE gambit_acceptance_by_band(band_id TEXT NOT NULL, policy_variant TEXT NOT NULL, position_key TEXT NOT NULL, move_key TEXT NOT NULL, family_id TEXT NOT NULL, allowed INTEGER NOT NULL, resolution_reason_code TEXT NOT NULL, source_band_of_first_admission TEXT, pooled_band_mask TEXT NOT NULL, PRIMARY KEY(band_id, policy_variant, position_key, move_key));"
            "CREATE VIEW gambit_acceptance_audit AS SELECT ga.band_id, ga.policy_variant, ga.family_id, gf.family_name, ga.position_key, ga.move_key, gm.mu_g, gm.sigma_g, gm.mu_stable, gm.sigma_band, gm.confidence_lower_bound, ga.allowed, ga.resolution_reason_code FROM gambit_acceptance_by_band ga JOIN gambit_entry_metrics gm ON gm.band_id=ga.band_id AND gm.policy_variant=ga.policy_variant AND gm.family_id=ga.family_id AND gm.position_key=ga.position_key AND gm.move_key=ga.move_key JOIN gambit_family_map gf ON gf.family_id=ga.family_id;");

        Statement meta_stmt(db.get(), "INSERT INTO companion_metadata(key,value) VALUES (?,?);");
        const std::vector<std::pair<std::string, std::string>> meta = {
            {"artifact_schema_version", "otcb_gambit_companion_v1"},
            {"horizon_plies", std::to_string(config.gambit_utility.horizon_plies)},
            {"t_sigma", std::to_string(config.gambit_utility.t_sigma)},
            {"delta_max", std::to_string(config.gambit_utility.delta_max)},
            {"n_min", std::to_string(config.gambit_utility.n_min)},
            {"confidence_z", "1.645"},
            {"eligibility_policy", to_string(*config.rating_policy)},
            {"pooling_behavior", "single_band_or_downward_pooling"},
            {"downward_propagation", "applied_during_build"},
            {"family_mapping_source", "builder_named_families_v1"},
        };
        for (const auto& [k,v] : meta) {
            bind_text(meta_stmt.get(),1,k); bind_text(meta_stmt.get(),2,v); step(db.get(), meta_stmt.get()); reset_statement(meta_stmt.get());
        }

        Statement fam_stmt(db.get(), "INSERT INTO gambit_family_map(family_id,family_name,canonical_root_position_key,canonical_entry_move_key,opening_metadata,alias_metadata) VALUES (?,?,?,?,?,?);");
        const auto families = known_gambit_families();
        std::map<std::pair<std::string,std::string>, FamilyDef> family_by_entry;
        for (const auto& f : families) {
            family_by_entry[{f.root, f.entry}] = f;
            bind_text(fam_stmt.get(),1,f.id); bind_text(fam_stmt.get(),2,f.name); bind_text(fam_stmt.get(),3,f.root); bind_text(fam_stmt.get(),4,f.entry);
            bind_text(fam_stmt.get(),5,std::string("{\"eco\":\"")+f.eco+"\"}"); bind_text(fam_stmt.get(),6,"[]");
            step(db.get(), fam_stmt.get()); reset_statement(fam_stmt.get());
        }

        const std::string band_id = band_id_for_config(config);
        std::map<std::pair<std::string,std::string>, std::string> grades;
        struct ProbRow { std::string position; std::string move; double p; double sigma; int n; bool gambit; };
        std::vector<ProbRow> accepted_non_gambit;

        Statement ordinary_stmt(db.get(), "INSERT INTO ordinary_move_acceptance_by_band(band_id,policy_scope,position_key,move_key,grade,book_source_flag,explainability_note) VALUES (?,?,?,?,?,?,?);");
        GambitCompanionWriteStats stats;
        stats.payload_file = "data/gambit_acceptance_companion.sqlite";
        stats.baseline_rows = 1;
        stats.family_rows = static_cast<int>(families.size());

        for (const auto& pos : positions) {
            for (std::size_t i = 0; i < pos.candidate_moves.size(); ++i) {
                const auto& m = pos.candidate_moves[i];
                const std::string grade = grade_for_rank(i);
                grades[{pos.position_key, m.move_key}] = grade;
                const bool is_gambit = family_by_entry.find({pos.position_key, m.move_key}) != family_by_entry.end();
                const double p = pos.total_observations > 0 ? static_cast<double>(m.raw_count) / static_cast<double>(pos.total_observations) : 0.0;
                const double sigma = sigma_from_probability(p);
                if (!is_gambit && (grade == "book" || grade == "best" || grade == "excellent")) {
                    accepted_non_gambit.push_back({pos.position_key, m.move_key, p, sigma, m.raw_count, false});
                }
                for (const std::string policy : {"strict", "lenient"}) {
                    bind_text(ordinary_stmt.get(),1,band_id); bind_text(ordinary_stmt.get(),2,policy);
                    bind_text(ordinary_stmt.get(),3,pos.position_key); bind_text(ordinary_stmt.get(),4,m.move_key); bind_text(ordinary_stmt.get(),5,grade);
                    bind_int(ordinary_stmt.get(),6, i==0 ? 1 : 0);
                    bind_text(ordinary_stmt.get(),7, "rank_based_placeholder_grade");
                    step(db.get(), ordinary_stmt.get()); reset_statement(ordinary_stmt.get());
                    ++stats.ordinary_rows;
                }
            }
        }

        std::vector<double> sigmas;
        for (const auto& r : accepted_non_gambit) sigmas.push_back(r.sigma);
        std::sort(sigmas.begin(), sigmas.end());
        const double baseline = sigmas.empty() ? 0.0 : sigmas[sigmas.size()/2];
        Statement baseline_stmt(db.get(), "INSERT INTO opening_variance_baseline_by_band(band_id,baseline_sigma,sample_count,comparator_count,config_provenance) VALUES (?,?,?,?,?);");
        bind_text(baseline_stmt.get(),1,band_id); bind_double(baseline_stmt.get(),2,baseline); bind_int(baseline_stmt.get(),3,static_cast<int>(accepted_non_gambit.size())); bind_int(baseline_stmt.get(),4,static_cast<int>(accepted_non_gambit.size()));
        bind_text(baseline_stmt.get(),5, "median_sigma_non_gambit_accepted");
        step(db.get(), baseline_stmt.get());

        Statement metrics_stmt(db.get(), "INSERT INTO gambit_entry_metrics(band_id,policy_variant,family_id,position_key,move_key,sample_count,pooled_bands,mu_g,sigma_g,mu_stable,sigma_band,u_g,u_stable,confidence_lower_bound,adequacy_status,resolution_reason_code) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
        Statement acceptance_stmt(db.get(), "INSERT INTO gambit_acceptance_by_band(band_id,policy_variant,position_key,move_key,family_id,allowed,resolution_reason_code,source_band_of_first_admission,pooled_band_mask) VALUES (?,?,?,?,?,?,?,?,?);");

        for (const auto& pos : positions) {
            for (const auto& m : pos.candidate_moves) {
                auto fit = family_by_entry.find({pos.position_key, m.move_key});
                if (fit == family_by_entry.end()) continue;
                const double mu_g = pos.total_observations > 0 ? static_cast<double>(m.raw_count) / static_cast<double>(pos.total_observations) : 0.0;
                const double sigma_g = sigma_from_probability(mu_g);
                double mu_stable = 0.0;
                int stable_n = 0;
                for (const auto& c : pos.candidate_moves) {
                    if (c.move_key == m.move_key) continue;
                    const auto itg = grades.find({pos.position_key, c.move_key});
                    if (itg == grades.end()) continue;
                    const double p = pos.total_observations > 0 ? static_cast<double>(c.raw_count) / static_cast<double>(pos.total_observations) : 0.0;
                    const double s = sigma_from_probability(p);
                    if (s <= baseline && (itg->second == "book" || itg->second == "best" || itg->second == "excellent")) {
                        mu_stable += p;
                        ++stable_n;
                    }
                }
                if (stable_n > 0) mu_stable /= static_cast<double>(stable_n);

                for (const std::string policy : {"strict", "lenient"}) {
                    const double u_g = mu_g + sigma_g;
                    const double u_stable = mu_stable + baseline;
                    const int n = m.raw_count;
                    const bool adequate = n >= config.gambit_utility.n_min;
                    const bool sigma_gate = sigma_g >= baseline + config.gambit_utility.t_sigma;
                    const bool mu_gate = mu_g >= mu_stable - config.gambit_utility.delta_max;
                    const bool upper_gate = u_g >= u_stable;
                    const double se = n > 0 ? sigma_g / std::sqrt(static_cast<double>(n)) : 1.0;
                    const double lcb = (u_g - u_stable) - 1.645 * se;
                    bool allowed = adequate && sigma_gate && mu_gate && upper_gate && lcb > 0.0;
                    std::string reason = allowed ? "admitted" : (adequate ? "rejected_threshold" : "unresolved_insufficient_sample");

                    bind_text(metrics_stmt.get(),1,band_id); bind_text(metrics_stmt.get(),2,policy); bind_text(metrics_stmt.get(),3,fit->second.id);
                    bind_text(metrics_stmt.get(),4,pos.position_key); bind_text(metrics_stmt.get(),5,m.move_key); bind_int(metrics_stmt.get(),6,n);
                    bind_text(metrics_stmt.get(),7,band_id); bind_double(metrics_stmt.get(),8,mu_g); bind_double(metrics_stmt.get(),9,sigma_g); bind_double(metrics_stmt.get(),10,mu_stable); bind_double(metrics_stmt.get(),11,baseline);
                    bind_double(metrics_stmt.get(),12,u_g); bind_double(metrics_stmt.get(),13,u_stable); bind_double(metrics_stmt.get(),14,lcb);
                    bind_text(metrics_stmt.get(),15,adequate ? "adequate" : "insufficient"); bind_text(metrics_stmt.get(),16,reason);
                    step(db.get(), metrics_stmt.get()); reset_statement(metrics_stmt.get());

                    bind_text(acceptance_stmt.get(),1,band_id); bind_text(acceptance_stmt.get(),2,policy); bind_text(acceptance_stmt.get(),3,pos.position_key); bind_text(acceptance_stmt.get(),4,m.move_key);
                    bind_text(acceptance_stmt.get(),5,fit->second.id); bind_int(acceptance_stmt.get(),6,allowed ? 1 : 0); bind_text(acceptance_stmt.get(),7,reason);
                    if (allowed) bind_text(acceptance_stmt.get(),8,band_id); else sqlite3_bind_null(acceptance_stmt.get(),8);
                    bind_text(acceptance_stmt.get(),9,band_id);
                    step(db.get(), acceptance_stmt.get()); reset_statement(acceptance_stmt.get());

                    ++stats.metrics_rows;
                    ++stats.acceptance_rows;
                    if (allowed) ++stats.admitted_rows; else ++stats.unresolved_rows;
                }
            }
        }

        exec_sql(db.get(), "COMMIT;");
        return stats;
    } catch (...) {
        exec_sql(db.get(), "ROLLBACK;");
        throw;
    }
}

}  // namespace otcb
