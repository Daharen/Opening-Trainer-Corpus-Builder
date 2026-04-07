#include "otcb/gambit_companion.hpp"

#include <sqlite3.h>

#include <algorithm>
#include <cmath>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "otcb/progress.hpp"

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
                                                         const std::vector<AggregatedPositionRecord>& positions,
                                                         ProgressReporter* progress) {
    (void)summary;
    if (std::filesystem::exists(sqlite_path)) std::filesystem::remove(sqlite_path);
    if (progress) {
        progress->stage_started(ProgressStage::ComputeRiskyOverlay, "computing risky companion overlay");
    }
    SqliteDb db(sqlite_path);
    exec_sql(db.get(), "BEGIN TRANSACTION;");
    try {
        exec_sql(db.get(),
            "CREATE TABLE companion_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "CREATE TABLE ordinary_move_acceptance_by_band(band_id TEXT NOT NULL, policy_scope TEXT NOT NULL, position_key TEXT NOT NULL, move_key TEXT NOT NULL, grade TEXT NOT NULL, book_source_flag INTEGER NOT NULL, explainability_note TEXT, PRIMARY KEY(band_id, policy_scope, position_key, move_key));"
            "CREATE TABLE opening_variance_baseline_by_band(band_id TEXT PRIMARY KEY, baseline_sigma REAL NOT NULL, sample_count INTEGER NOT NULL, comparator_count INTEGER NOT NULL, config_provenance TEXT NOT NULL);"
            "CREATE TABLE risky_entry_metrics(band_id TEXT NOT NULL, policy_variant TEXT NOT NULL, scope_variant TEXT NOT NULL, position_key TEXT NOT NULL, move_key TEXT NOT NULL, sample_count INTEGER NOT NULL, supported_reply_count INTEGER NOT NULL, pooled_band_provenance TEXT NOT NULL, mu_candidate REAL NOT NULL, sigma_candidate REAL NOT NULL, mu_base REAL NOT NULL, sigma_base REAL NOT NULL, variance_surplus REAL NOT NULL, mean_penalty REAL NOT NULL, optimistic_ceiling_value REAL NOT NULL, adequacy_status TEXT NOT NULL, resolution_reason_code TEXT NOT NULL, PRIMARY KEY(band_id, policy_variant, scope_variant, position_key, move_key));"
            "CREATE TABLE risky_acceptance_by_band(band_id TEXT NOT NULL, policy_variant TEXT NOT NULL, scope_variant TEXT NOT NULL, position_key TEXT NOT NULL, move_key TEXT NOT NULL, allowed INTEGER NOT NULL, resolution_reason_code TEXT NOT NULL, source_band_of_first_admission TEXT, pooled_band_provenance TEXT NOT NULL, annotation TEXT, PRIMARY KEY(band_id, policy_variant, scope_variant, position_key, move_key));"
            "CREATE VIEW risky_acceptance_audit AS SELECT ra.band_id, ra.policy_variant, ra.scope_variant, ra.position_key, ra.move_key, rm.mu_candidate, rm.sigma_candidate, rm.mu_base, rm.sigma_base, rm.mean_penalty, rm.optimistic_ceiling_value, rm.adequacy_status, ra.allowed, ra.resolution_reason_code FROM risky_acceptance_by_band ra JOIN risky_entry_metrics rm ON rm.band_id=ra.band_id AND rm.policy_variant=ra.policy_variant AND rm.scope_variant=ra.scope_variant AND rm.position_key=ra.position_key AND rm.move_key=ra.move_key;");

        Statement meta_stmt(db.get(), "INSERT INTO companion_metadata(key,value) VALUES (?,?);");
        const std::vector<std::pair<std::string, std::string>> meta = {
            {"artifact_schema_version", "otcb_risky_companion_v1"},
            {"horizon_plies", std::to_string(config.gambit_utility.horizon_plies)},
            {"candidate_min_support", std::to_string(config.gambit_utility.candidate_min_support)},
            {"t_sigma", std::to_string(config.gambit_utility.t_sigma)},
            {"delta_max", std::to_string(config.gambit_utility.delta_max)},
            {"mu_floor", std::to_string(config.gambit_utility.mu_floor)},
            {"n_min", std::to_string(config.gambit_utility.n_min)},
            {"r_min", std::to_string(config.gambit_utility.r_min)},
            {"k", std::to_string(config.gambit_utility.k)},
            {"pooling_behavior", config.gambit_utility.enable_downward_pooling ? "enabled" : "disabled"},
            {"downward_propagation", config.gambit_utility.enable_downward_propagation ? "enabled" : "disabled"},
            {"continuation_oracle", "builder_local_ordinary_acceptance_only"},
        };
        for (const auto& [k,v] : meta) {
            bind_text(meta_stmt.get(),1,k); bind_text(meta_stmt.get(),2,v); step(db.get(), meta_stmt.get()); reset_statement(meta_stmt.get());
        }

        const std::string band_id = band_id_for_config(config);
        std::map<std::pair<std::string,std::string>, std::string> grades;
        struct ProbRow { std::string position; std::string move; double p; double sigma; int n; };
        std::vector<ProbRow> accepted_non_gambit;

        Statement ordinary_stmt(db.get(), "INSERT INTO ordinary_move_acceptance_by_band(band_id,policy_scope,position_key,move_key,grade,book_source_flag,explainability_note) VALUES (?,?,?,?,?,?,?);");
        GambitCompanionWriteStats stats;
        stats.payload_file = "data/risky_companion.sqlite";
        stats.baseline_rows = 1;

        for (const auto& pos : positions) {
            if (progress) {
                progress->update([&](ProgressSnapshot& s) {
                    s.risky_current_band = band_id;
                    s.risky_positions_considered += 1;
                });
            }
            for (std::size_t i = 0; i < pos.candidate_moves.size(); ++i) {
                const auto& m = pos.candidate_moves[i];
                const std::string grade = grade_for_rank(i);
                grades[{pos.position_key, m.move_key}] = grade;
                const double p = pos.total_observations > 0 ? static_cast<double>(m.raw_count) / static_cast<double>(pos.total_observations) : 0.0;
                const double sigma = sigma_from_probability(p);
                if (grade == "book" || grade == "best" || grade == "excellent") {
                    accepted_non_gambit.push_back({pos.position_key, m.move_key, p, sigma, m.raw_count});
                }
                for (const std::string& policy : config.gambit_utility.continuation_policies) {
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

        Statement metrics_stmt(db.get(), "INSERT INTO risky_entry_metrics(band_id,policy_variant,scope_variant,position_key,move_key,sample_count,supported_reply_count,pooled_band_provenance,mu_candidate,sigma_candidate,mu_base,sigma_base,variance_surplus,mean_penalty,optimistic_ceiling_value,adequacy_status,resolution_reason_code) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
        Statement acceptance_stmt(db.get(), "INSERT INTO risky_acceptance_by_band(band_id,policy_variant,scope_variant,position_key,move_key,allowed,resolution_reason_code,source_band_of_first_admission,pooled_band_provenance,annotation) VALUES (?,?,?,?,?,?,?,?,?,?);");

        for (const auto& pos : positions) {
            for (const auto& m : pos.candidate_moves) {
                const auto grade_it = grades.find({pos.position_key, m.move_key});
                if (grade_it == grades.end() || (grade_it->second != "fail" && grade_it->second != "good")) {
                    continue;
                }
                if (progress) {
                    progress->update([&](ProgressSnapshot& s) { s.risky_candidate_fails_considered += 1; });
                }
                if (m.raw_count < config.gambit_utility.candidate_min_support) {
                    ++stats.unresolved_rows;
                    if (progress) {
                        progress->update([&](ProgressSnapshot& s) { s.risky_candidates_skipped_support += 1; });
                    }
                    continue;
                }
                const double mu_g = pos.total_observations > 0 ? static_cast<double>(m.raw_count) / static_cast<double>(pos.total_observations) : 0.0;
                const double sigma_g = sigma_from_probability(mu_g);
                double mu_stable = 0.0;
                int stable_n = 0;
                int reply_support_count = 0;
                for (const auto& c : pos.candidate_moves) {
                    if (c.move_key == m.move_key) {
                        continue;
                    }
                    ++reply_support_count;
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
                for (const std::string& policy : config.gambit_utility.continuation_policies) {
                    const std::string scope = "risky_sharp";
                    if (progress) {
                        progress->update([&](ProgressSnapshot& s) {
                            s.risky_current_policy = policy;
                            s.risky_current_scope = scope;
                        });
                    }
                    const double u_g = mu_g + config.gambit_utility.k * sigma_g;
                    const double u_stable = mu_stable + config.gambit_utility.k * baseline;
                    const int n = m.raw_count;
                    const bool adequate = n >= config.gambit_utility.n_min && stable_n > 0 && reply_support_count >= config.gambit_utility.r_min;
                    const bool sigma_gate = sigma_g >= baseline + config.gambit_utility.t_sigma;
                    const bool mu_gate = mu_g >= mu_stable - config.gambit_utility.delta_max;
                    const bool upper_gate = u_g >= u_stable;
                    const bool floor_gate = mu_g >= config.gambit_utility.mu_floor;
                    bool allowed = adequate && sigma_gate && mu_gate && upper_gate && floor_gate;
                    std::string reason = "rejected_threshold";
                    if (!adequate) reason = "unresolved_insufficient_support";
                    else if (!floor_gate) reason = "rejected_absolute_floor";
                    else if (allowed) reason = "admitted";

                    bind_text(metrics_stmt.get(),1,band_id); bind_text(metrics_stmt.get(),2,policy); bind_text(metrics_stmt.get(),3,scope);
                    bind_text(metrics_stmt.get(),4,pos.position_key); bind_text(metrics_stmt.get(),5,m.move_key); bind_int(metrics_stmt.get(),6,n);
                    bind_int(metrics_stmt.get(),7,reply_support_count);
                    bind_text(metrics_stmt.get(),8,band_id); bind_double(metrics_stmt.get(),9,mu_g); bind_double(metrics_stmt.get(),10,sigma_g); bind_double(metrics_stmt.get(),11,mu_stable); bind_double(metrics_stmt.get(),12,baseline);
                    bind_double(metrics_stmt.get(),13,sigma_g - baseline); bind_double(metrics_stmt.get(),14,std::max(0.0, mu_stable - mu_g)); bind_double(metrics_stmt.get(),15,u_g);
                    bind_text(metrics_stmt.get(),16,adequate ? "adequate" : "insufficient"); bind_text(metrics_stmt.get(),17,reason);
                    step(db.get(), metrics_stmt.get()); reset_statement(metrics_stmt.get());

                    bind_text(acceptance_stmt.get(),1,band_id); bind_text(acceptance_stmt.get(),2,policy); bind_text(acceptance_stmt.get(),3,scope); bind_text(acceptance_stmt.get(),4,pos.position_key); bind_text(acceptance_stmt.get(),5,m.move_key);
                    bind_int(acceptance_stmt.get(),6,allowed ? 1 : 0); bind_text(acceptance_stmt.get(),7,reason);
                    if (allowed) bind_text(acceptance_stmt.get(),8,band_id); else sqlite3_bind_null(acceptance_stmt.get(),8);
                    bind_text(acceptance_stmt.get(),9,band_id);
                    bind_text(acceptance_stmt.get(),10,"family_or_gambit_metadata_optional_only");
                    step(db.get(), acceptance_stmt.get()); reset_statement(acceptance_stmt.get());

                    ++stats.metrics_rows;
                    ++stats.acceptance_rows;
                    if (allowed) ++stats.admitted_rows;
                    else if (!adequate) ++stats.unresolved_rows;
                    else ++stats.rejected_rows;
                    if (progress) {
                        progress->update([&](ProgressSnapshot& s) {
                            s.risky_candidates_evaluated += 1;
                            s.risky_admitted_rows = stats.admitted_rows;
                            s.risky_rejected_rows = stats.rejected_rows;
                            s.risky_unresolved_rows = stats.unresolved_rows;
                        });
                    }
                }
            }
        }

        exec_sql(db.get(), "COMMIT;");
        if (progress) {
            progress->stage_completed("risky companion computed");
        }
        return stats;
    } catch (...) {
        exec_sql(db.get(), "ROLLBACK;");
        if (progress) {
            progress->stage_failed("risky companion computation failed");
        }
        throw;
    }
}

}  // namespace otcb
