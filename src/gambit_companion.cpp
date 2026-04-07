#include "otcb/gambit_companion.hpp"

#include <sqlite3.h>

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <string>
#include <vector>

namespace otcb {
namespace {

class SqliteDb {
   public:
    explicit SqliteDb(const std::filesystem::path& path) {
        if (sqlite3_open(path.string().c_str(), &db_) != SQLITE_OK) throw std::runtime_error("Failed to open sqlite db");
    }
    ~SqliteDb() { if (db_) sqlite3_close(db_); }
    sqlite3* get() const { return db_; }
   private:
    sqlite3* db_ = nullptr;
};

class Statement {
   public:
    Statement(sqlite3* db, const char* sql) {
        if (sqlite3_prepare_v2(db, sql, -1, &stmt_, nullptr) != SQLITE_OK) throw std::runtime_error("Failed to prepare sqlite statement");
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
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
}

std::string band_id_for_config(const BuildConfig& config) {
    return std::to_string(config.min_rating) + "-" + std::to_string(config.max_rating);
}

double sigma_from_probability(const double p) {
    return std::sqrt(std::max(0.0, p * (1.0 - p)));
}

bool is_accepted_grade(const std::string& grade, const std::string& policy) {
    if (grade == "book" || grade == "best" || grade == "excellent") return true;
    return policy == "lenient" && grade == "good";
}

std::string grade_for_rank(const std::size_t i) {
    if (i == 0) return "book";
    if (i == 1) return "best";
    if (i == 2) return "excellent";
    if (i == 3) return "good";
    return "fail";
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
        progress->update([&](ProgressSnapshot& s) {
            s.risky_current_band = band_id_for_config(config);
            s.risky_current_scope = "risky_sharp";
            s.risky_current_policy = config.gambit_utility.continuation_policies.empty() ? "strict" : config.gambit_utility.continuation_policies.front();
        });
    }

    SqliteDb db(sqlite_path);
    exec_sql(db.get(), "BEGIN TRANSACTION;");
    try {
        exec_sql(db.get(),
            "CREATE TABLE companion_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "CREATE TABLE ordinary_move_acceptance_by_band(band_id TEXT NOT NULL, policy_variant TEXT NOT NULL, position_key TEXT NOT NULL, move_key TEXT NOT NULL, grade TEXT NOT NULL, accepted INTEGER NOT NULL, is_ordinary_fail INTEGER NOT NULL, support_count INTEGER NOT NULL, PRIMARY KEY(band_id, policy_variant, position_key, move_key));"
            "CREATE TABLE opening_variance_baseline_by_band(band_id TEXT NOT NULL, policy_variant TEXT NOT NULL, scope_variant TEXT NOT NULL, position_key TEXT NOT NULL, comparator_count INTEGER NOT NULL, sample_count INTEGER NOT NULL, mu_base REAL NOT NULL, sigma_base REAL NOT NULL, PRIMARY KEY(band_id, policy_variant, scope_variant, position_key));"
            "CREATE TABLE risky_entry_metrics(band_id TEXT NOT NULL, policy_variant TEXT NOT NULL, scope_variant TEXT NOT NULL, position_key TEXT NOT NULL, move_key TEXT NOT NULL, sample_count INTEGER NOT NULL, supported_reply_count INTEGER NOT NULL, pooled_band_provenance TEXT NOT NULL, mu_candidate REAL NOT NULL, sigma_candidate REAL NOT NULL, mu_base REAL NOT NULL, sigma_base REAL NOT NULL, variance_surplus REAL NOT NULL, mean_penalty REAL NOT NULL, optimistic_ceiling_value REAL NOT NULL, adequacy_status TEXT NOT NULL, reason_code TEXT NOT NULL, PRIMARY KEY(band_id, policy_variant, scope_variant, position_key, move_key));"
            "CREATE TABLE risky_acceptance_by_band(band_id TEXT NOT NULL, policy_variant TEXT NOT NULL, scope_variant TEXT NOT NULL, position_key TEXT NOT NULL, move_key TEXT NOT NULL, allowed INTEGER NOT NULL, reason_code TEXT NOT NULL, source_band_of_first_admission TEXT, pooled_band_provenance TEXT NOT NULL, annotation TEXT, PRIMARY KEY(band_id, policy_variant, scope_variant, position_key, move_key));");

        Statement meta_stmt(db.get(), "INSERT INTO companion_metadata(key,value) VALUES (?,?);");
        const std::vector<std::pair<std::string, std::string>> meta = {
            {"artifact_schema_version", "otcb_risky_companion_v1"},
            {"scope_variants", config.gambit_utility.emit_scope_risky_sharp ? "risky_sharp" : ""},
            {"risky_horizon_plies", std::to_string(config.gambit_utility.horizon_plies)},
            {"risky_candidate_min_support", std::to_string(config.gambit_utility.candidate_min_support)},
            {"risky_t_sigma", std::to_string(config.gambit_utility.t_sigma)},
            {"risky_delta_max", std::to_string(config.gambit_utility.delta_max)},
            {"risky_mu_floor", std::to_string(config.gambit_utility.mu_floor)},
            {"risky_n_min", std::to_string(config.gambit_utility.n_min)},
            {"risky_r_min", std::to_string(config.gambit_utility.r_min)},
            {"risky_k", std::to_string(config.gambit_utility.k)},
            {"pooling_behavior", config.gambit_utility.enable_downward_pooling ? "enabled" : "disabled"},
            {"downward_propagation_behavior", config.gambit_utility.enable_downward_propagation ? "enabled" : "disabled"},
            {"oracle_scope", "builder_local_ordinary_acceptance_only"},
        };
        for (const auto& [k, v] : meta) {
            bind_text(meta_stmt.get(), 1, k);
            bind_text(meta_stmt.get(), 2, v);
            step(db.get(), meta_stmt.get());
        }

        const std::string band_id = band_id_for_config(config);
        Statement ordinary_stmt(db.get(), "INSERT INTO ordinary_move_acceptance_by_band(band_id,policy_variant,position_key,move_key,grade,accepted,is_ordinary_fail,support_count) VALUES (?,?,?,?,?,?,?,?);");
        Statement baseline_stmt(db.get(), "INSERT INTO opening_variance_baseline_by_band(band_id,policy_variant,scope_variant,position_key,comparator_count,sample_count,mu_base,sigma_base) VALUES (?,?,?,?,?,?,?,?);");
        Statement metrics_stmt(db.get(), "INSERT INTO risky_entry_metrics(band_id,policy_variant,scope_variant,position_key,move_key,sample_count,supported_reply_count,pooled_band_provenance,mu_candidate,sigma_candidate,mu_base,sigma_base,variance_surplus,mean_penalty,optimistic_ceiling_value,adequacy_status,reason_code) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
        Statement acceptance_stmt(db.get(), "INSERT INTO risky_acceptance_by_band(band_id,policy_variant,scope_variant,position_key,move_key,allowed,reason_code,source_band_of_first_admission,pooled_band_provenance,annotation) VALUES (?,?,?,?,?,?,?,?,?,?);");

        GambitCompanionWriteStats stats;
        stats.payload_file = "data/risky_acceptance_companion.sqlite";

        int total_candidates = 0;
        for (const auto& pos : positions) total_candidates += static_cast<int>(pos.candidate_moves.size());

        for (const auto& policy : config.gambit_utility.continuation_policies) {
            if (progress) progress->update([&](ProgressSnapshot& s) { s.risky_current_policy = policy; });
            for (const auto& pos : positions) {
                const int supported_replies = std::max(0, static_cast<int>(pos.candidate_moves.size()) - 1);
                std::vector<double> accepted_sigmas;
                std::vector<double> accepted_means;
                int accepted_sample_sum = 0;

                for (std::size_t i = 0; i < pos.candidate_moves.size(); ++i) {
                    const auto& mv = pos.candidate_moves[i];
                    const auto grade = grade_for_rank(i);
                    const bool accepted = is_accepted_grade(grade, policy);
                    const bool ordinary_fail = !accepted;
                    bind_text(ordinary_stmt.get(), 1, band_id);
                    bind_text(ordinary_stmt.get(), 2, policy);
                    bind_text(ordinary_stmt.get(), 3, pos.position_key);
                    bind_text(ordinary_stmt.get(), 4, mv.move_key);
                    bind_text(ordinary_stmt.get(), 5, grade);
                    bind_int(ordinary_stmt.get(), 6, accepted ? 1 : 0);
                    bind_int(ordinary_stmt.get(), 7, ordinary_fail ? 1 : 0);
                    bind_int(ordinary_stmt.get(), 8, mv.raw_count);
                    step(db.get(), ordinary_stmt.get());
                    ++stats.ordinary_rows;

                    if (accepted) {
                        const double mu = pos.total_observations > 0 ? static_cast<double>(mv.raw_count) / static_cast<double>(pos.total_observations) : 0.0;
                        accepted_means.push_back(mu);
                        accepted_sigmas.push_back(sigma_from_probability(mu));
                        accepted_sample_sum += mv.raw_count;
                    }
                }

                double mu_base = 0.0;
                double sigma_base = 0.0;
                int comparator_count = 0;
                if (!accepted_sigmas.empty()) {
                    std::vector<double> ordered = accepted_sigmas;
                    std::sort(ordered.begin(), ordered.end());
                    const double median_sigma = ordered[ordered.size() / 2];
                    for (std::size_t i = 0; i < accepted_sigmas.size(); ++i) {
                        if (accepted_sigmas[i] <= median_sigma) {
                            mu_base += accepted_means[i];
                            sigma_base += accepted_sigmas[i];
                            ++comparator_count;
                        }
                    }
                    if (comparator_count > 0) {
                        mu_base /= static_cast<double>(comparator_count);
                        sigma_base /= static_cast<double>(comparator_count);
                    }
                }

                bind_text(baseline_stmt.get(), 1, band_id);
                bind_text(baseline_stmt.get(), 2, policy);
                bind_text(baseline_stmt.get(), 3, "risky_sharp");
                bind_text(baseline_stmt.get(), 4, pos.position_key);
                bind_int(baseline_stmt.get(), 5, comparator_count);
                bind_int(baseline_stmt.get(), 6, accepted_sample_sum);
                bind_double(baseline_stmt.get(), 7, mu_base);
                bind_double(baseline_stmt.get(), 8, sigma_base);
                step(db.get(), baseline_stmt.get());
                ++stats.baseline_rows;

                for (std::size_t i = 0; i < pos.candidate_moves.size(); ++i) {
                    const auto& mv = pos.candidate_moves[i];
                    const bool accepted = is_accepted_grade(grade_for_rank(i), policy);
                    if (accepted) continue; // candidate narrowing: evaluate ordinary fails only

                    const double mu_candidate = pos.total_observations > 0 ? static_cast<double>(mv.raw_count) / static_cast<double>(pos.total_observations) : 0.0;
                    const double sigma_candidate = sigma_from_probability(mu_candidate);
                    const double variance_surplus = sigma_candidate - sigma_base;
                    const double mean_penalty = mu_base - mu_candidate;
                    const double optimistic_ceiling = mu_candidate + config.gambit_utility.k * sigma_candidate;
                    const double baseline_ceiling = mu_base + config.gambit_utility.k * sigma_base;

                    std::string adequacy = "adequate";
                    std::string reason = "rejected";
                    bool allowed = false;

                    if (mv.raw_count < config.gambit_utility.candidate_min_support) {
                        adequacy = "insufficient_candidate_support";
                        reason = "candidate_support_below_floor";
                        ++stats.unresolved_rows;
                    } else if (mv.raw_count < config.gambit_utility.n_min || supported_replies < config.gambit_utility.r_min || comparator_count == 0) {
                        adequacy = "insufficient_support";
                        reason = comparator_count == 0 ? "missing_stable_baseline" : "insufficient_support";
                        ++stats.unresolved_rows;
                    } else {
                        const bool sigma_gate = variance_surplus >= config.gambit_utility.t_sigma;
                        const bool mean_gate = mean_penalty <= config.gambit_utility.delta_max;
                        const bool optimistic_gate = optimistic_ceiling >= baseline_ceiling;
                        const bool floor_gate = mu_candidate >= config.gambit_utility.mu_floor;
                        allowed = sigma_gate && mean_gate && optimistic_gate && floor_gate;
                        if (allowed) {
                            reason = "admitted";
                            ++stats.admitted_rows;
                        } else {
                            reason = !sigma_gate ? "variance_surplus_gate_failed"
                                   : !mean_gate ? "mean_penalty_gate_failed"
                                   : !optimistic_gate ? "optimistic_ceiling_gate_failed"
                                   : "absolute_floor_gate_failed";
                            ++stats.rejected_rows;
                        }
                    }

                    bind_text(metrics_stmt.get(), 1, band_id);
                    bind_text(metrics_stmt.get(), 2, policy);
                    bind_text(metrics_stmt.get(), 3, "risky_sharp");
                    bind_text(metrics_stmt.get(), 4, pos.position_key);
                    bind_text(metrics_stmt.get(), 5, mv.move_key);
                    bind_int(metrics_stmt.get(), 6, mv.raw_count);
                    bind_int(metrics_stmt.get(), 7, supported_replies);
                    bind_text(metrics_stmt.get(), 8, band_id);
                    bind_double(metrics_stmt.get(), 9, mu_candidate);
                    bind_double(metrics_stmt.get(), 10, sigma_candidate);
                    bind_double(metrics_stmt.get(), 11, mu_base);
                    bind_double(metrics_stmt.get(), 12, sigma_base);
                    bind_double(metrics_stmt.get(), 13, variance_surplus);
                    bind_double(metrics_stmt.get(), 14, mean_penalty);
                    bind_double(metrics_stmt.get(), 15, optimistic_ceiling);
                    bind_text(metrics_stmt.get(), 16, adequacy);
                    bind_text(metrics_stmt.get(), 17, reason);
                    step(db.get(), metrics_stmt.get());
                    ++stats.metrics_rows;

                    bind_text(acceptance_stmt.get(), 1, band_id);
                    bind_text(acceptance_stmt.get(), 2, policy);
                    bind_text(acceptance_stmt.get(), 3, "risky_sharp");
                    bind_text(acceptance_stmt.get(), 4, pos.position_key);
                    bind_text(acceptance_stmt.get(), 5, mv.move_key);
                    bind_int(acceptance_stmt.get(), 6, allowed ? 1 : 0);
                    bind_text(acceptance_stmt.get(), 7, reason);
                    if (allowed) bind_text(acceptance_stmt.get(), 8, band_id); else sqlite3_bind_null(acceptance_stmt.get(), 8);
                    bind_text(acceptance_stmt.get(), 9, band_id);
                    bind_text(acceptance_stmt.get(), 10, "builder-local ordinary accepted continuation oracle");
                    step(db.get(), acceptance_stmt.get());
                    ++stats.acceptance_rows;

                    if (progress) {
                        progress->update([&](ProgressSnapshot& s) {
                            ++s.risky_candidate_fails_considered;
                            if (reason == "candidate_support_below_floor") ++s.risky_candidates_skipped_support;
                            else ++s.risky_candidates_evaluated;
                            s.risky_positions_considered = static_cast<int>(positions.size());
                            s.risky_admitted_rows = stats.admitted_rows;
                            s.risky_rejected_rows = stats.rejected_rows;
                            s.risky_unresolved_rows = stats.unresolved_rows;
                            s.risky_pooling_events = 0;
                            s.risky_memo_hit_rate = 0.0;
                            s.risky_estimated_remaining_work = std::max(0, total_candidates - s.risky_candidate_fails_considered);
                            s.last_event_message = "risky overlay evaluation active";
                        });
                    }
                }
            }
        }

        exec_sql(db.get(), "COMMIT;");
        if (progress) {
            progress->stage_completed(
                "risky overlay complete candidates=" + std::to_string(stats.metrics_rows) +
                " admitted=" + std::to_string(stats.admitted_rows) +
                " rejected=" + std::to_string(stats.rejected_rows) +
                " unresolved=" + std::to_string(stats.unresolved_rows));
        }
        return stats;
    } catch (...) {
        exec_sql(db.get(), "ROLLBACK;");
        if (progress) progress->stage_failed("risky overlay failed");
        throw;
    }
}

}  // namespace otcb
