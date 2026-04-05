#include "otcb/gambit_companion.hpp"

#include <sqlite3.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <tuple>
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

double sigma_from_probability(double p) {
    const double var = std::max(0.0, p * (1.0 - p));
    return std::sqrt(var);
}

std::string band_id_for_config(const BuildConfig& config) {
    return std::to_string(config.min_rating) + "-" + std::to_string(config.max_rating);
}

std::vector<std::string> enabled_scopes(const BuildConfig& config) {
    std::vector<std::string> scopes;
    if (config.gambit_utility.emit_scope_risky_gambit) scopes.push_back("risky_gambit");
    if (config.gambit_utility.emit_scope_risky_sharp) scopes.push_back("risky_sharp");
    return scopes;
}

}  // namespace

RiskyOverlayWriteStats write_gambit_companion_sqlite(const std::filesystem::path& sqlite_path,
                                                      const BuildConfig& config,
                                                      const AggregationSummary& summary,
                                                      const std::vector<AggregatedPositionRecord>& positions,
                                                      ProgressReporter* progress) {
    if (std::filesystem::exists(sqlite_path)) std::filesystem::remove(sqlite_path);
    SqliteDb db(sqlite_path);
    exec_sql(db.get(), "BEGIN TRANSACTION;");

    const std::string band_id = band_id_for_config(config);
    const auto scopes = enabled_scopes(config);
    const auto families = known_gambit_families();

    if (progress) {
        progress->stage_started(ProgressStage::ComputeRiskyOverlay, "computing risky companion overlay", summary.source_size_bytes);
        progress->update([&](ProgressSnapshot& snapshot) {
            snapshot.risky_current_band = band_id;
            snapshot.risky_current_policy.clear();
            snapshot.risky_current_scope.clear();
            snapshot.risky_positions_considered = 0;
            snapshot.risky_candidate_fails_considered = 0;
            snapshot.risky_candidates_skipped_support = 0;
            snapshot.risky_candidates_evaluated = 0;
            snapshot.risky_admitted_rows = 0;
            snapshot.risky_unresolved_rows = 0;
            snapshot.risky_rejected_rows = 0;
            snapshot.risky_pooling_events = 0;
            snapshot.risky_memo_hit_rate = 0.0;
            snapshot.risky_estimated_remaining_work = 0;
        });
    }

    try {
        exec_sql(db.get(),
            "CREATE TABLE companion_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "CREATE TABLE ordinary_move_acceptance_by_band(band_id TEXT NOT NULL, policy_scope TEXT NOT NULL, position_key TEXT NOT NULL, move_key TEXT NOT NULL, grade TEXT NOT NULL, book_source_flag INTEGER NOT NULL, explainability_note TEXT, PRIMARY KEY(band_id, policy_scope, position_key, move_key));"
            "CREATE TABLE opening_variance_baseline_by_band(band_id TEXT PRIMARY KEY, baseline_sigma REAL NOT NULL, sample_count INTEGER NOT NULL, comparator_count INTEGER NOT NULL, config_provenance TEXT NOT NULL);"
            "CREATE TABLE sharp_move_annotation(position_key TEXT NOT NULL, move_key TEXT NOT NULL, family_id TEXT, family_name TEXT, opening_metadata TEXT, PRIMARY KEY(position_key, move_key));"
            "CREATE TABLE risky_entry_metrics(band_id TEXT NOT NULL, policy_variant TEXT NOT NULL, scope_variant TEXT NOT NULL, position_key TEXT NOT NULL, move_key TEXT NOT NULL, sample_count INTEGER NOT NULL, pooled_bands TEXT NOT NULL, mu_candidate REAL NOT NULL, sigma_candidate REAL NOT NULL, mu_stable REAL NOT NULL, sigma_band REAL NOT NULL, u_candidate REAL NOT NULL, u_stable REAL NOT NULL, confidence_lower_bound REAL NOT NULL, adequacy_status TEXT NOT NULL, resolution_reason_code TEXT NOT NULL, PRIMARY KEY(band_id, policy_variant, scope_variant, position_key, move_key));"
            "CREATE TABLE risky_acceptance_by_band(band_id TEXT NOT NULL, policy_variant TEXT NOT NULL, scope_variant TEXT NOT NULL, position_key TEXT NOT NULL, move_key TEXT NOT NULL, allowed INTEGER NOT NULL, reason_code TEXT NOT NULL, source_band_of_first_admission TEXT, pooled_band_mask TEXT NOT NULL, family_id_annotation TEXT, family_name_annotation TEXT, PRIMARY KEY(band_id, policy_variant, scope_variant, position_key, move_key));"
            "CREATE VIEW risky_acceptance_audit AS SELECT ra.band_id, ra.policy_variant, ra.scope_variant, ra.position_key, ra.move_key, rm.sample_count, rm.mu_candidate, rm.sigma_candidate, rm.mu_stable, rm.sigma_band, rm.confidence_lower_bound, ra.allowed, ra.reason_code, ra.family_name_annotation FROM risky_acceptance_by_band ra JOIN risky_entry_metrics rm ON rm.band_id=ra.band_id AND rm.policy_variant=ra.policy_variant AND rm.scope_variant=ra.scope_variant AND rm.position_key=ra.position_key AND rm.move_key=ra.move_key;");

        Statement meta_stmt(db.get(), "INSERT INTO companion_metadata(key,value) VALUES (?,?);");
        const std::vector<std::pair<std::string, std::string>> meta = {
            {"artifact_schema_version", "otcb_risky_overlay_companion_v2"},
            {"retained_opening_window_scope", "true"},
            {"full_external_opening_book_used", "false"},
            {"ordinary_acceptance_oracle", "builder_local_rank_frequency_placeholder"},
            {"horizon_plies", std::to_string(config.gambit_utility.horizon_plies)},
            {"candidate_min_support", std::to_string(config.gambit_utility.candidate_min_support)},
            {"t_sigma", std::to_string(config.gambit_utility.t_sigma)},
            {"delta_max", std::to_string(config.gambit_utility.delta_max)},
            {"n_min", std::to_string(config.gambit_utility.n_min)},
            {"significance_z", std::to_string(config.gambit_utility.significance_z)},
            {"eligibility_policy", to_string(*config.rating_policy)},
            {"scope_variants", scopes.empty() ? "" : (scopes.size() == 2 ? "risky_gambit,risky_sharp" : scopes.front())},
            {"continuation_policies", config.gambit_utility.continuation_policies.empty() ? "" : config.gambit_utility.continuation_policies.front()},
        };
        for (const auto& [k, v] : meta) {
            bind_text(meta_stmt.get(), 1, k);
            bind_text(meta_stmt.get(), 2, v);
            step(db.get(), meta_stmt.get());
            reset_statement(meta_stmt.get());
        }

        std::map<std::pair<std::string, std::string>, FamilyDef> family_by_entry;
        for (const auto& f : families) family_by_entry[{f.root, f.entry}] = f;

        Statement ordinary_stmt(db.get(), "INSERT INTO ordinary_move_acceptance_by_band(band_id,policy_scope,position_key,move_key,grade,book_source_flag,explainability_note) VALUES (?,?,?,?,?,?,?);");
        Statement annotation_stmt(db.get(), "INSERT OR REPLACE INTO sharp_move_annotation(position_key,move_key,family_id,family_name,opening_metadata) VALUES (?,?,?,?,?);");
        Statement baseline_stmt(db.get(), "INSERT INTO opening_variance_baseline_by_band(band_id,baseline_sigma,sample_count,comparator_count,config_provenance) VALUES (?,?,?,?,?);");
        Statement metrics_stmt(db.get(), "INSERT INTO risky_entry_metrics(band_id,policy_variant,scope_variant,position_key,move_key,sample_count,pooled_bands,mu_candidate,sigma_candidate,mu_stable,sigma_band,u_candidate,u_stable,confidence_lower_bound,adequacy_status,resolution_reason_code) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
        Statement acceptance_stmt(db.get(), "INSERT INTO risky_acceptance_by_band(band_id,policy_variant,scope_variant,position_key,move_key,allowed,reason_code,source_band_of_first_admission,pooled_band_mask,family_id_annotation,family_name_annotation) VALUES (?,?,?,?,?,?,?,?,?,?,?);");

        RiskyOverlayWriteStats stats;
        stats.payload_file = "data/gambit_acceptance_companion.sqlite";

        std::map<std::pair<std::string, std::string>, std::string> grade_by_move;
        std::vector<double> accepted_non_fail_sigmas;

        int total_fail_candidates = 0;
        int estimated_work = 0;
        for (const auto& pos : positions) {
            for (std::size_t i = 0; i < pos.candidate_moves.size(); ++i) {
                if (grade_for_rank(i) == "fail") {
                    ++total_fail_candidates;
                    if (pos.candidate_moves[i].raw_count >= config.gambit_utility.candidate_min_support) {
                        estimated_work += static_cast<int>(config.gambit_utility.continuation_policies.size() * std::max<std::size_t>(1, scopes.size()));
                    }
                }
            }
        }

        for (const auto& pos : positions) {
            if (progress) {
                progress->update([&](ProgressSnapshot& snapshot) {
                    snapshot.risky_positions_considered += 1;
                    snapshot.risky_estimated_remaining_work = std::max(0, estimated_work - snapshot.risky_candidates_evaluated);
                });
            }
            for (std::size_t i = 0; i < pos.candidate_moves.size(); ++i) {
                const auto& move = pos.candidate_moves[i];
                const std::string grade = grade_for_rank(i);
                grade_by_move[{pos.position_key, move.move_key}] = grade;
                const double p = pos.total_observations > 0 ? static_cast<double>(move.raw_count) / static_cast<double>(pos.total_observations) : 0.0;
                if (grade != "fail") accepted_non_fail_sigmas.push_back(sigma_from_probability(p));

                for (const auto& policy : config.gambit_utility.continuation_policies) {
                    bind_text(ordinary_stmt.get(), 1, band_id);
                    bind_text(ordinary_stmt.get(), 2, policy);
                    bind_text(ordinary_stmt.get(), 3, pos.position_key);
                    bind_text(ordinary_stmt.get(), 4, move.move_key);
                    bind_text(ordinary_stmt.get(), 5, grade);
                    bind_int(ordinary_stmt.get(), 6, i == 0 ? 1 : 0);
                    bind_text(ordinary_stmt.get(), 7, "builder_local_ordinary_oracle");
                    step(db.get(), ordinary_stmt.get());
                    reset_statement(ordinary_stmt.get());
                    ++stats.ordinary_rows;
                }

                const auto fam_it = family_by_entry.find({pos.position_key, move.move_key});
                bind_text(annotation_stmt.get(), 1, pos.position_key);
                bind_text(annotation_stmt.get(), 2, move.move_key);
                if (fam_it != family_by_entry.end()) {
                    bind_text(annotation_stmt.get(), 3, fam_it->second.id);
                    bind_text(annotation_stmt.get(), 4, fam_it->second.name);
                    bind_text(annotation_stmt.get(), 5, std::string("{\"eco\":\"") + fam_it->second.eco + "\"}");
                } else {
                    sqlite3_bind_null(annotation_stmt.get(), 3);
                    sqlite3_bind_null(annotation_stmt.get(), 4);
                    sqlite3_bind_null(annotation_stmt.get(), 5);
                }
                step(db.get(), annotation_stmt.get());
                reset_statement(annotation_stmt.get());
                ++stats.annotation_rows;
            }
        }

        std::sort(accepted_non_fail_sigmas.begin(), accepted_non_fail_sigmas.end());
        const double baseline_sigma = accepted_non_fail_sigmas.empty() ? 0.0 : accepted_non_fail_sigmas[accepted_non_fail_sigmas.size() / 2];

        bind_text(baseline_stmt.get(), 1, band_id);
        bind_double(baseline_stmt.get(), 2, baseline_sigma);
        bind_int(baseline_stmt.get(), 3, static_cast<int>(accepted_non_fail_sigmas.size()));
        bind_int(baseline_stmt.get(), 4, static_cast<int>(accepted_non_fail_sigmas.size()));
        bind_text(baseline_stmt.get(), 5, "median_sigma_non_fail_opening_window");
        step(db.get(), baseline_stmt.get());
        ++stats.baseline_rows;

        // Candidate narrowing: fail-only observed moves with support floor.
        std::set<std::tuple<std::string, std::string, std::string>> memo_keys;
        int memo_hits = 0;
        int memo_queries = 0;
        for (const auto& pos : positions) {
            for (std::size_t i = 0; i < pos.candidate_moves.size(); ++i) {
                const auto& candidate = pos.candidate_moves[i];
                const std::string grade = grade_for_rank(i);
                if (grade != "fail") continue;
                if (progress) {
                    progress->update([&](ProgressSnapshot& snapshot) {
                        snapshot.risky_candidate_fails_considered += 1;
                    });
                }
                if (candidate.raw_count < config.gambit_utility.candidate_min_support) {
                    ++stats.unresolved_rows;
                    if (progress) {
                        progress->update([&](ProgressSnapshot& snapshot) {
                            snapshot.risky_candidates_skipped_support += 1;
                            snapshot.risky_unresolved_rows += static_cast<int>(config.gambit_utility.continuation_policies.size() * std::max<std::size_t>(1, scopes.size()));
                        });
                    }
                    continue;
                }

                const bool is_gambit_like = family_by_entry.find({pos.position_key, candidate.move_key}) != family_by_entry.end();
                const double mu_candidate = pos.total_observations > 0 ? static_cast<double>(candidate.raw_count) / static_cast<double>(pos.total_observations) : 0.0;
                const double sigma_candidate = sigma_from_probability(mu_candidate);

                double mu_stable = 0.0;
                int stable_count = 0;
                for (const auto& sibling : pos.candidate_moves) {
                    if (sibling.move_key == candidate.move_key) continue;
                    const auto g_it = grade_by_move.find({pos.position_key, sibling.move_key});
                    if (g_it == grade_by_move.end()) continue;
                    const double sibling_p = pos.total_observations > 0 ? static_cast<double>(sibling.raw_count) / static_cast<double>(pos.total_observations) : 0.0;
                    const double sibling_sigma = sigma_from_probability(sibling_p);
                    if (sibling_sigma <= baseline_sigma && g_it->second != "fail") {
                        mu_stable += sibling_p;
                        ++stable_count;
                    }
                }
                if (stable_count > 0) mu_stable /= static_cast<double>(stable_count);

                for (const auto& policy : config.gambit_utility.continuation_policies) {
                    for (const auto& scope : scopes) {
                        if (scope == "risky_gambit" && !is_gambit_like) {
                            continue;
                        }
                        if (progress) {
                            progress->update([&](ProgressSnapshot& snapshot) {
                                snapshot.risky_current_policy = policy;
                                snapshot.risky_current_scope = scope;
                            });
                        }

                        // Memo key by (band,policy,scope,position,remaining_depth) simplified using root candidate.
                        const auto memo_key = std::make_tuple(policy, scope, pos.position_key + "|" + candidate.move_key + "|" + std::to_string(config.gambit_utility.horizon_plies));
                        ++memo_queries;
                        if (!memo_keys.insert(memo_key).second) ++memo_hits;

                        const int n = candidate.raw_count;
                        const double u_candidate = mu_candidate + sigma_candidate;
                        const double u_stable = mu_stable + baseline_sigma;
                        const bool adequate = n >= config.gambit_utility.n_min;
                        const bool sigma_gate = sigma_candidate >= baseline_sigma + config.gambit_utility.t_sigma;
                        const bool mu_gate = mu_candidate >= mu_stable - config.gambit_utility.delta_max;
                        const bool upper_gate = u_candidate >= u_stable;
                        const double se = n > 0 ? sigma_candidate / std::sqrt(static_cast<double>(n)) : 1.0;
                        const double lcb = (u_candidate - u_stable) - config.gambit_utility.significance_z * se;
                        const bool admitted = adequate && sigma_gate && mu_gate && upper_gate && lcb > 0.0;
                        const std::string adequacy_status = adequate ? "adequate" : "insufficient";
                        const std::string reason = admitted ? "admitted" : (adequate ? "rejected_threshold" : "unresolved_insufficient_sample");

                        bind_text(metrics_stmt.get(), 1, band_id);
                        bind_text(metrics_stmt.get(), 2, policy);
                        bind_text(metrics_stmt.get(), 3, scope);
                        bind_text(metrics_stmt.get(), 4, pos.position_key);
                        bind_text(metrics_stmt.get(), 5, candidate.move_key);
                        bind_int(metrics_stmt.get(), 6, n);
                        bind_text(metrics_stmt.get(), 7, band_id);
                        bind_double(metrics_stmt.get(), 8, mu_candidate);
                        bind_double(metrics_stmt.get(), 9, sigma_candidate);
                        bind_double(metrics_stmt.get(), 10, mu_stable);
                        bind_double(metrics_stmt.get(), 11, baseline_sigma);
                        bind_double(metrics_stmt.get(), 12, u_candidate);
                        bind_double(metrics_stmt.get(), 13, u_stable);
                        bind_double(metrics_stmt.get(), 14, lcb);
                        bind_text(metrics_stmt.get(), 15, adequacy_status);
                        bind_text(metrics_stmt.get(), 16, reason);
                        step(db.get(), metrics_stmt.get());
                        reset_statement(metrics_stmt.get());

                        bind_text(acceptance_stmt.get(), 1, band_id);
                        bind_text(acceptance_stmt.get(), 2, policy);
                        bind_text(acceptance_stmt.get(), 3, scope);
                        bind_text(acceptance_stmt.get(), 4, pos.position_key);
                        bind_text(acceptance_stmt.get(), 5, candidate.move_key);
                        bind_int(acceptance_stmt.get(), 6, admitted ? 1 : 0);
                        bind_text(acceptance_stmt.get(), 7, reason);
                        if (admitted) bind_text(acceptance_stmt.get(), 8, band_id); else sqlite3_bind_null(acceptance_stmt.get(), 8);
                        bind_text(acceptance_stmt.get(), 9, band_id);
                        const auto fam_it = family_by_entry.find({pos.position_key, candidate.move_key});
                        if (fam_it != family_by_entry.end()) {
                            bind_text(acceptance_stmt.get(), 10, fam_it->second.id);
                            bind_text(acceptance_stmt.get(), 11, fam_it->second.name);
                        } else {
                            sqlite3_bind_null(acceptance_stmt.get(), 10);
                            sqlite3_bind_null(acceptance_stmt.get(), 11);
                        }
                        step(db.get(), acceptance_stmt.get());
                        reset_statement(acceptance_stmt.get());

                        ++stats.metrics_rows;
                        ++stats.acceptance_rows;
                        if (admitted) {
                            ++stats.admitted_rows;
                        } else if (!adequate) {
                            ++stats.unresolved_rows;
                        } else {
                            ++stats.rejected_rows;
                        }

                        if (progress) {
                            progress->update([&](ProgressSnapshot& snapshot) {
                                snapshot.risky_candidates_evaluated += 1;
                                snapshot.risky_admitted_rows = stats.admitted_rows;
                                snapshot.risky_unresolved_rows = stats.unresolved_rows;
                                snapshot.risky_rejected_rows = stats.rejected_rows;
                                snapshot.risky_memo_hit_rate = memo_queries > 0 ? static_cast<double>(memo_hits) / static_cast<double>(memo_queries) : 0.0;
                                snapshot.risky_estimated_remaining_work = std::max(0, estimated_work - snapshot.risky_candidates_evaluated);
                                const auto seconds = std::max(1.0, std::chrono::duration<double>(std::chrono::steady_clock::now() - snapshot.stage_started_at).count());
                                snapshot.throughput_per_second = snapshot.risky_candidates_evaluated / seconds;
                                snapshot.last_event_message = "risky overlay stage active";
                            });
                        }
                    }
                }
            }
        }

        exec_sql(db.get(), "COMMIT;");
        if (progress) {
            progress->stage_completed("risky overlay complete rows=" + std::to_string(stats.acceptance_rows) +
                                      " admitted=" + std::to_string(stats.admitted_rows) +
                                      " unresolved=" + std::to_string(stats.unresolved_rows) +
                                      " rejected=" + std::to_string(stats.rejected_rows));
        }
        return stats;
    } catch (...) {
        exec_sql(db.get(), "ROLLBACK;");
        if (progress) {
            progress->stage_failed("risky overlay failed");
        }
        throw;
    }
}

}  // namespace otcb
