import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent
HISTORY_PATH = ROOT / "data" / "reports" / "pipeline_execution_history.json"


RESET_BEFORE_STEP = {
    "clean_data": [ROOT / "data" / "processed"],
    "integrate_data": [ROOT / "data" / "integrated"],
    "engineer_features": [ROOT / "data" / "features"],
    "run_eda": [ROOT / "data" / "eda"],
    "menu_profitability": [ROOT / "data" / "analytics"],
    "spark_menu_classification": [ROOT / "data" / "ml", ROOT / "models"]
}


STEPS = [
    ("generate_data", "Data", "data_generator/generate_data.py"),
    ("inject_quality_issues", "Data", "data_generator/inject_quality_issues.py"),
    ("assess_raw_quality", "Data", "spark/quality.py"),
    ("ingest_raw_data", "Data", "spark/ingest.py"),
    ("clean_data", "Data", "preprocessing/cleaning.py"),
    ("validate_cleaned_data", "Validation", "preprocessing/validate_cleaned.py"),
    ("integrate_data", "Data", "spark/integration.py"),
    ("validate_integrated_data", "Validation", "spark/validate_integration.py"),
    ("run_spark_sql", "Data", "spark/sql_queries.py"),
    ("engineer_features", "Data", "feature_engineering/feature_engineering.py"),
    ("validate_features", "Validation", "feature_engineering/validate_features.py"),
    ("run_eda", "Analytics", "eda/eda_analysis.py"),
    ("validate_eda", "Validation", "eda/validate_eda.py"),

    ("menu_profitability", "Analytics", "analytics/menu/menu_profitability.py"),
    ("validate_menu_profitability", "Validation", "analytics/menu/validate_menu_profitability.py"),
    ("menu_classification", "Analytics", "analytics/menu/menu_classification.py"),
    ("validate_menu_classification", "Validation", "analytics/menu/validate_menu_classification.py"),
    ("wastage_analysis", "Analytics", "analytics/wastage/wastage_analysis.py"),
    ("validate_wastage_analysis", "Validation", "analytics/wastage/validate_wastage_analysis.py"),
    ("tricky_menu_cases", "Analytics", "analytics/menu/tricky_menu_cases.py"),
    ("validate_tricky_menu_cases", "Validation", "analytics/menu/validate_tricky_menu_cases.py"),
    ("slow_moving_dishes", "Analytics", "analytics/menu/slow_moving_dishes.py"),
    ("validate_slow_moving_dishes", "Validation", "analytics/menu/validate_slow_moving_dishes.py"),
    ("peak_period_analysis", "Analytics", "analytics/peak_period/peak_period_analysis.py"),
    ("validate_peak_period_analysis", "Validation", "analytics/peak_period/validate_peak_period.py"),

    ("customer_segmentation", "Analytics", "analytics/customer/customer_segmentation.py"),
    ("validate_customer_segmentation", "Validation", "analytics/customer/validate_customer_segmentation.py"),
    ("rfm_analysis", "Analytics", "analytics/customer/rfm_analysis.py"),
    ("validate_rfm_analysis", "Validation", "analytics/customer/validate_rfm_analysis.py"),
    ("customer_churn_risk", "Analytics", "analytics/customer/customer_churn_risk.py"),
    ("validate_customer_churn_risk", "Validation", "analytics/customer/validate_customer_churn_risk.py"),
    ("market_basket_analysis", "Analytics", "analytics/market_basket/market_basket_analysis.py"),
    ("validate_market_basket_analysis", "Validation", "analytics/market_basket/validate_market_basket_analysis.py"),
    ("ordering_channel_analysis", "Analytics", "analytics/channels/ordering_channel_analysis.py"),
    ("validate_ordering_channel_analysis", "Validation", "analytics/channels/validate_ordering_channel_analysis.py"),

    ("price_intelligence", "Analytics", "analytics/pricing/price_intelligence.py"),
    ("validate_price_intelligence", "Validation", "analytics/pricing/validate_price_intelligence.py"),
    ("price_sensitivity", "Analytics", "analytics/pricing/price_sensitivity.py"),
    ("validate_price_sensitivity", "Validation", "analytics/pricing/validate_price_sensitivity.py"),
    ("promotion_effectiveness", "Analytics", "analytics/promotions/promotion_effectiveness.py"),
    ("validate_promotion_effectiveness", "Validation", "analytics/promotions/validate_promotion_effectiveness.py"),
    ("promotion_trap_detection", "Analytics", "analytics/promotions/promotion_trap_detection.py"),
    ("validate_promotion_trap_detection", "Validation", "analytics/promotions/validate_promotion_trap_detection.py"),
    ("rating_satisfaction", "Analytics", "analytics/ratings/rating_satisfaction.py"),
    ("validate_rating_satisfaction", "Validation", "analytics/ratings/validate_rating_satisfaction.py"),
    ("sales_anomaly_detection", "Analytics", "analytics/anomalies/sales_anomaly_detection.py"),
    ("validate_sales_anomaly_detection", "Validation", "analytics/anomalies/validate_sales_anomaly_detection.py"),
    ("rating_anomaly_detection", "Analytics", "analytics/anomalies/rating_anomaly_detection.py"),
    ("validate_rating_anomaly_detection", "Validation", "analytics/anomalies/validate_rating_anomaly_detection.py"),

    ("location_menu_performance", "Analytics", "analytics/locations/location_menu_performance.py"),
    ("validate_location_menu_performance", "Validation", "analytics/locations/validate_location_menu_performance.py"),
    ("multi_location_intelligence", "Analytics", "analytics/locations/multi_location_intelligence.py"),
    ("validate_multi_location_intelligence", "Validation", "analytics/locations/validate_multi_location_intelligence.py"),

    ("spark_menu_classification", "Machine Learning", "ml/spark/spark_menu_classification.py"),
    ("validate_spark_menu_classification", "Validation", "ml/spark/validate_spark_menu_classification.py"),
    ("python_menu_classification", "Machine Learning", "ml/python/python_menu_classification.py"),
    ("validate_python_menu_classification", "Validation", "ml/python/validate_python_menu_classification.py"),
    ("dual_pipeline_comparison", "Machine Learning", "ml/comparison/dual_pipeline_comparison.py"),
    ("validate_dual_pipeline_comparison", "Validation", "ml/comparison/validate_dual_pipeline_comparison.py"),

    ("demand_forecasting", "Machine Learning", "ml/forecasting/demand_forecasting.py"),
    ("validate_demand_forecasting", "Validation", "ml/forecasting/validate_demand_forecasting.py"),
    ("time_aware_forecast_validation", "Machine Learning", "ml/forecasting/time_aware_validation.py"),
    ("validate_time_aware_forecast", "Validation", "ml/forecasting/validate_time_aware_validation.py"),
    ("forecast_accuracy_evaluation", "Machine Learning", "ml/forecasting/forecast_accuracy_evaluation.py"),
    ("validate_forecast_accuracy", "Validation", "ml/forecasting/validate_forecast_accuracy_evaluation.py"),
    ("wastage_risk_prediction", "Machine Learning", "ml/wastage/wastage_risk_prediction.py"),
    ("validate_wastage_risk_prediction", "Validation", "ml/wastage/validate_wastage_risk_prediction.py"),

    ("bundle_recommendations", "Recommendations", "analytics/recommendations/bundle_recommendations.py"),
    ("validate_bundle_recommendations", "Validation", "analytics/recommendations/validate_bundle_recommendations.py"),
    ("recommendation_engine", "Recommendations", "analytics/recommendations/recommendation_engine.py"),
    ("validate_recommendation_engine", "Validation", "analytics/recommendations/validate_recommendation_engine.py"),
    ("recommendation_evidence", "Recommendations", "analytics/recommendations/recommendation_evidence.py"),
    ("validate_recommendation_evidence", "Validation", "analytics/recommendations/validate_recommendation_evidence.py"),
    ("recommendation_priority", "Recommendations", "analytics/recommendations/recommendation_priority.py"),
    ("validate_recommendation_priority", "Validation", "analytics/recommendations/validate_recommendation_priority.py"),

    ("what_if_scenario_analysis", "Scenarios", "analytics/scenarios/what_if_scenario_analysis.py"),
    ("validate_what_if_scenarios", "Validation", "analytics/scenarios/validate_what_if_scenario_analysis.py"),
    ("scenario_impact_analysis", "Scenarios", "analytics/scenarios/scenario_impact_analysis.py"),
    ("validate_scenario_impact", "Validation", "analytics/scenarios/validate_scenario_impact_analysis.py"),
]


def now_iso():
    return datetime.now().astimezone().isoformat(timespec="seconds")


def load_history():
    candidates = [HISTORY_PATH, HISTORY_PATH.with_suffix(".tmp")]
    candidates = [path for path in candidates if path.exists()]
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)

    for path in candidates:
        try:
            with path.open("r", encoding="utf-8") as file:
                data = json.load(file)
        except (json.JSONDecodeError, OSError):
            continue

        if isinstance(data, dict) and isinstance(data.get("runs"), list):
            return data

    return {"pipeline": "DineIQ full analytics pipeline", "runs": []}


def save_history(history):
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    history["updated_at"] = now_iso()
    temporary_path = HISTORY_PATH.with_suffix(".tmp")
    payload = json.dumps(history, indent=2) + "\n"
    temporary_path.write_text(payload, encoding="utf-8")

    for attempt in range(8):
        try:
            temporary_path.replace(HISTORY_PATH)
            return
        except PermissionError:
            if attempt < 7:
                time.sleep(0.25)

    try:
        HISTORY_PATH.write_text(payload, encoding="utf-8")
        temporary_path.unlink(missing_ok=True)
    except PermissionError:
        print(
            f"Warning: {HISTORY_PATH.name} is temporarily locked. "
            f"The latest history remains in {temporary_path.name}."
        )


def parse_args():
    step_names = [step[0] for step in STEPS]
    parser = argparse.ArgumentParser(
        description="Run the complete DineIQ data, analytics, ML and validation pipeline."
    )
    parser.add_argument(
        "--from-step",
        choices=step_names,
        help="Resume the pipeline from this step."
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all steps without running them."
    )
    return parser.parse_args()


def selected_steps(from_step):
    if from_step is None:
        return STEPS

    start_index = next(
        index for index, step in enumerate(STEPS) if step[0] == from_step
    )
    return STEPS[start_index:]


def validate_step_files(steps):
    missing = [relative_path for _, _, relative_path in steps if not (ROOT / relative_path).is_file()]
    if missing:
        print("Missing pipeline files:")
        for relative_path in missing:
            print(f"  - {relative_path}")
        return False
    return True


def reset_step_outputs(step_name):
    for target in RESET_BEFORE_STEP.get(step_name, []):
        resolved_target = target.resolve()
        try:
            resolved_target.relative_to(ROOT)
        except ValueError as error:
            raise RuntimeError(f"Refusing to clear path outside the project: {resolved_target}") from error

        if resolved_target == ROOT:
            raise RuntimeError("Refusing to clear the project root.")

        if resolved_target.exists():
            shutil.rmtree(resolved_target)


def print_steps(steps):
    for index, (name, group, relative_path) in enumerate(steps, start=1):
        print(f"{index:02d}. [{group}] {name}: {relative_path}")


def main():
    args = parse_args()

    if args.list:
        print_steps(STEPS)
        return 0

    steps = selected_steps(args.from_step)
    if not validate_step_files(steps):
        return 2

    history = load_history()
    run = {
        "run_id": uuid.uuid4().hex,
        "command": " ".join(sys.argv),
        "python_executable": sys.executable,
        "working_directory": str(ROOT),
        "started_at": now_iso(),
        "finished_at": None,
        "duration_seconds": None,
        "status": "running",
        "start_step": steps[0][0],
        "total_steps": len(steps),
        "completed_steps": 0,
        "failed_step": None,
        "steps": []
    }
    history["runs"].append(run)
    save_history(history)

    pipeline_started = time.perf_counter()
    environment = os.environ.copy()
    environment["PYTHONUNBUFFERED"] = "1"
    existing_python_path = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        str(ROOT)
        if not existing_python_path
        else f"{ROOT}{os.pathsep}{existing_python_path}"
    )

    print(f"DineIQ pipeline run: {run['run_id']}")
    print(f"Steps: {len(steps)}")
    print(f"History: {HISTORY_PATH}")

    try:
        for index, (name, group, relative_path) in enumerate(steps, start=1):
            script_path = ROOT / relative_path
            step = {
                "position": index,
                "name": name,
                "group": group,
                "script": relative_path,
                "started_at": now_iso(),
                "finished_at": None,
                "duration_seconds": None,
                "status": "running",
                "exit_code": None,
                "error": None
            }
            run["steps"].append(step)
            save_history(history)

            print()
            print("=" * 78)
            print(f"[{index}/{len(steps)}] {name}")
            print(relative_path)
            print("=" * 78)

            step_started = time.perf_counter()
            try:
                reset_step_outputs(name)
            except OSError as error:
                step["duration_seconds"] = round(time.perf_counter() - step_started, 3)
                step["finished_at"] = now_iso()
                step["status"] = "failed"
                step["error"] = str(error)
                run["status"] = "failed"
                run["failed_step"] = name
                run["finished_at"] = now_iso()
                run["duration_seconds"] = round(time.perf_counter() - pipeline_started, 3)
                save_history(history)
                print(f"Unable to clear output for {name}: {error}")
                print("Close the API, Spark, or file-preview process using that folder and retry.")
                return 1

            result = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=ROOT,
                env=environment
            )
            step["duration_seconds"] = round(time.perf_counter() - step_started, 3)
            step["finished_at"] = now_iso()
            step["exit_code"] = result.returncode
            step["status"] = "completed" if result.returncode == 0 else "failed"

            if result.returncode != 0:
                run["status"] = "failed"
                run["failed_step"] = name
                run["finished_at"] = now_iso()
                run["duration_seconds"] = round(time.perf_counter() - pipeline_started, 3)
                save_history(history)
                print()
                print(f"Pipeline stopped: {name} failed with exit code {result.returncode}.")
                print(f"Resume with: uv run python run_pipeline.py --from-step {name}")
                return result.returncode or 1

            run["completed_steps"] += 1
            save_history(history)
            print(f"Completed {name} in {step['duration_seconds']:.3f} seconds.")

    except KeyboardInterrupt:
        if run["steps"] and run["steps"][-1]["status"] == "running":
            current = run["steps"][-1]
            current["status"] = "cancelled"
            current["finished_at"] = now_iso()
            current["duration_seconds"] = round(
                time.perf_counter() - step_started,
                3
            )
            run["failed_step"] = current["name"]
        run["status"] = "cancelled"
        run["finished_at"] = now_iso()
        run["duration_seconds"] = round(time.perf_counter() - pipeline_started, 3)
        save_history(history)
        print("\nPipeline cancelled.")
        return 130

    run["status"] = "completed"
    run["finished_at"] = now_iso()
    run["duration_seconds"] = round(time.perf_counter() - pipeline_started, 3)
    save_history(history)

    print()
    print("DineIQ pipeline completed successfully.")
    print(f"Total time: {run['duration_seconds']:.3f} seconds")
    print(f"History: {HISTORY_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
