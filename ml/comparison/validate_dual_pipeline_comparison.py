from pathlib import Path
import pandas as pd
from config.settings import ML_DATA_FOLDER

output_folder = Path(f"{ML_DATA_FOLDER}/comparison")
comparison_path = output_folder / "dual_pipeline_menu_comparison.parquet"
summary_path = output_folder / "dual_pipeline_comparison_summary.parquet"
folds_path = output_folder / "dual_pipeline_fold_assignments.parquet"
comparison_csv_path = output_folder / "dual_pipeline_menu_comparison.csv"
summary_csv_path = output_folder / "dual_pipeline_comparison_summary.csv"

checks = []

def check(name, condition, detail=""):
    passed = bool(condition)
    checks.append((name, passed, detail))
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {name}" + (f" - {detail}" if detail else ""))

print("========STEP 14 DUAL PIPELINE VALIDATION========")

check("Comparison Parquet exists", comparison_path.exists())
check("Summary Parquet exists", summary_path.exists())
check("Fold assignments Parquet exists", folds_path.exists())
check("Comparison CSV exists", comparison_csv_path.exists())
check("Summary CSV exists", summary_csv_path.exists())

if not all([comparison_path.exists(), summary_path.exists(), folds_path.exists()]):
    print("\nValidation stopped because required Parquet outputs are missing.")
    raise SystemExit(1)

comparison = pd.read_parquet(comparison_path)
summary = pd.read_parquet(summary_path)
folds = pd.read_parquet(folds_path)

required_comparison_columns = [
    "item_id", "item_name", "fold_id", "actual_class",
    "spark_prediction", "python_prediction",
    "spark_probability", "python_probability",
    "match_status", "probability_difference",
    "spark_correct", "python_correct",
    "final_consistency_status", "disagreement_explanation",
    "spark_model_version", "python_model_version", "comparison_version",
]
required_summary_columns = [
    "total_unseen_cases", "matches", "mismatches",
    "agreement_percentage",
    "spark_oof_accuracy",
    "python_oof_accuracy",
    "average_probability_difference",
]

for column in required_comparison_columns:
    check(f"Comparison column: {column}", column in comparison.columns)
for column in required_summary_columns:
    check(f"Summary column: {column}", column in summary.columns)

if not all(column in comparison.columns for column in required_comparison_columns):
    print("\nValidation stopped because comparison output is missing required columns.")
    raise SystemExit(1)
if not all(column in summary.columns for column in required_summary_columns):
    print("\nValidation stopped because summary output is missing required columns.")
    raise SystemExit(1)

total = len(comparison)
unique_items = comparison["item_id"].nunique()
matches = int((comparison["spark_prediction"] == comparison["python_prediction"]).sum())
mismatches = total - matches
agreement = matches / total * 100 if total else 0
spark_accuracy = (comparison["spark_prediction"] == comparison["actual_class"]).mean() * 100
python_accuracy = (comparison["python_prediction"] == comparison["actual_class"]).mean() * 100
calculated_difference = (comparison["spark_probability"] - comparison["python_probability"]).abs()

check("At least 100 unseen comparison cases", total >= 100, f"{total} cases")
check("One comparison row per item", unique_items == total, f"{unique_items}/{total} unique")
check("No missing actual classes", comparison["actual_class"].notna().all())
check("No missing Spark predictions", comparison["spark_prediction"].notna().all())
check("No missing Python predictions", comparison["python_prediction"].notna().all())
check("No missing Spark probabilities", comparison["spark_probability"].notna().all())
check("No missing Python probabilities", comparison["python_probability"].notna().all())
check("Spark probabilities are valid", comparison["spark_probability"].between(0, 1).all())
check("Python probabilities are valid", comparison["python_probability"].between(0, 1).all())

expected_match = comparison.apply(
    lambda row: "Match" if row["spark_prediction"] == row["python_prediction"] else "Mismatch",
    axis=1,
)
check("Match/mismatch labels are correct", (comparison["match_status"] == expected_match).all())
check("Probability differences are correct", (comparison["probability_difference"] - calculated_difference).abs().max() < 1e-9)
check("Spark correctness flags are correct", (comparison["spark_correct"] == (comparison["spark_prediction"] == comparison["actual_class"])).all())
check("Python correctness flags are correct", (comparison["python_correct"] == (comparison["python_prediction"] == comparison["actual_class"])).all())
check("All rows have final consistency status", comparison["final_consistency_status"].notna().all() and comparison["final_consistency_status"].astype(str).str.strip().ne("").all())

mismatch_explanations = comparison.loc[comparison["match_status"] == "Mismatch", "disagreement_explanation"]
check("All mismatches have disagreement explanations", mismatch_explanations.notna().all() and mismatch_explanations.astype(str).str.strip().ne("").all())

check("Five OOF folds are present", set(comparison["fold_id"].unique()) == {0, 1, 2, 3, 4})
check("Every OOF fold contains records", comparison.groupby("fold_id").size().reindex(range(5), fill_value=0).gt(0).all())
check("Fold assignment count matches comparison", len(folds) == total, f"{len(folds)} fold rows / {total} comparison rows")
check("Fold assignment IDs are unique", "item_id" in folds.columns and folds["item_id"].nunique() == len(folds))
check("Spark and Python model versions are recorded", comparison["spark_model_version"].notna().all() and comparison["python_model_version"].notna().all())
check("Spark and Python pipelines identify different model versions", (comparison["spark_model_version"].astype(str) != comparison["python_model_version"].astype(str)).all())
check("Comparison version is recorded", comparison["comparison_version"].notna().all() and comparison["comparison_version"].astype(str).str.strip().ne("").all())
check("Comparison contains real matches", matches > 0, f"{matches} matches")
check("Comparison contains real disagreements", mismatches > 0, f"{mismatches} mismatches")

summary_row = summary.iloc[0]
check("Summary unseen-case count is correct", int(summary_row["total_unseen_cases"]) == total)
check("Summary match count is correct", int(summary_row["matches"]) == matches)
check("Summary mismatch count is correct", int(summary_row["mismatches"]) == mismatches)
check("Summary agreement percentage is correct", abs(float(summary_row["agreement_percentage"]) - agreement) < 0.01, f"{agreement:.2f}%")
check("Summary Spark OOF accuracy is correct", abs(float(summary_row["spark_oof_accuracy"]) - spark_accuracy) < 0.01, f"{spark_accuracy:.2f}%")
check("Summary Python OOF accuracy is correct", abs(float(summary_row["python_oof_accuracy"]) - python_accuracy) < 0.01, f"{python_accuracy:.2f}%")
check("Summary average probability difference is correct", abs(float(summary_row["average_probability_difference"]) - calculated_difference.mean()) < 0.0001, f"{calculated_difference.mean():.4f}")

passed = sum(result for _, result, _ in checks)
failed = len(checks) - passed

print("\n========VALIDATION SUMMARY========")
print(f"Passed: {passed}/{len(checks)}")
print(f"Failed: {failed}/{len(checks)}")
print(f"Unseen Cases: {total}")
print(f"Matches: {matches}")
print(f"Mismatches: {mismatches}")
print(f"Overall Agreement: {agreement:.2f}%")
print(f"Spark OOF Accuracy: {spark_accuracy:.2f}%")
print(f"Python OOF Accuracy: {python_accuracy:.2f}%")

if failed == 0:
    print("\nSTEP 14 VALIDATION PASSED")
else:
    print("\nSTEP 14 VALIDATION FAILED")
    raise SystemExit(1)
