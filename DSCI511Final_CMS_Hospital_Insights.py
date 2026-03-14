import os
import pandas as pd
from datetime import datetime

script_dir = os.path.dirname(os.path.abspath(__file__))
stage_dir = script_dir
derived_dir = script_dir
final_dir = script_dir

os.makedirs(final_dir, exist_ok=True)

unplanned_file = os.path.join(stage_dir, "dsci511proj_stage_unplanned_visits.csv")
dim_hospital_file = os.path.join(derived_dir, "dsci511proj_dim_hospital.csv")
agg_hcahps_file = os.path.join(derived_dir, "dsci511proj_agg_hcahps_hospital.csv")
dim_zip_prevent_med_file = os.path.join(derived_dir, "dsci511proj_dim_zip_prevent_med.csv")
final_output_file = os.path.join(final_dir, "CMS Hospital Insights Agg Output.csv")

dataset_name_value = "dsci511proj_hospital_insights"
dataset_version_value = "v1"
pipeline_run_date_value = datetime.today().strftime("%Y-%m-%d")
source_unplanned_dataset_id_value = "632h-zaca"
source_hospital_info_dataset_id_value = "xubh-q36u"
source_hcahps_dataset_id_value = "dgck-syfz"
source_preventive_dataset_id_value = "0330-b6e0"

def load_csv(file_path):
    return pd.read_csv(file_path, low_memory=False)
def print_table_summary(df, table_name):
    print(f"\nTable: {table_name}")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
def ensure_required_columns(df, required_columns, table_name):
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"mMssing Required COlumns From: {table_name}: {missing_columns}")
def reorder_columns(df, preferred_order):
    existing_preferred = [col for col in preferred_order if col in df.columns]
    remaining = [col for col in df.columns if col not in existing_preferred]
    return df[existing_preferred + remaining]
def main():
    print("Loading Datasets...")
    print(f"Project Output Folder: {final_dir}")
    unplanned_df = load_csv(unplanned_file)
    dim_hospital_df = load_csv(dim_hospital_file)
    agg_hcahps_df = load_csv(agg_hcahps_file)
    dim_zip_prevent_med_df = load_csv(dim_zip_prevent_med_file)
    print_table_summary(unplanned_df, "dsci511proj_stage_unplanned_visits")
    print_table_summary(dim_hospital_df, "dsci511proj_dim_hospital")
    print_table_summary(agg_hcahps_df, "dsci511proj_agg_hcahps_hospital")
    print_table_summary(dim_zip_prevent_med_df, "dsci511proj_dim_zip_prevent_med")
    ensure_required_columns(
        unplanned_df,
        ["facility_id", "measure_id", "measure_name"],
        "dsci511proj_stage_unplanned_visits")
    ensure_required_columns(
        dim_hospital_df,
        ["facility_id"],
        "dsci511proj_dim_hospital")
    ensure_required_columns(
        agg_hcahps_df,
        ["facility_id"],
        "dsci511proj_agg_hcahps_hospital")
    ensure_required_columns(
        dim_zip_prevent_med_df,
        ["zip_code"],
        "dsci511proj_dim_zip_prevent_med")
    print("\njoining hospital dimension to unplanned visits")
    hospital_insights_df = unplanned_df.merge(
        dim_hospital_df,
        on="facility_id",
        how="left",
        suffixes=("", "_hospital"))
    print_table_summary(hospital_insights_df, "after hospital join")
    print("\njoining hcahps hospital aggregate")
    hospital_insights_df = hospital_insights_df.merge(
        agg_hcahps_df,
        on="facility_id",
        how="left",
        suffixes=("", "_hcahps"))
    print_table_summary(hospital_insights_df, "After HCAHPS Join")
    ensure_required_columns(
        hospital_insights_df,
        ["zip_code"],
        "Insights After Hopsital Join")
    print("\nJoining Preventive Medicine Zip Code Dimension")
    hospital_insights_df = hospital_insights_df.merge(
        dim_zip_prevent_med_df,
        on="zip_code",
        how="left",
        suffixes=("", "_prevent_med"))
    print_table_summary(hospital_insights_df, "After Preventive Med Data Join")
    print("\nAdding Dataset Metadata...")
    hospital_insights_df.insert(0, "dataset_name", dataset_name_value)
    hospital_insights_df.insert(1, "dataset_version", dataset_version_value)
    hospital_insights_df.insert(2, "pipeline_run_date", pipeline_run_date_value)
    hospital_insights_df.insert(3, "source_unplanned_dataset_id", source_unplanned_dataset_id_value)
    hospital_insights_df.insert(4, "source_hospital_info_dataset_id", source_hospital_info_dataset_id_value)
    hospital_insights_df.insert(5, "source_hcahps_dataset_id", source_hcahps_dataset_id_value)
    hospital_insights_df.insert(6, "source_preventive_dataset_id", source_preventive_dataset_id_value)
    preferred_column_order = [
        "dataset_name",
        "dataset_version",
        "pipeline_run_date",
        "source_unplanned_dataset_id",
        "source_hospital_info_dataset_id",
        "source_hcahps_dataset_id",
        "source_preventive_dataset_id",
        "facility_id",
        "facility_name",
        "telephone_number",
        "address",
        "city_town",
        "county_parish",
        "state",
        "zip_code",
        "hospital_type",
        "hospital_ownership",
        "emergency_services",
        "hospital_overall_rating",
        "meets_criteria_for_birthing_friendly_designation",
        "measure_id",
        "measure_name",
        "compared_to_national",
        "score",
        "denominator",
        "lower_estimate",
        "higher_estimate",
        "number_of_patients",
        "number_of_patients_returned",
        "start_date",
        "end_date",
        "footnote",
        "hcahps_avg_answer_percent",
        "hcahps_avg_linear_mean_value",
        "hcahps_avg_star_rating",
        "hcahps_avg_response_rate",
        "hcahps_total_completed_surveys",
        "hcahps_measure_count",
        "prevent_med_min_medicare_pricing_new_patient",
        "prevent_med_max_medicare_pricing_new_patient",
        "prevent_med_mode_medicare_pricing_new_patient",
        "prevent_med_min_copay_new_patient",
        "prevent_med_max_copay_new_patient",
        "prevent_med_mode_copay_new_patient",
        "prevent_med_most_utilized_proc_new_patient",
        "prevent_med_min_medicare_pricing_established_patient",
        "prevent_med_max_medicare_pricing_established_patient",
        "prevent_med_mode_medicare_pricing_established_patient",
        "prevent_med_min_copay_established_patient",
        "prevent_med_max_copay_established_patient",
        "prevent_med_mode_copay_established_patient",
        "prevent_med_most_utilized_proc_established_patient",]
    hospital_insights_df = reorder_columns(hospital_insights_df, preferred_column_order)

    print("\nSaving Final Dataset")
    hospital_insights_df.to_csv(final_output_file, index=False)
    print_table_summary(hospital_insights_df, "dsci511proj_hospital_insights")
    print(f"Saved to: {final_output_file}")
    print("\nFinal Master Dataset Built")
if __name__ == "__main__":
    main()
