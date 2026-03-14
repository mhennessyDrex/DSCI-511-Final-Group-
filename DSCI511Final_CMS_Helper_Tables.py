import os
import pandas as pd

script_dir = os.path.dirname(os.path.abspath(__file__))
stage_dir = script_dir
derived_dir = script_dir

os.makedirs(derived_dir, exist_ok=True)

unplanned_file = os.path.join(stage_dir, "dsci511proj_stage_unplanned_visits.csv")
hospital_info_file = os.path.join(stage_dir, "dsci511proj_stage_hospital_info.csv")
hcahps_file = os.path.join(stage_dir, "dsci511proj_stage_hcahps.csv")
prevent_med_file = os.path.join(stage_dir, "dsci511proj_stage_prevent_med.csv")
dim_hospital_file = os.path.join(derived_dir, "dsci511proj_dim_hospital.csv")
agg_hcahps_file = os.path.join(derived_dir, "dsci511proj_agg_hcahps_hospital.csv")
dim_zip_prevent_med_file = os.path.join(derived_dir, "dsci511proj_dim_zip_prevent_med.csv")
def load_csv(file_path):
    return pd.read_csv(file_path, low_memory=False)
def print_table_summary(df, table_name):
    print(f"\nTable: {table_name}")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
def standardize_yes_no(value):
    if pd.isna(value):
        return pd.NA
    value_str = str(value).strip().lower()
    if value_str in {"yes", "y", "true", "1"}:
        return "yes"
    if value_str in {"no", "n", "false", "0"}:
        return "no"
    return str(value).strip()
def coerce_numeric(series):
    return pd.to_numeric(series, errors="coerce")
def first_non_null(series):
    non_null = series.dropna()
    if len(non_null) == 0:
        return pd.NA
    return non_null.iloc[0]
def build_dim_hospital(hospital_info_df):
    df = hospital_info_df.copy()
    expected_columns = [
        "facility_id","facility_name","address","city_town","county_parish","state","zip_code","telephone_number","hospital_type","hospital_ownership","emergency_services","hospital_overall_rating","meets_criteria_for_birthing_friendly_designation",]
    available_columns = [col for col in expected_columns if col in df.columns]
    df = df[available_columns].copy()
    if "emergency_services" in df.columns:
        df["emergency_services"] = df["emergency_services"].apply(standardize_yes_no)
    if "meets_criteria_for_birthing_friendly_designation" in df.columns:
        df["meets_criteria_for_birthing_friendly_designation"] = (
            df["meets_criteria_for_birthing_friendly_designation"].apply(standardize_yes_no))
    if "hospital_overall_rating" in df.columns:
        df["hospital_overall_rating"] = coerce_numeric(df["hospital_overall_rating"])
    df = df.drop_duplicates(subset=["facility_id"]).reset_index(drop=True)
    return df
def build_agg_hcahps_hospital(hcahps_df):
    df = hcahps_df.copy()
    numeric_candidates = [
        "hcahps_answer_percent","hcahps_linear_mean_value","patient_survey_star_rating","survey_response_rate_percent","number_of_completed_surveys",]
    for col in numeric_candidates:
        if col in df.columns:
            df[col] = coerce_numeric(df[col])
    group_cols = ["facility_id"]
    agg_dict = {}
    if "facility_name" in df.columns:
        agg_dict["facility_name"] = first_non_null
    if "state" in df.columns:
        agg_dict["state"] = first_non_null
    if "zip_code" in df.columns:
        agg_dict["zip_code"] = first_non_null
    if "hcahps_answer_percent" in df.columns:
        agg_dict["hcahps_answer_percent"] = "mean"
    if "hcahps_linear_mean_value" in df.columns:
        agg_dict["hcahps_linear_mean_value"] = "mean"
    if "patient_survey_star_rating" in df.columns:
        agg_dict["patient_survey_star_rating"] = "mean"
    if "survey_response_rate_percent" in df.columns:
        agg_dict["survey_response_rate_percent"] = "mean"
    if "number_of_completed_surveys" in df.columns:
        agg_dict["number_of_completed_surveys"] = "sum"
    if "hcahps_measure_id" in df.columns:
        agg_dict["hcahps_measure_id"] = pd.Series.nunique
    agg_df = df.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()
    rename_map = {
        "hcahps_answer_percent": "hcahps_avg_answer_percent","hcahps_linear_mean_value": "hcahps_avg_linear_mean_value","patient_survey_star_rating": "hcahps_avg_star_rating","survey_response_rate_percent": "hcahps_avg_response_rate","number_of_completed_surveys": "hcahps_total_completed_surveys","hcahps_measure_id": "hcahps_measure_count",}
    agg_df = agg_df.rename(columns=rename_map)
    return agg_df
def build_dim_zip_prevent_med(prevent_med_df):
    df = prevent_med_df.copy()
    for col in df.columns:
        if any(token in col for token in ["pricing", "copay", "payment", "charge", "allowed"]):
            df[col] = coerce_numeric(df[col])
    if "zip_code" in df.columns:
        df = df.sort_values(by=["zip_code"]).drop_duplicates(subset=["zip_code"]).reset_index(drop=True)
    rename_map = {
        "min_medicare_pricing_for_new_patient": "prevent_med_min_medicare_pricing_new_patient",
        "max_medicare_pricing_for_new_patient": "prevent_med_max_medicare_pricing_new_patient",
        "mode_medicare_pricing_for_new_patient": "prevent_med_mode_medicare_pricing_new_patient",
        "min_copay_for_new_patient": "prevent_med_min_copay_new_patient",
        "max_copay_for_new_patient": "prevent_med_max_copay_new_patient",
        "mode_copay_for_new_patient": "prevent_med_mode_copay_new_patient",
        "most_utilized_procedure_code_for_new_patient": "prevent_med_most_utilized_proc_new_patient",
        "min_medicare_pricing_for_established_patient": "prevent_med_min_medicare_pricing_established_patient",
        "max_medicare_pricing_for_established_patient": "prevent_med_max_medicare_pricing_established_patient",
        "mode_medicare_pricing_for_established_patient": "prevent_med_mode_medicare_pricing_established_patient",
        "min_copay_for_established_patient": "prevent_med_min_copay_established_patient",
        "max_copay_for_established_patient": "prevent_med_max_copay_established_patient",
        "mode_copay_for_established_patient": "prevent_med_mode_copay_established_patient",
        "most_utilized_procedure_code_for_established_patient": "prevent_med_most_utilized_proc_established_patient",}
    existing_rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=existing_rename_map)
    return df
def main():
    print("Staged Datasets Loading...")
    hospital_info_df = load_csv(hospital_info_file)
    hcahps_df = load_csv(hcahps_file)
    prevent_med_df = load_csv(prevent_med_file)
    print("\nBuilding Hospital Dimension")
    dim_hospital_df = build_dim_hospital(hospital_info_df)
    print_table_summary(dim_hospital_df, "dsci511proj_dim_hospital")
    dim_hospital_df.to_csv(dim_hospital_file, index=False)
    print(f"Saved: {dim_hospital_file}")
    print("\nBuilding HCAHPS Hospital Agg Data Table")
    agg_hcahps_df = build_agg_hcahps_hospital(hcahps_df)
    print_table_summary(agg_hcahps_df, "dsci511proj_agg_hcahps_hospital")
    agg_hcahps_df.to_csv(agg_hcahps_file, index=False)
    print(f"Saved: {agg_hcahps_file}")
    print("\nBuilding Prevent Med Dimensional Zip")
    dim_zip_prevent_med_df = build_dim_zip_prevent_med(prevent_med_df)
    print_table_summary(dim_zip_prevent_med_df, "dsci511proj_dim_zip_prevent_med")
    dim_zip_prevent_med_df.to_csv(dim_zip_prevent_med_file, index=False)
    print(f"Saved: {dim_zip_prevent_med_file}")

    print("\nHelper Tables Built")
if __name__ == "__main__":
    main()
