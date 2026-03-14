import os
import re
import pandas as pd

script_dir = os.path.dirname(os.path.abspath(__file__))
raw_dir = script_dir
stage_dir = script_dir

input_files = {
    "dsci511proj_raw_unplanned_visits.csv": "dsci511proj_stage_unplanned_visits.csv",
    "dsci511proj_raw_hospital_info.csv": "dsci511proj_stage_hospital_info.csv",
    "dsci511proj_raw_hcahps.csv": "dsci511proj_stage_hcahps.csv",
    "dsci511proj_raw_prevent_med.csv": "dsci511proj_stage_prevent_med.csv",
}
def clean_column_name(column_name):
    column_name = str(column_name).strip().lower()
    column_name = re.sub(r"[^a-z0-9]+", "_", column_name)
    column_name = re.sub(r"_+", "_", column_name).strip("_")
    return column_name
def clean_columns(df):
    df = df.copy()
    df.columns = [clean_column_name(col) for col in df.columns]
    return df
def trim_text_columns(df):
    df = df.copy()
    text_columns = df.select_dtypes(include=["object"]).columns.tolist()
    for col in text_columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df
def standardize_zip5(value):
    if pd.isna(value):
        return pd.NA
    value_str = str(value).strip()
    if value_str == "":
        return pd.NA
    match = re.search(r"(\d{5})", value_str)
    if match:
        return match.group(1)
    return pd.NA
def standardize_zip_columns(df):
    df = df.copy()
    zip_columns = [col for col in df.columns if "zip" in col]
    for col in zip_columns:
        df[col] = df[col].apply(standardize_zip5)
    return df
def convert_date_columns(df):
    df = df.copy()
    date_columns = [col for col in df.columns if "date" in col or col.endswith("_period")]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df
def clean_numeric_series(series):
    cleaned = (
        series.astype(str)
        .str.strip()
        .str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace("%", "", regex=False))
    cleaned = cleaned.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "Not Available": pd.NA})
    return pd.to_numeric(cleaned, errors="coerce")
def convert_numeric_columns(df):
    df = df.copy()
    protected_columns = {
        "facility_id","measure_id","hcahps_measure_id","zip_code","telephone_number","state","county_parish","city_town","address","facility_name","measure_name","hcahps_question","hcahps_answer_description","hospital_type","hospital_ownership","footnote","compared_to_national","most_utilized_procedure_code_for_new_patient","most_utilized_procedure_code_for_established_patient","hcpcs_code","hcpcs_description",}
    likely_numeric_keywords = [
        "score","denominator","estimate","number_of","percent","rate","mean","rating","payment","charge","allowed","copay","services","survey","response",]
    for col in df.columns:
        if col in protected_columns:
            continue
        if any(keyword in col for keyword in likely_numeric_keywords):
            df[col] = clean_numeric_series(df[col])
    return df
def clean_dataframe(df):
    df = clean_columns(df)
    df = trim_text_columns(df)
    df = standardize_zip_columns(df)
    df = convert_date_columns(df)
    df = convert_numeric_columns(df)
    return df
def print_basic_summary(df, table_name):
    print(f"\nTable: {table_name}")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    zip_columns = [col for col in df.columns if "zip" in col]
    if zip_columns:
        print(f"Zip Columns Standardized: {', '.join(zip_columns)}")
    date_columns = [col for col in df.columns if "date" in col or col.endswith('_period')]
    if date_columns:
        print(f"Date Columns Converted: {', '.join(date_columns)}")
def process_file(input_file_name, output_file_name):
    input_path = os.path.join(raw_dir, input_file_name)
    output_path = os.path.join(stage_dir, output_file_name)
    print(f"\nLoading File: {input_path}")
    df = pd.read_csv(input_path, low_memory=False)
    df = clean_dataframe(df)
    print_basic_summary(df, output_file_name)
    df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")
def main():
    for input_file_name, output_file_name in input_files.items():
        process_file(input_file_name, output_file_name)
    print("\nClean Data Complete")
if __name__ == "__main__":
    main()