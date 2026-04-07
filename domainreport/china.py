import pandas as pd
import re
import os

# Get script directory
base_dir = os.path.dirname(os.path.abspath(__file__))

# File paths
report_path = os.path.join(base_dir, "Report.csv")
source_path = os.path.join(base_dir, "domain_availability.csv")

# Load files
df_report = pd.read_csv(report_path, encoding="utf-8-sig", nrows=39)  # first 39 domains
df_source = pd.read_csv(source_path, encoding="utf-8-sig")

# Clean column names
df_report.columns = df_report.columns.str.strip()
df_source.columns = df_source.columns.str.strip()

# Normalize column names in report (remove spaces)
df_report.columns = df_report.columns.str.replace(" ", "")

# Ensure target columns are numeric
for col in ["C.UNICOM(%)", "C.TELECOM(%)", "C.MOBILE(%)"]:
    if col in df_report.columns:
        df_report[col] = pd.to_numeric(df_report[col], errors="coerce")

# Clean domain (remove port if exists)
def clean_domain(domain):
    return re.split(r":", str(domain))[0].strip()

# Function to extract CU / CT / CM availability from source
def get_values(domain):
    # Convert both domain and Task name to lowercase and strip spaces
    domain = domain.lower().strip()
    tasknames = df_source["Task name"].str.lower().str.strip()
    
    # Match domain anywhere in Task name (ignores _CU/_CT/_CM suffix)
    matches = df_source[tasknames.str.contains(domain, na=False)]
    
    # Debug: show what Task names matched
    print(f"\nDomain: {domain}")
    print("Matching Task names in source:")
    if matches.empty:
        print("  None found!")
    else:
        for t in matches["Task name"]:
            print(f"  {t}")

    # Extract Availability for each suffix
    cu = matches[matches["Task name"].str.contains("_cu", na=False, case=False)]["Availability(%)"].max()
    ct = matches[matches["Task name"].str.contains("_ct", na=False, case=False)]["Availability(%)"].max()
    cm = matches[matches["Task name"].str.contains("_cm", na=False, case=False)]["Availability(%)"].max()
    
    # Debug: show extracted values
    print(f"  C.UNICOM(%): {cu}")
    print(f"  C.TELECOM(%): {ct}")
    print(f"  C.MOBILE(%): {cm}")
    
    return cu, ct, cm

# Fill C.* columns in Report
for i, row in df_report.iterrows():
    domain = clean_domain(row["DOMAIN"])
    if pd.notna(domain) and domain != "":
        cu, ct, cm = get_values(domain)
        df_report.at[i, "C.UNICOM(%)"] = cu
        df_report.at[i, "C.TELECOM(%)"] = ct
        df_report.at[i, "C.MOBILE(%)"] = cm

# Save updated report
output_path = os.path.join(base_dir, "Report_filled.csv")
df_report.to_csv(output_path, index=False, encoding="utf-8-sig")

print("\nDone Report_filled.csv created with values from domain_availability.csv")
