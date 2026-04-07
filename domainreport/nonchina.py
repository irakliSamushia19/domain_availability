import pandas as pd
import re
import os

# Paths
base_dir = os.path.dirname(os.path.abspath(__file__))
report_path = os.path.join(base_dir, "Report.csv")
source_path = os.path.join(base_dir, "domain_availability.csv")

# Load second table from Report.csv (skip first 40 rows)
df_report = pd.read_csv(report_path, encoding="utf-8-sig", skiprows=40)

# Clean columns (strip spaces)
df_report.columns = df_report.columns.str.strip()

# Load source availability data
df_source = pd.read_csv(source_path, encoding="utf-8-sig")
df_source.columns = df_source.columns.str.strip()

# Clean domain helper: remove port and lowercase
def clean_domain(domain):
    return re.split(r":", str(domain))[0].strip().lower()

# Countries to filter task names by
COUNTRY_CODES = ['vn', 'id', 'th', 'my']

# Function to get availability from source, filtering by domain and country code
def get_availability(domain):
    domain = domain.lower().strip()
    tasknames = df_source["Task name"].str.lower().str.strip()
    
    # Filter tasknames containing the domain
    domain_matches = df_source[tasknames.str.contains(domain, na=False)]
    
    if domain_matches.empty:
        print(f"Domain '{domain}' not found in source.")
        return None
    
    # Further filter tasknames containing any of the country codes
    # Build a regex that matches any of the country codes as substrings
    country_pattern = "|".join(COUNTRY_CODES)
    filtered = domain_matches[domain_matches["Task name"].str.lower().str.contains(country_pattern, na=False)]
    
    if filtered.empty:
        print(f"Domain '{domain}' found but no matching country codes {COUNTRY_CODES} in task names.")
        return None
    
    availability = filtered["Availability(%)"].max()
    print(f"Domain '{domain}' with country codes found. Availability: {availability}")
    return availability

# Fill AVAILABILITY(%) in the report's second table
for i, row in df_report.iterrows():
    domain = clean_domain(row["DOMAIN"])
    if pd.notna(domain) and domain != "":
        availability = get_availability(domain)
        df_report.at[i, "AVAILABILITY(%)"] = availability

# Save updated second table to a new CSV
output_path = os.path.join(base_dir, "Report_filled_second_table.csv")
df_report.to_csv(output_path, index=False, encoding="utf-8-sig")

print("\nDone Report_filled_second_table.csv created with updated AVAILABILITY(%) values")
