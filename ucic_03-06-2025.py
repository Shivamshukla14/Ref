import pandas as pd
import numpy as np
import re
from datetime import datetime
from rapidfuzz.fuzz import token_sort_ratio

# ---------- Step 1: Load & Clean Data ---------- #

def load_and_clean_data():
    df_master = pd.read_csv("UCIC_Dump.csv", dtype=str)
    df_new = pd.read_excel("ucic_02-62025.xlsx", dtype=str)

    # Standardize column names
    df_master.columns = df_master.columns.str.lower().str.strip()
    df_new.columns = df_new.columns.str.lower().str.strip()

    # Normalize and clean data
    for df in [df_master, df_new]:
        df.fillna("", inplace=True)
        df['dob'] = pd.to_datetime(df.get('dob', df.get('birth_date')), errors='coerce', dayfirst=True).dt.date
        df['first_name'] = df['first_name'].str.strip().str.upper()
        df['last_name'] = df['last_name'].str.strip().str.upper()
        df['organization_name'] = df['organization_name'].str.strip().str.upper()
        df['pan'] = df['pan'].str.strip().str.upper()
        df['aadhar_no'] = df['aadhar_no'].astype(str).str.extract(r'(\d{4})$', expand=False).fillna("")

    return df_master, df_new


# ---------- Step 2: PAN Validation ---------- #

def is_valid_pan(pan):
    return bool(re.match(r'^[A-Z]{5}[0-9]{4}[A-Z]$', pan))


# ---------- Step 3: Similarity Matching Logic ---------- #

def find_ucic_match(new_row, df_master):
    pan = new_row['pan']
    dob = new_row['dob']
    first_name = new_row['first_name']
    last_name = new_row['last_name']
    org_name = new_row['organization_name']
    aadhar_suffix = new_row['aadhar_no']
    party_tc = new_row.get('party_tc', '').strip().upper()

    # 1. PAN match if valid
    if is_valid_pan(pan):
        matched = df_master[df_master['pan'] == pan]
        if not matched.empty:
            return matched.iloc[0]['ucic']

    # Determine individual or organization
    is_individual = (party_tc == "PERSON")

    # 2. Match individuals
    if is_individual:
        # Aadhar + DOB match
        if aadhar_suffix:
            matched = df_master[
                (df_master['aadhar_no'] == aadhar_suffix) &
                (df_master['dob'] == dob) &
                (df_master['party_tc'].str.upper() == 'PERSON')
            ]
            if not matched.empty:
                return matched.iloc[0]['ucic']

        # Fuzzy name + DOB match
        potential = df_master[
            (df_master['dob'] == dob) &
            (df_master['party_tc'].str.upper() == 'PERSON')
        ]
        for _, row in potential.iterrows():
            score = token_sort_ratio(f"{first_name} {last_name}", f"{row['first_name']} {row['last_name']}")
            if score >= 90:
                return row['ucic']

    # 3. Match organizations by fuzzy org_name + DOB
    else:
        potential = df_master[
            (df_master['dob'] == dob) &
            (df_master['party_tc'].str.upper() != 'PERSON')
        ]
        for _, row in potential.iterrows():
            score = token_sort_ratio(org_name, row['organization_name'])
            if score >= 90:
                return row['ucic']

    return None


# ---------- Step 4: Match All Customers ---------- #

def match_customers(df_master, df_new):
    matched = []
    unmatched = []

    for idx, row in df_new.iterrows():
        ucic = find_ucic_match(row, df_master)
        result = row.to_dict()
        if ucic:
            result['matched_ucic'] = ucic
            matched.append(result)
        else:
            unmatched.append(result)

        if (idx + 1) % 5000 == 0:
            print(f"Processed {idx + 1} records...")

    return pd.DataFrame(matched), pd.DataFrame(unmatched)


# ---------- Step 5: Output to Excel ---------- #

def generate_reports(matched_df, unmatched_df):
    matched_df.to_excel("matched_ucics.xlsx", index=False)
    unmatched_df.to_excel("unmatched_ucics.xlsx", index=False)
    print(f"\n✅ Matching complete.")
    print(f"🔍 Matched records: {len(matched_df)}")
    print(f"❌ Unmatched records: {len(unmatched_df)}")


# ---------- Step 6: Main Pipeline ---------- #

if __name__ == "__main__":
    print("🚀 Loading and processing data...")
    df_master, df_new = load_and_clean_data()
    print("🔗 Matching UCICs...")
    matched_df, unmatched_df = match_customers(df_master, df_new)
    print("💾 Saving results...")
    generate_reports(matched_df, unmatched_df)
