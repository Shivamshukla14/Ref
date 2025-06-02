import pandas as pd
import numpy as np
import re
from datetime import datetime
from rapidfuzz import process, fuzz

def load_and_clean_data():
    df_master = pd.read_csv("UCIC_Dump.csv", dtype=str)
    df_new = pd.read_excel("ucic_02-62025.xlsx", dtype=str)

    df_master.columns = df_master.columns.str.lower().str.strip()
    df_new.columns = df_new.columns.str.lower().str.strip()

    for df in [df_master, df_new]:
        df.fillna("", inplace=True)
        dob_col = 'dob' if 'dob' in df.columns else 'birth_date'
        df['dob'] = pd.to_datetime(df[dob_col], errors='coerce', dayfirst=True).dt.date
        df['first_name'] = df.get('first_name', '').str.strip().str.upper()
        df['last_name'] = df.get('last_name', '').str.strip().str.upper()
        df['organization_name'] = df.get('organization_name', '').str.strip().str.upper()
        df['pan'] = df.get('pan', '').str.strip().str.upper()
        df['aadhar_no'] = df.get('aadhar_no', '').astype(str).str.extract(r'(\d{4})$', expand=False).fillna("")
        df['party_tc'] = df.get('party_tc', '').str.strip().str.upper()
        df['ucic'] = df.get('ucic', '').str.strip()

    return df_master, df_new


def is_valid_pan(pan):
    return bool(re.match(r'^[A-Z]{5}[0-9]{4}[A-Z]$', pan))


def find_ucic_match_fast(row, master_persons, master_orgs):
    pan = row.get('pan', '')
    dob = row.get('dob')
    aadhar = row.get('aadhar_no')
    first_name = row.get('first_name', '')
    last_name = row.get('last_name', '')
    full_name = f"{first_name} {last_name}".strip()
    org_name = row.get('organization_name', '')
    party_tc = row.get('party_tc', '')

    # 1. PAN match
    if pan and is_valid_pan(pan):
        match = master_persons[master_persons['pan'] == pan]
        if not match.empty:
            return match.iloc[0]['ucic']

    # Person Logic
    if party_tc == "PERSON":
        candidates = master_persons[master_persons['dob'] == dob]

        # 2. Aadhar + DOB match
        if aadhar:
            match = candidates[candidates['aadhar_no'] == aadhar]
            if not match.empty:
                return match.iloc[0]['ucic']

        # 3. Fuzzy Name Match
        names_list = candidates.apply(lambda x: f"{x['first_name']} {x['last_name']}", axis=1).tolist()
        if names_list:
            match, score, idx = process.extractOne(full_name, names_list, scorer=fuzz.token_sort_ratio)
            if score >= 90:
                return candidates.iloc[idx]['ucic']

    # Organization Logic
    else:
        candidates = master_orgs[master_orgs['dob'] == dob]
        org_list = candidates['organization_name'].tolist()
        if org_name and org_list:
            match, score, idx = process.extractOne(org_name, org_list, scorer=fuzz.token_sort_ratio)
            if score >= 90:
                return candidates.iloc[idx]['ucic']

    return None


def match_all(df_master, df_new):
    matched, unmatched = [], []

    master_persons = df_master[df_master['party_tc'] == 'PERSON']
    master_orgs = df_master[df_master['party_tc'] != 'PERSON']
    master_persons = master_persons[master_persons['ucic'] != '']
    master_orgs = master_orgs[master_orgs['ucic'] != '']

    for i, row in df_new.iterrows():
        ucic = find_ucic_match_fast(row, master_persons, master_orgs)
        rec = row.to_dict()
        if ucic:
            rec['matched_ucic'] = ucic
            matched.append(rec)
        else:
            unmatched.append(rec)

        if (i+1) % 5000 == 0:
            print(f"Processed {i+1} rows...")

    return pd.DataFrame(matched), pd.DataFrame(unmatched)


def generate_reports(matched_df, unmatched_df):
    matched_df.to_excel("matched_ucics.xlsx", index=False)
    unmatched_df.to_excel("unmatched_ucics.xlsx", index=False)
    print("\n✅ Matching complete.")
    print(f"🔍 Matched: {len(matched_df)}")
    print(f"❌ Unmatched: {len(unmatched_df)}")


if __name__ == "__main__":
    print("🚀 Loading data...")
    df_master, df_new = load_and_clean_data()
    print("🔎 Matching UCICs...")
    matched_df, unmatched_df = match_all(df_master, df_new)
    print("📁 Saving output...")
    generate_reports(matched_df, unmatched_df)
