import pandas as pd
import re
from datetime import datetime
from rapidfuzz import fuzz
import unidecode

# ------------------ Load and Normalize ------------------ #
df = pd.read_csv("customer_data.csv")

# Normalize PAN
df['pan'] = df['pan'].astype(str).str.strip().str.upper()

# Normalize DOB with multiple format support
def normalize_dob(dob_str):
    if pd.isna(dob_str):
        return None
    formats = ["%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d"]
    for fmt in formats:
        try:
            return datetime.strptime(dob_str.strip(), fmt).date()
        except:
            continue
    return None

df['dob'] = df['dob'].astype(str).apply(normalize_dob)

# ------------------ PAN Validation ------------------ #
def is_valid_pan(pan):
    """
    Validates a PAN based on structure and excludes test/dummy PANs like 'AAAAA...'
    """
    if not isinstance(pan, str):
        return False
    pan = pan.strip().upper()
    # PAN must follow structural format and cannot start with 'AAAAA'
    if re.fullmatch(r'^[A-Z]{3}[PCHABGJLFT][A-Z][0-9]{4}[A-Z]$', pan):
        return not pan.startswith('AAAAA')
    return False


# ------------------ Normalize Name and Org ------------------ #
def normalize_name(name):
    if pd.isna(name):
        return ''
    name = unidecode.unidecode(name.lower())
    name = re.sub(r'[^a-z\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

df['first_name_norm'] = df['first_name'].apply(normalize_name)
df['last_name_norm'] = df['last_name'].apply(normalize_name)
df['organization_name_norm'] = df['organization_name'].apply(normalize_name)

# Combine for full name
df['full_name'] = (df['first_name_norm'] + ' ' + df['last_name_norm']).str.strip()

# ------------------ REPORT 1: Valid PAN with Multiple UCICs ------------------ #
valid_pan_df = df[df['pan_valid']]

# Find PANs assigned to multiple UCICs
multi_ucic_by_pan = (
    valid_pan_df.groupby('pan')
    .filter(lambda x: x['ucic'].nunique() > 1)
    .sort_values(['pan', 'ucic'])
)

multi_ucic_by_pan.to_csv("01_valid_pan_multiple_ucic.csv", index=False)

# ------------------ REPORT 2: Similar Name + DOB, Different UCIC ------------------ #
invalid_pan_df = df[~df['pan_valid']]

name_dob_candidates = invalid_pan_df[
    invalid_pan_df['full_name'].notnull() & invalid_pan_df['dob'].notnull()
].copy()

# Create a similarity cluster
def cluster_similar_names(df, threshold=85):
    clusters = []
    used = set()

    for i, row_i in df.iterrows():
        if i in used:
            continue
        group = [i]
        name_i, dob_i = row_i['full_name'], row_i['dob']

        for j, row_j in df.loc[df.index > i].iterrows():
            if j in used:
                continue
            name_j, dob_j = row_j['full_name'], row_j['dob']

            if dob_i == dob_j and fuzz.token_sort_ratio(name_i, name_j) >= threshold:
                group.append(j)
                used.add(j)
        if len(group) > 1:
            clusters.append(df.loc[group])
            used.update(group)
    return clusters

name_dob_clusters = cluster_similar_names(name_dob_candidates)

# Save to CSV
for i, group in enumerate(name_dob_clusters, 1):
    group.to_csv(f"02_similar_name_dob_diff_ucic_cluster_{i}.csv", index=False)

# ------------------ REPORT 3: Similar Org Name + DOB, Different UCIC ------------------ #
org_dob_candidates = invalid_pan_df[
    invalid_pan_df['organization_name_norm'].notnull() & invalid_pan_df['dob'].notnull()
].copy()

def cluster_similar_orgs(df, threshold=85):
    clusters = []
    used = set()

    for i, row_i in df.iterrows():
        if i in used:
            continue
        group = [i]
        name_i, dob_i = row_i['organization_name_norm'], row_i['dob']

        for j, row_j in df.loc[df.index > i].iterrows():
            if j in used:
                continue
            name_j, dob_j = row_j['organization_name_norm'], row_j['dob']

            if dob_i == dob_j and fuzz.token_sort_ratio(name_i, name_j) >= threshold:
                group.append(j)
                used.add(j)
        if len(group) > 1:
            clusters.append(df.loc[group])
            used.update(group)
    return clusters

org_dob_clusters = cluster_similar_orgs(org_dob_candidates)

for i, group in enumerate(org_dob_clusters, 1):
    group.to_csv(f"03_similar_org_dob_diff_ucic_cluster_{i}.csv", index=False)

# ------------------ Summary ------------------ #
print("✅ Reports generated:")
print("1. 01_valid_pan_multiple_ucic.csv")
print("2. 02_similar_name_dob_diff_ucic_cluster_X.csv")
print("3. 03_similar_org_dob_diff_ucic_cluster_X.csv")
