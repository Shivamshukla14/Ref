import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# -- Load your dataframe
# df = pd.read_csv("your_file.csv")

# Parse both dates correctly
df['Advice Date'] = pd.to_datetime(df['Advice Date'], format='%d-%m-%y', errors='coerce')
df['Allocation Date'] = pd.to_datetime(df['Allocation Date'], format='%Y-%m-%d', errors='coerce')

# Create month-year columns
df['Advice_MonthYear'] = df['Advice Date'].dt.strftime('%b-%Y')
df['Allocation_MonthYear'] = df['Allocation Date'].dt.strftime('%b-%Y')

# Filter and deduplicate due rows by Advice Ref#
due_df = (
    df[df['Charge_Code_due'] == 9]
    .drop_duplicates(subset=['LOANACCTNO', 'Advice Ref#'])
    .groupby(['LOANACCTNO', 'Advice_MonthYear'], as_index=False)
    .agg({'DueAmount': 'sum'})
    .rename(columns={'Advice_MonthYear': 'MonthYear'})
)

# Collected amounts grouped by allocation month
collected_df = (
    df.groupby(['LOANACCTNO', 'Allocation_MonthYear'], as_index=False)
    .agg({'Collected Amount': 'sum'})
    .rename(columns={'Allocation_MonthYear': 'MonthYear'})
)

# Round with Decimal and convert to fixed string format
def to_fixed_str(val):
    return format(Decimal(val).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP), '.2f')

due_df['DueAmount'] = due_df['DueAmount'].apply(to_fixed_str)
collected_df['Collected Amount'] = collected_df['Collected Amount'].apply(to_fixed_str)

# Ensure full key space is retained
all_keys = pd.concat([
    due_df[['LOANACCTNO', 'MonthYear']],
    collected_df[['LOANACCTNO', 'MonthYear']]
]).drop_duplicates()

# Merge the two
final_df = all_keys.merge(due_df, on=['LOANACCTNO', 'MonthYear'], how='left')
final_df = final_df.merge(collected_df, on=['LOANACCTNO', 'MonthYear'], how='left')

# Fill missing values with "0.00" (string)
final_df['DueAmount'] = final_df['DueAmount'].fillna('0.00')
final_df['Collected Amount'] = final_df['Collected Amount'].fillna('0.00')

# Sort properly
final_df['MonthYear_sort'] = pd.to_datetime(final_df['MonthYear'], format='%b-%Y', errors='coerce')
final_df = final_df.sort_values(by=['MonthYear_sort', 'LOANACCTNO']).drop(columns='MonthYear_sort')

# Final step: force all numeric columns to string before CSV export
final_df['DueAmount'] = final_df['DueAmount'].astype(str)
final_df['Collected Amount'] = final_df['Collected Amount'].astype(str)

# Save to CSV WITHOUT letting float formatting creep in
final_df.to_csv('final_report.csv', index=False, quoting=1)  # quoting=1 forces quotes around text/numbers
