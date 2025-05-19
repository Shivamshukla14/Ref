import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# Step 1: Load your data (replace this with your actual source)
# Example: df = pd.read_csv("your_file.csv")

# Step 2: Parse Advice Date (format: DD-MM-YY) and Allocation Date (format: YYYY-MM-DD)
df['Advice Date'] = pd.to_datetime(df['Advice Date'], format='%d-%m-%y', errors='coerce')
df['Allocation Date'] = pd.to_datetime(df['Allocation Date'], format='%Y-%m-%d', errors='coerce')

# Step 3: Create Month-Year columns
df['Advice_MonthYear'] = df['Advice Date'].dt.strftime('%b-%Y')  # e.g., 'Mar-2024'
df['Allocation_MonthYear'] = df['Allocation Date'].dt.strftime('%b-%Y')

# Step 4: Filter and deduplicate dues by Advice Ref# (to avoid overcounting partial payments)
due_df = (
    df[df['Charge_Code_due'] == 9]
    .drop_duplicates(subset=['LOANACCTNO', 'Advice Ref#'])  # One due per advice
    .groupby(['LOANACCTNO', 'Advice_MonthYear'], as_index=False)
    .agg({'DueAmount': 'sum'})
    .rename(columns={'Advice_MonthYear': 'MonthYear'})
)

# Step 5: Aggregate collected amounts by Allocation Date
collected_df = (
    df.groupby(['LOANACCTNO', 'Allocation_MonthYear'], as_index=False)
    .agg({'Collected Amount': 'sum'})
    .rename(columns={'Allocation_MonthYear': 'MonthYear'})
)

# Step 6: Round values to 2 decimal places using Decimal to avoid floating-point errors
def to_fixed_str(val):
    return str(Decimal(val).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))

due_df['DueAmount'] = due_df['DueAmount'].apply(to_fixed_str)
collected_df['Collected Amount'] = collected_df['Collected Amount'].apply(to_fixed_str)

# Step 7: Prepare full keys to ensure all LOANACCTNO-MonthYear combinations are captured
all_keys = pd.concat([
    due_df[['LOANACCTNO', 'MonthYear']],
    collected_df[['LOANACCTNO', 'MonthYear']]
]).drop_duplicates()

# Step 8: Merge dues and collections
final_df = all_keys.merge(due_df, on=['LOANACCTNO', 'MonthYear'], how='left')
final_df = final_df.merge(collected_df, on=['LOANACCTNO', 'MonthYear'], how='left')

# Step 9: Fill NaN with "0.00" (as string for consistency)
final_df['DueAmount'] = final_df['DueAmount'].fillna('0.00')
final_df['Collected Amount'] = final_df['Collected Amount'].fillna('0.00')

# Step 10: Sort by MonthYear and LOANACCTNO
final_df['MonthYear_sort'] = pd.to_datetime(final_df['MonthYear'], format='%b-%Y', errors='coerce')
final_df = final_df.sort_values(by=['MonthYear_sort', 'LOANACCTNO']).drop(columns='MonthYear_sort')

# Step 11: Save to CSV with exact decimals
final_df.to_csv('final_report.csv', index=False)

# Optional: View result
print(final_df.head(10))
