import pandas as pd

# Step 1: Load your data (replace this with your actual data source)
# For example, df = pd.read_excel("your_data.xlsx")
# Here we assume df is already available

# Step 2: Convert date columns to datetime
df['Advice Date'] = pd.to_datetime(df['Advice Date'])
df['Allocation Date'] = pd.to_datetime(df['Allocation Date'])

# Step 3: Create MonthYear columns in "Apr-2021" format
df['Advice_MonthYear'] = df['Advice Date'].dt.strftime('%b-%Y')
df['Allocation_MonthYear'] = df['Allocation Date'].dt.strftime('%b-%Y')

# Step 4: Filter for valid dues (Charge_Code_due == 9) and group by LAN + Advice_MonthYear
due_df = (
    df[df['Charge_Code_due'] == 9]
    .groupby(['LOANACCTNO', 'Advice_MonthYear'], as_index=False)
    .agg({'DueAmount': 'sum'})
    .rename(columns={'Advice_MonthYear': 'MonthYear'})
)

# Step 5: Group collected amounts by LAN + Allocation_MonthYear
collected_df = (
    df.groupby(['LOANACCTNO', 'Allocation_MonthYear'], as_index=False)
    .agg({'Collected Amount': 'sum'})
    .rename(columns={'Allocation_MonthYear': 'MonthYear'})
)

# Step 6: Combine all (LAN, MonthYear) pairs from both due and collected sets
all_keys = pd.concat([
    due_df[['LOANACCTNO', 'MonthYear']],
    collected_df[['LOANACCTNO', 'MonthYear']]
]).drop_duplicates()

# Step 7: Merge due and collected values on keys
final_df = all_keys.merge(due_df, on=['LOANACCTNO', 'MonthYear'], how='left')
final_df = final_df.merge(collected_df, on=['LOANACCTNO', 'MonthYear'], how='left')

# Step 8: Fill missing values with 0
final_df['DueAmount'] = final_df['DueAmount'].fillna(0)
final_df['Collected Amount'] = final_df['Collected Amount'].fillna(0)

# Step 9: Optional - sort the result by MonthYear and LOANACCTNO
final_df['MonthYear_sort'] = pd.to_datetime(final_df['MonthYear'], format='%b-%Y')
final_df = final_df.sort_values(by=['MonthYear_sort', 'LOANACCTNO']).drop(columns='MonthYear_sort')

# Step 10: Final Output
print(final_df)

# Optional: Export to Excel or CSV
# final_df.to_excel("monthly_due_collected_report.xlsx", index=False)
# final_df.to_csv("monthly_due_collected_report.csv", index=False)
