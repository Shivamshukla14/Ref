import pandas as pd

# Load data (update file paths if needed)
allocation_df = pd.read_csv('allocation_data.csv')
due_df = pd.read_csv('due_data.csv')

# Rename columns for consistency to enable merge
allocation_df.rename(columns={
    'Loan Account #': 'LOANACCTNO',
    'Advice Ref #': 'Advice Ref#'
}, inplace=True)

# Merge due_df with allocation_df on LOANACCTNO and Advice Ref# to align records side by side
# Use outer join if you want to see all records including unmatched ones
# For strict match in due_df, use left join
merged_df = pd.merge(
    due_df,
    allocation_df,
    how='left',  # change to 'outer' to get unmatched on both sides
    on=['LOANACCTNO', 'Advice Ref#'],
    suffixes=('_due', '_allocation')
)

# Select and reorder columns to present a clear matching scenario to the manager
columns_due = [
    'LOANACCTNO', 'Product Code', 'AGREEMENTID', 'Advice Ref#', 'Advice Date', 'Advice Month',
    'DueAmount', 'Principal Due Component', 'Interest Due Component', 'Installment',
    'Charge Code_due', 'CHARGEDESCCHARGEDESC'
]

columns_allocation = [
    'Receipt Ref #', 'Allocation Date', 'Allocation Month', 'Collected_Amount',
    'PRINCOM-P_COLLECTED', 'INTCOMP_COLLECTED', 'Charge Code_allocation', 'CHARGE_COLDESC'
]

# Check if all these columns exist in merged_df, some assignment to empty string if missing to avoid errors
for col in columns_due:
    if col not in merged_df.columns:
        merged_df[col] = None
for col in columns_allocation:
    if col not in merged_df.columns:
        merged_df[col] = None

final_columns = columns_due + columns_allocation

report_df = merged_df[final_columns]

# Save the consolidated matching report to CSV
report_df.to_csv('consolidated_matching_report.csv', index=False)

print("Consolidated matching report generated: consolidated_matching_report.csv")
print(report_df.head())

