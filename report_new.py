import pandas as pd

# Sample DataFrames (replace these with your actual data)
# Due_df = pd.read_csv('path_to_due_df.csv')
# Allocation_Collection = pd.read_csv('path_to_allocation_collection.csv')

# Filter Due_df for charge code 9
filtered_due_df = Due_df[Due_df['Charge Code'] == 9]

# Merge with Allocation_Collection to get the required columns
merged_df = pd.merge(filtered_due_df, Allocation_Collection, left_on='Advice Ref#', right_on='Receipt Ref #', how='left')

# Prepare the first report
first_report = merged_df[['LOANACCTNO', 'Due Amount', 'Advice Ref#', 'Advice Date', 'Allocation Date', 'Receipt Ref #', 'Collected Amount']]

# Display the first report
print("First Report:")
print(first_report)

# Prepare the second report
# Group by LOANACCTNO and Advice Month to calculate totals
second_report = (filtered_due_df.groupby(['LOANACCTNO', 'Advice Month'])
                 .agg(Total_Due_Amount=('Due Amount', 'sum'),
                     Collected_Amount=('Collected Amount', 'sum'))
                 .reset_index())

# Calculate Due Collection for Month
second_report['Due_Collection'] = second_report['Total_Due_Amount'] == second_report['Collected_Amount']

# Calculate Overdue
second_report['Overdue'] = second_report.apply(lambda x: max(0, x['Collected_Amount'] - x['Total_Due_Amount']), axis=1)

# Display the second report
print("\nSecond Report:")
print(second_report)
