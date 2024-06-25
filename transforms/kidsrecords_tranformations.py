import pandas as pd

# Load the dataset
file_path = r'C:\Users\jnthn\Documents\Data_Science\DataWhizz\old_files\Consumers.csv'  # Replace with your actual file path
df = pd.read_csv(file_path, low_memory=False)

# Identify relevant columns that match the expected pattern
relevant_columns = [col for col in df.columns if col.startswith('Kids_')]

# Reshape the dataset to long format
df_long = pd.melt(df, id_vars=['Id'], value_vars=relevant_columns, var_name='Category', value_name='Presence')

# Split the Category column into Gender and Age_Range
split_categories = df_long['Category'].str.split('_', expand=True)
df_long['Gender'] = split_categories[1]
df_long['Age_Range'] = split_categories[2] + '_' + split_categories[3]

# Filter out rows where Presence is not 'Y'
df_long = df_long[df_long['Presence'] == 'Y']

# Drop the 'Presence' and 'Category' columns as they are redundant now
df_long = df_long.drop(columns=['Presence', 'Category'])

# Group by Id and create columns for up to 6 children
df_children = df_long.groupby('Id').apply(lambda x: x.reset_index(drop=True)).reset_index(drop=True)

# Create separate columns for each child
df_children['Child_Number'] = df_children.groupby('Id').cumcount() + 1
df_children = df_children.pivot(index='Id', columns='Child_Number', values=['Gender', 'Age_Range'])
df_children.columns = [f'{col[0]}_{col[1]}' for col in df_children.columns]
df_children = df_children.reset_index()

# Merge with the original dataset to retain other columns
df_final = pd.merge(df.drop(columns=relevant_columns), df_children, on='Id', how='left')

# Save the transformed dataset
output_path = r'C:\Users\jnthn\Documents\Data_Science\DataWhizz\kidsrecords_transformed.csv'  # Replace with your desired output path
df_final.to_csv(output_path, index=False)

# Print the first few rows of the transformed dataset to verify
print(df_final.head())
