
# Decoding File that would read from parquet and write to parquet. Should work, but has not been tested.

import pandas as pd
import os

# Function to prepare decoding dictionaries
def prepare_decoding_dicts(decodes_df):
    decode_dicts = {}
    current_column = None

    for index, row in decodes_df.iterrows():
        if pd.notnull(row.iloc[0]) and row.iloc[0].startswith('['):
            current_column = row.iloc[0].strip('[]')
            if current_column not in decode_dicts:
                decode_dicts[current_column] = {}

        if current_column and pd.notnull(row.iloc[2]) and pd.notnull(row.iloc[3]):
            coded_value = row.iloc[2]
            decoded_value = row.iloc[3]
            decode_dicts[current_column][coded_value] = decoded_value

    return decode_dicts

# Function to decode the dataset using the dictionaries
def decode_dataset(dataset_df, decode_dicts):
    for column in dataset_df.columns:
        if column in decode_dicts:
            dataset_df[column] = dataset_df[column].map(decode_dicts[column]).fillna('NaN')
    return dataset_df

# Load the decodes file
consumer_decodes_df = pd.read_csv(r'C:\Users\jnthn\Documents\Data_Science\DataWhizz\Decoder Files\Consumer Decodes.csv')

# Prepare the decoding dictionaries
decoding_dicts = prepare_decoding_dicts(consumer_decodes_df)

# Print the resulting dictionaries for verification
print("\nDecoding Dictionaries:")
for col, d in decoding_dicts.items():
    print(f"Column: {col}, Dictionary: {d}")

# Load the dataset from a Parquet file
consumer_df = pd.read_parquet(r'C:\Users\jnthn\Documents\Data_Science\DataWhizz\old_files\Consumers.parquet')

# Apply the decoding
decoded_dataset_df = decode_dataset(consumer_df, decoding_dicts)

# Print the transformed dataset
print(decoded_dataset_df)

# Ensure the output directory exists
output_dir = r'C:\Users\jnthn\Documents\Data_Science\DataWhizz\old_files'
output_file = os.path.join(output_dir, 'Transformed_Consumers.parquet')

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Save the transformed dataset to a new Parquet file
decoded_dataset_df.to_parquet(output_file, index=False)
