import pandas as pd

# Function to prepare decoding dictionaries
def prepare_decoding_dicts(decodes_df):
    decode_dicts = {}
    current_column = None

    for index, row in decodes_df.iterrows():
        # Debug: Print the current row being processed
        #print(f"Processing row {index}: {row.tolist()}")

        if pd.notnull(row.iloc[0]) and row.iloc[0].startswith('['):
            current_column = row.iloc[0].strip('[]')
            if current_column not in decode_dicts:
                decode_dicts[current_column] = {}
            #print(f"New column detected: {current_column}")

        if current_column and pd.notnull(row.iloc[2]) and pd.notnull(row.iloc[3]):
            coded_value = row.iloc[2]
            decoded_value = row.iloc[3]
            decode_dicts[current_column][coded_value] = decoded_value
            #print(f"Adding to {current_column} - Code: {coded_value}, Decode: {decoded_value}")

    return decode_dicts

# Example usage with your DataFrame
# consumer_decodes_df = pd.read_csv('path_to_your_consumer_decodes_file.csv')
consumer_decodes_df = pd.read_csv(r'C:\Users\jnthn\Documents\Data_Science\DataWhizz\Decoder Files\Consumer Decodes.csv')

# Prepare the decoding dictionaries
decoding_dicts = prepare_decoding_dicts(consumer_decodes_df)

# Print the resulting dictionaries for verification
print("\nDecoding Dictionaries:")
for col, d in decoding_dicts.items():
    print(f"Column: {col}, Dictionary: {d}")

# Function to decode the dataset using the dictionaries
def decode_dataset(dataset_df, decode_dicts):
    for column in dataset_df.columns:
        if column in decode_dicts:
            dataset_df[column] = dataset_df[column].map(decode_dicts[column])
    return dataset_df

# Create the decoding dictionaries
decode_dicts = prepare_decoding_dicts(consumer_decodes_df)

# Load the dataset
consumer_df = pd.read_csv(r'C:\Users\jnthn\Documents\Data_Science\DataWhizz\old_files\Consumers.csv')

# Apply the decoding
decoded_dataset_df = decode_dataset(consumer_df, decode_dicts)

# Print the transformed dataset
print(decoded_dataset_df)

# Optionally, save the transformed dataset to a new CSV file
decoded_dataset_df.to_csv('C:/Users/jnthn/Documents/Data_Science/DataWhizz/transformed_dataset.csv', index=False)