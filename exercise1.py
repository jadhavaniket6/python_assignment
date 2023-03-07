import pandas as pd
import sqlite3


# Connect to the database
connection = sqlite3.connect('database')

# Read the input dataset
input_data = pd.read_csv('input_data.csv')

# Read the existing data from the target table
existing_data = pd.read_sql('SELECT * FROM target_table', connection)

# Use primary key combination to identify new, updated or deleted rows
# Primary key combination: ['id', 'date']
new_rows = input_data.merge(existing_data, on=['id', 'date'], how='left', indicator=True)
new_rows = new_rows[new_rows['_merge'] == 'left_only']
new_rows['action'] = 'I' # Insert action

updated_rows = input_data.merge(existing_data, on=['id', 'date'], how='inner', indicator=True)
updated_rows = updated_rows[updated_rows['_merge'] == 'both']
updated_rows['action'] = 'U' # Update action

deleted_rows = existing_data.merge(input_data, on=['id', 'date'], how='left', indicator=True)
deleted_rows = deleted_rows[deleted_rows['_merge'] == 'left_only']
deleted_rows['action'] = 'D' # Delete action

# Step 4: Create a new column to indicate action
input_data['action'] = ''

# Step 5: Merge the new dataset with existing dataset
merged_data = pd.concat([existing_data, new_rows, updated_rows])

# Step 6: Update existing dataset
merged_data = merged_data[merged_data['action'] != 'D']
merged_data.drop_duplicates(subset=['id', 'date'], keep='last', inplace=True)

# Step 7: Write updated dataset to target table
merged_data.to_sql('target_table', connection, if_exists='replace')
