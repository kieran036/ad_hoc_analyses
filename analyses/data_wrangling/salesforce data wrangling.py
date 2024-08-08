"""
Created on Wed Feb 10 2020
Title: Salesforce_data_cleaning
"""
# Wrangles dirty data, performs checks on uniqueness required, and imputes new unique keys.

import pandas as pd
import re
import regex as re


# Create directory variables for reading files
file_path_prefix_sp = # SharePoint path prefix
file_path_prefix_od = # OneDrive path prefix
suffix_raw_file = # Salesforce file
suffix_cleaned_file = #Cleaned file name
suffix_key_fix_file = r'Mappings\account_id_fix_key.csv '

# Define reading and writing locations
file_loc_read = file_path_prefix_od[:-1] + suffix_raw_file
file_loc_write = file_path_prefix_sp[:-1] + suffix_cleaned_file

# Read with correct encoding
with open(file_loc_read) as f:
    file_encoding = f.encoding
df_raw_data = pd.read_csv(file_loc_read, sep=',', header=0, encoding=file_encoding)


# Clean columns function to standardise
def clean_column_names(pd_dataframe):
    # Function extracts columns, changes into string, changes to lower case, replaces non-alphanumeric to single
    # underscore, then creates replacement dictionary form columns and clean the columns
    column_names_raw = pd_dataframe.columns
    # Extract columns
    column_names_string = column_names_raw.astype(str)
    column_names_lower = column_names_string.str.lower()
    nn = len(column_names_lower)
    list_column_names_underscore = [0] * nn

    for i in range(nn):
        list_column_names_underscore[i] = re.sub('[^a-zA-Z0-9]+', '_', column_names_lower[i])

    cols_clean = pd.Series(list_column_names_underscore)
    cols_dict = dict(zip(column_names_raw, cols_clean))
    data = pd_dataframe.rename(columns=cols_dict)
    return data


# Cleans columns of dataset
df_data = clean_column_names(df_raw_data)

# Check to make sure all contact IDs are unique
count_contact_id = len(df_data['contact_id'])  # Equals value_a
df_unique_contact_id = pd.unique(df_data['contact_id'])  # Equals value_a
count_unique_contact_id = len(df_unique_contact_id)  # Equals value_a

if count_contact_id == count_unique_contact_id:
    print('No issue present', count_contact_id)
else:
    print('Issue present', count_contact_id, count_unique_contact_id)


df_unique_account_id = pd.unique(df_data['account_id'])  # Equals value_b
count_unique_account_id = len(df_unique_account_id)  # Equals value_b
count_account_id = len(df_data['account_id'])  # Equals value_a


# Check to make sure there are multiple records for account IDs
if count_account_id != count_unique_account_id:
    print('No issue present', count_unique_account_id, count_account_id)
else:
    print('Issue present', count_unique_account_id)

# Loop to build prefix key to ensure account and contact IDs are unique
capital_a = 65
capital_z = 91

caps_alphabet = ['A'] * (capital_z - capital_a)
for j in range(capital_a, capital_z):
    caps_alphabet[j - 65] = chr(j)

list_prefix_key = [0] * (26 ** 5)  # 11881376
p = 0
for k in caps_alphabet:
    for l in caps_alphabet:
        for m in caps_alphabet:
            for n in caps_alphabet:
                for o in caps_alphabet:
                    list_prefix_key[p] = k + l + m + n + o + '-'
                    p = p + 1


# Build lists for contact and account ID prefixes
list_contact_id_prefixes = list_prefix_key[:count_unique_contact_id]  # value_a
list_keys_for_account_id = list_prefix_key[:count_unique_account_id]  # value_b

# Transform into pandas series to concatenate with IDs as suffix
df_prefix_contact_id = pd.Series(list_contact_id_prefixes)
df_prefix_account_id = pd.Series(list_keys_for_account_id)

# Add new contact ID column
df_data['new_contact_id'] = df_prefix_contact_id + df_data['contact_id']
# Add contact ID error column to remove from work
df_data['account_id_error'] = df_prefix_contact_id + df_data['account_id']

# Create a key with account ID column and the correct new account ID column
df_account_id_key = pd.DataFrame(df_unique_account_id, columns=['account_id'])
df_account_id_key['new_account_id'] = df_prefix_account_id + df_unique_account_id

# Join df with contact ID error column to new account ID column
df_data = pd.merge(df_data, df_account_id_key, how='left', left_on='account_id', right_on='account_id')

# Reorder columns. Order removed to make code shareable.
df_data = pd.DataFrame(df_data,
                       columns=['##New order])

df_data['col_names'] = df_data['col_names'].str.upper()


df_account_id_fix_key = pd.DataFrame(df_data,
                                     columns=['new_account_id', 'account_id_error', 'account_id'])
df_account_id_fix_key.to_csv(file_path_prefix_sp[:-1] + suffix_key_fix_file, index=False)
# df_data.to_csv(file_loc_write, index=False)