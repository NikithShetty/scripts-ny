import pandas as pd
import sys
import os
from datetime import datetime


def validate_and_rename_columns(df):
    # Expected columns in order from ClickHouse table
    expected_columns = [
        'dep',
        'fleet',
        'type',
        'regndate',
        'obu_iemi',
        'chalo_deviceID',
        'date',
        'vehicle_number',
        'depot',
        'ny_device_id'
    ]

    # Check if we have enough columns
    if len(df.columns) < len(expected_columns):
        raise ValueError(
            f"CSV file has {len(df.columns)} columns, but we need {len(expected_columns)} columns")

    # Print column mapping
    print("\nColumn mapping:")
    print("Original -> New")
    print("-" * 50)
    for i, new_col in enumerate(expected_columns):
        if i < len(df.columns):
            print(f"{df.columns[i]} -> {new_col}")
    print("-" * 50)

    # Create new dataframe with renamed columns
    new_df = pd.DataFrame()

    # Map columns by position
    for i, new_col in enumerate(expected_columns):
        if i < len(df.columns):
            new_df[new_col] = df.iloc[:, i]

    # Convert date columns to datetime
    try:
        new_df['regndate'] = pd.to_datetime(
            new_df['regndate'], errors='coerce')
        new_df['date'] = pd.to_datetime(new_df['date'], errors='coerce')
    except Exception as e:
        print(f"Warning: Could not convert date columns: {str(e)}")

    # Clean string columns - replace empty strings and 'nan' with None
    string_columns = ['dep', 'fleet', 'type', 'obu_iemi', 'chalo_deviceID',
                      'vehicle_number', 'depot', 'ny_device_id']
    for col in string_columns:
        if col in new_df.columns:
            new_df[col] = new_df[col].replace(
                ['', 'nan', 'NaN', 'NULL', 'null'], None)

    return new_df


def main():
    if len(sys.argv) != 2:
        print("Usage: python fix-fleet-csv-header.py <path_to_csv>")
        sys.exit(1)

    input_file = sys.argv[1]

    if not os.path.exists(input_file):
        print(f"Error: File {input_file} does not exist")
        sys.exit(1)

    try:
        # Read the CSV file with all columns as strings
        print(f"Reading file: {input_file}")
        df = pd.read_csv(input_file, dtype=str)

        # Print original column names
        print("\nOriginal columns:")
        for i, col in enumerate(df.columns):
            print(f"{i}: {col}")

        # Rename and validate columns
        print("\nRenaming and validating columns...")
        df = validate_and_rename_columns(df)

        # Create output filename
        output_file = os.path.splitext(input_file)[0] + '_header_fixed.csv'

        # Save the modified CSV
        df.to_csv(output_file, index=False)
        print(f"\nSuccessfully processed {input_file}")
        print(f"Output saved to {output_file}")

        # Print final column names
        print("\nFinal columns:")
        for i, col in enumerate(df.columns):
            print(f"{i}: {col}")

    except Exception as e:
        print(f"Error processing file: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
