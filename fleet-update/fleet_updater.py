import pandas as pd
import os
import argparse
from datetime import datetime

def process_fleet_data(input_directory):
    # --- 1. Setup Paths ---
    # Get the parent of the input directory (e.g., /some_path/fleet-update/input)
    parent_of_input_dir = os.path.dirname(input_directory) 
    # The root for output folders will be a sibling to parent_of_input_dir's "input" part
    # e.g., if input_directory is ".../fleet-update/input/17-06-2025",
    # parent_of_input_dir is ".../fleet-update/input"
    # os.path.dirname(parent_of_input_dir) is ".../fleet-update"
    # output_root will be ".../fleet-update/output"
    output_root = os.path.join(os.path.dirname(parent_of_input_dir), "output")

    today_date_folder_name = datetime.now().strftime("%Y-%m-%d")
    timestamp_folder_name = datetime.now().strftime("%H-%M-%S")
    
    output_dir = os.path.join(output_root, today_date_folder_name, timestamp_folder_name)

    os.makedirs(output_dir, exist_ok=True)
    print(f"Output will be saved in: {output_dir}")

    existing_mapping_file = os.path.join(input_directory, "fleet-device-mapping.csv")
    new_ev_file = os.path.join(input_directory, "new-ev.csv")

    overlap_exists_file_out = os.path.join(output_dir, "overlap_exists.csv")
    overlap_override_file_out = os.path.join(output_dir, "overlap_override.csv")
    updated_mapping_file_out = os.path.join(output_dir, "updated-fleet-device-mapping.csv")

    # --- 2. Read and Normalize CSVs ---
    try:
        print(f"Reading existing mapping file: {existing_mapping_file}")
        existing_df_orig_case = pd.read_csv(existing_mapping_file, dtype=str)
        existing_df = existing_df_orig_case.copy()
        # Store original columns for final output
        original_existing_cols = list(existing_df_orig_case.columns)
        
        # Normalize column names to lowercase for processing
        existing_df.columns = [col.lower().replace(' ', '_') for col in existing_df.columns]
        # Specifically ensure key columns are as expected, handling variations
        rename_map_existing = {'fleet': 'fleet', 'obu_iemi': 'obu_iemi', 'dep': 'dep', 'type': 'type'}
        existing_df.rename(columns={k_orig: v_target for k_orig, v_target in rename_map_existing.items() if k_orig in existing_df.columns}, inplace=True)


        print(f"Reading new EV file: {new_ev_file}")
        new_ev_df_orig_case = pd.read_csv(new_ev_file, dtype=str)
        new_ev_df = new_ev_df_orig_case.copy()
        new_ev_df.columns = [col.lower().replace(' ', '_') for col in new_ev_df.columns]
        rename_map_new = {'fleet': 'fleet', 'obu_iemi': 'obu_iemi', 'dep': 'dep', 'type': 'type'}
        new_ev_df.rename(columns={k_orig: v_target for k_orig, v_target in rename_map_new.items() if k_orig in new_ev_df.columns}, inplace=True)

    except FileNotFoundError as e:
        print(f"Error: Input file not found. {e}")
        return
    except pd.errors.EmptyDataError as e:
        print(f"Error: One of the input files is empty or not a valid CSV. {e}")
        return

    print(f"Existing mapping data: {len(existing_df)} rows")
    print(f"New EV data: {len(new_ev_df)} rows")

    # Ensure 'fleet' column exists after normalization
    if 'fleet' not in existing_df.columns:
        print(f"Error: 'fleet' column (or equivalent like 'Fleet') missing in {existing_mapping_file} after normalization.")
        return
    if 'fleet' not in new_ev_df.columns:
        print(f"Error: 'fleet' column (or equivalent like 'FLEET') missing in {new_ev_file} after normalization.")
        return

    # Convert 'fleet' to string for consistent comparison
    existing_df['fleet'] = existing_df['fleet'].astype(str)
    new_ev_df['fleet'] = new_ev_df['fleet'].astype(str)

    # --- 3. Identify Overlaps ---
    comparison_cols = ['obu_iemi', 'dep', 'type'] # These must be present after normalization
    
    # Ensure comparison columns exist in both dataframes, fill with NaN if not, then convert to string
    for df, df_name in [(existing_df, "existing mapping"), (new_ev_df, "new EV data")]:
        for col in comparison_cols:
            if col not in df.columns:
                print(f"Warning: Column '{col}' not found in {df_name}. Will be treated as NaN for comparison.")
                df[col] = pd.NA # Add missing column as NA
            df[col] = df[col].astype(str).fillna('nan') # Convert to string, fill NaNs with 'nan' string

    overlap_exists_list = []
    overlap_override_list = []
    
    # Find fleets in new_ev_df that are also in existing_df
    merged_df = pd.merge(new_ev_df, existing_df, on='fleet', how='inner', suffixes=('_new', '_existing'))

    for index, row in merged_df.iterrows():
        is_duplicate = True
        for col in comparison_cols:
            val_new = row.get(f'{col}_new', 'nan') 
            val_existing = row.get(f'{col}_existing', 'nan')
            if val_new != val_existing:
                is_duplicate = False
                break
        
        # Get the original row from new_ev_df (with original casing) to save in overlap files
        original_new_ev_row_for_output = new_ev_df_orig_case[new_ev_df_orig_case.iloc[:, new_ev_df_orig_case.columns.get_loc('FLEET' if 'FLEET' in new_ev_df_orig_case.columns else 'fleet')].astype(str) == row['fleet']].iloc[0]

        if is_duplicate:
            overlap_exists_list.append(original_new_ev_row_for_output)
        else:
            overlap_override_list.append(original_new_ev_row_for_output)

    overlap_exists_df = pd.DataFrame(overlap_exists_list)
    overlap_override_df = pd.DataFrame(overlap_override_list)

    # Save overlap files
    if not overlap_exists_df.empty:
        overlap_exists_df.to_csv(overlap_exists_file_out, index=False)
    else:
        pd.DataFrame(columns=new_ev_df_orig_case.columns).to_csv(overlap_exists_file_out, index=False) # Save empty with headers
    print(f"Saved {len(overlap_exists_df)} exact duplicate rows to {overlap_exists_file_out}")

    if not overlap_override_df.empty:
        overlap_override_df.to_csv(overlap_override_file_out, index=False)
    else:
        pd.DataFrame(columns=new_ev_df_orig_case.columns).to_csv(overlap_override_file_out, index=False) # Save empty with headers
    print(f"Saved {len(overlap_override_df)} conflicting overlap rows to {overlap_override_file_out}")

    # --- 4. User Prompt for Overrides ---
    perform_overwrite = False
    if not overlap_override_df.empty:
        print(f"\nIMPORTANT: {overlap_override_file_out} contains {len(overlap_override_df)} fleets with differing data.")
        while True:
            user_choice = input("Do you want to overwrite the existing data with these new values? (yes/no): ").strip().lower()
            if user_choice in ['yes', 'y']:
                perform_overwrite = True
                print("Overwriting enabled for conflicting entries.")
                break
            elif user_choice in ['no', 'n']:
                perform_overwrite = False
                print("Conflicting entries will NOT be overwritten.")
                break
            else:
                print("Invalid input. Please enter 'yes' or 'no'.")
    
    # --- 5. Process and Merge Data ---
    # Start with the original existing data (with original casing)
    final_df = existing_df_orig_case.copy()
    
    # Convert 'Fleet' (or equivalent) in final_df to string for matching
    fleet_col_in_final = 'Fleet' if 'Fleet' in final_df.columns else 'fleet' # Adapt to actual casing
    if fleet_col_in_final not in final_df.columns:
        print(f"Critical error: Primary key '{fleet_col_in_final}' not found in the base DataFrame for final output.")
        return
    final_df[fleet_col_in_final] = final_df[fleet_col_in_final].astype(str)

    # Fleets to be added from new_ev.csv (these are either purely new or overrides approved by user)
    rows_to_add_from_new_ev = []

    # Handle overrides if approved
    if perform_overwrite and not overlap_override_df.empty:
        # Remove rows from final_df that will be overridden
        fleets_to_override_str = overlap_override_df.iloc[:, overlap_override_df.columns.get_loc('FLEET' if 'FLEET' in overlap_override_df.columns else 'fleet')].astype(str).tolist()
        final_df = final_df[~final_df[fleet_col_in_final].isin(fleets_to_override_str)]
        # Add these override rows (from original new_ev_df casing) to the list of rows to add
        for _, row in overlap_override_df.iterrows():
             rows_to_add_from_new_ev.append(row) # row is already a Series from overlap_override_df (original casing)

    # Identify purely new fleets (not in existing, not in overrides, not in exact duplicates)
    existing_fleet_ids = set(existing_df['fleet'].astype(str)) # Use normalized lowercase 'fleet'

    # Determine the fleet ID column name from the original new_ev.csv casing.
    # Based on the provided new-ev.csv, the column is 'FLEET'.
    fleet_col_in_new_ev_orig = 'FLEET' 

    # Check if this key column exists in the original new_ev_df_orig_case, from which overlap DFs are derived.
    if fleet_col_in_new_ev_orig not in new_ev_df_orig_case.columns:
        print(f"Error: Critical - Expected fleet ID column '{fleet_col_in_new_ev_orig}' not found in new-ev.csv columns: {list(new_ev_df_orig_case.columns)}")
        return # Stop execution if the key column is missing

    if not overlap_override_df.empty:
        if fleet_col_in_new_ev_orig in overlap_override_df.columns:
            override_fleet_ids = set(overlap_override_df[fleet_col_in_new_ev_orig].astype(str))
        else:
            # This case should ideally not happen if overlap_override_df is derived correctly from new_ev_df_orig_case
            print(f"Warning: Fleet ID column '{fleet_col_in_new_ev_orig}' not found in in-memory overlap_override_df. Columns present: {list(overlap_override_df.columns)}. Assuming no override IDs.")
            override_fleet_ids = set()
    else:
        override_fleet_ids = set()

    if not overlap_exists_df.empty:
        if fleet_col_in_new_ev_orig in overlap_exists_df.columns:
            exact_duplicate_fleet_ids = set(overlap_exists_df[fleet_col_in_new_ev_orig].astype(str))
        else:
            # Similar to above, this indicates an issue if this path is taken.
            print(f"Warning: Fleet ID column '{fleet_col_in_new_ev_orig}' not found in in-memory overlap_exists_df. Columns present: {list(overlap_exists_df.columns)}. Assuming no exact duplicate IDs.")
            exact_duplicate_fleet_ids = set()
    else:
        exact_duplicate_fleet_ids = set()
    
    processed_new_fleet_ids = override_fleet_ids.union(exact_duplicate_fleet_ids)

    for index, new_row_series_orig_case in new_ev_df_orig_case.iterrows():
        # Get fleet ID from new_row (original casing) and convert to string
        new_fleet_id_orig_case_colname = 'FLEET' if 'FLEET' in new_ev_df_orig_case.columns else 'fleet'
        current_new_fleet_id = str(new_row_series_orig_case[new_fleet_id_orig_case_colname])

        if current_new_fleet_id not in existing_fleet_ids and current_new_fleet_id not in processed_new_fleet_ids:
            rows_to_add_from_new_ev.append(new_row_series_orig_case)
        elif current_new_fleet_id in override_fleet_ids and not perform_overwrite:
            # This case is when user said NO to overwrite, these are already skipped by not adding to rows_to_add_from_new_ev
            # and not removing from final_df earlier.
            pass


    if rows_to_add_from_new_ev:
        # Convert list of Series to DataFrame
        new_rows_df_to_add = pd.DataFrame(rows_to_add_from_new_ev)
        # Ensure new_rows_df_to_add has columns matching final_df (original_existing_cols)
        # For columns in new_rows_df_to_add not in final_df, they will be added.
        # For columns in final_df not in new_rows_df_to_add, they will be NaN.
        # We need to map new_ev_df_orig_case column names to existing_df_orig_case column names
        
        # Create a mapping from new_ev_df_orig_case column names to existing_df_orig_case column names
        # This is tricky if they don't align perfectly. Assuming 'FLEET' maps to 'Fleet', etc.
        # A more robust approach would be a predefined mapping or user input for column mapping.
        # For now, let's assume a simple case-insensitive match for the key columns.
        
        # Example simple mapping (adjust if more complex mapping is needed):
        column_map_for_concat = {
            'FLEET': 'Fleet', 'Obu Iemi': 'Obu Iemi', 'DEP': 'Dep', 'Type': 'Type'
            # Add other mappings if new_ev.csv has more columns to bring in
        }
        # Filter map to only include columns present in new_rows_df_to_add
        filtered_map = {k: v for k, v in column_map_for_concat.items() if k in new_rows_df_to_add.columns}
        new_rows_df_renamed = new_rows_df_to_add.rename(columns=filtered_map)

        final_df = pd.concat([final_df, new_rows_df_renamed], ignore_index=True)
        # Reorder columns to match the original existing_df_orig_case and fill missing
        final_df = final_df.reindex(columns=original_existing_cols)


    # --- 6. Save Final Output ---
    final_df.to_csv(updated_mapping_file_out, index=False)
    print(f"\nSuccessfully created updated mapping file: {updated_mapping_file_out} with {len(final_df)} rows.")
    print("Processing complete.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Update fleet device mapping CSV.")
    parser.add_argument("input_directory", type=str, help="Path to the directory containing 'fleet-device-mapping.csv' and 'new-ev.csv'.")
    args = parser.parse_args()
    
    process_fleet_data(args.input_directory)
