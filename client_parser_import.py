import pandas as pd
import os
import re
import csv
import io # Used to read file-like objects (bytes) from uploads

# --- Parser 1: Anand Rathi (xlsx) ---
def parse_anand_rathi_format1(file_content: bytes) -> pd.DataFrame:
    """
    Parses Anand Rathi format from an uploaded file's byte content.
    - Reads 'Sheet1'.
    - Uses the first row (index 0) as the header.
    - Extracts the 'LongName' column.
    """
    try:
        # Use io.BytesIO to read the uploaded file content in memory
        df = pd.read_excel(io.BytesIO(file_content), sheet_name='Sheet1', header=0)
        
        if 'LongName' in df.columns:
            names_df = df[['LongName']].dropna().drop_duplicates()
            names_df.rename(columns={'LongName': 'Client Name'}, inplace=True)
            return names_df
        else:
            print("⚠️ Parser 'anand_rathi_format1' ran but 'LongName' column was not found.")
            return pd.DataFrame(columns=['Client Name'])
            
    except Exception as e:
        print(f"❌ Error in 'parse_anand_rathi_format1': {e}")
        return pd.DataFrame(columns=['Client Name'])

# --- Parser 2: GEPL (xlsx) ---
def parse_gepl_format1(file_content: bytes) -> pd.DataFrame:
    """
    Parses GEPL format from an uploaded file's byte content.
    - Reads 'Query Master' sheet.
    - Uses the first row (index 0) as the header.
    - Extracts the 'CLIENTNAME' column.
    """
    try:
        df = pd.read_excel(io.BytesIO(file_content), sheet_name='Query Master', header=0)
        
        if 'CLIENTNAME' in df.columns:
            names_df = df[['CLIENTNAME']].dropna().drop_duplicates()
            names_df.rename(columns={'CLIENTNAME': 'Client Name'}, inplace=True)
            return names_df
        else:
            print("⚠️ Parser 'gepl_format1' ran but 'CLIENTNAME' column was not found.")
            return pd.DataFrame(columns=['Client Name'])
                
    except Exception as e:
        print(f"❌ Error in 'parse_gepl_format1': {e}")
        return pd.DataFrame(columns=['Client Name'])

# --- Parser 3: IIFL (xlsx) ---
def parse_iifl_format1(file_content: bytes) -> pd.DataFrame:
   
    try:
        # Read the first sheet by its index (0)
        df = pd.read_excel(io.BytesIO(file_content), sheet_name=0, header=0)
        
        if 'NAME' in df.columns:
            names_df = df[['NAME']].dropna().drop_duplicates()
            names_df.rename(columns={'NAME': 'Client Name'}, inplace=True)
            return names_df
        else:
            print("⚠️ Parser 'iifl_format1' ran but 'NAME' column was not found.")
            return pd.DataFrame(columns=['Client Name'])
                
    except Exception as e:
        print(f"❌ Error in 'parse_iifl_format1': {e}")
        return pd.DataFrame(columns=['Client Name'])

    

# --- Parser 4: Motilal (csv) - Corrected v2 ---
def parse_motilal_format1(file_content: bytes) -> pd.DataFrame:
    """
    Parses the 'MOTILAL INDIVIDUAL AP CLIENT LIST.csv' format.
    - This is a standard 1-line-per-record CSV.
    - Skips the 1-line header.
    - Extracts the client name from column 2 (index 2).
    """
    names_list = []
    try:
        # Decode bytes to string
        file_text = file_content.decode('utf-8')
    except UnicodeDecodeError:
        try:
            file_text = file_content.decode('latin1') # Fallback
        except Exception as e:
            print(f"❌ Error decoding Motilal CSV: {e}")
            return pd.DataFrame(columns=['Client Name'])

    try:
        # Use csv.reader on the decoded string
        reader = csv.reader(io.StringIO(file_text))
        
        # Skip the 1-line header
        try:
            next(reader) # Skip header line
        except StopIteration:
            print("⚠️ Parser 'motilal_format1' ran but file was empty.")
            return pd.DataFrame(columns=['Client Name'])

        # Read the rest of the file
        for line in reader:
            try:
                if len(line) > 2:
                    client_name = line[2].strip() # CLIENTNAME is at index 2
                    if client_name: # Ensure it's not an empty string
                        names_list.append(client_name)
            except Exception as line_e:
                continue # Skip bad line
                        
    except Exception as e:
        print(f"❌ Error in 'parse_motilal_format1' while reading CSV: {e}")
        return pd.DataFrame(columns=['Client Name'])

    if not names_list:
        return pd.DataFrame(columns=['Client Name'])

    names_df = pd.DataFrame(names_list, columns=['Client Name'])
    names_df = names_df.drop_duplicates()
    return names_df

# --- Parser 5: PL (xlsx) ---
# --- Parser 5: PL (xlsx) - UPDATED v2 ---
def parse_pl_format1(file_content: bytes) -> pd.DataFrame:
    """
    Parses PL Client List Excel files.
    - Tries to find the correct sheet (tries multiple possible names)
    - Handles multi-row headers
    - Extracts from 'CLIENT NAME' and/or 'LD-CLIENT NAME' columns
    """
    try:
        # First, try to read all sheet names
        xls = pd.ExcelFile(io.BytesIO(file_content))
        sheet_names = xls.sheet_names
        
        print(f"   DEBUG: Found sheets: {sheet_names}")
        
        # Try different possible sheet names
        target_sheet = None
        for sheet in sheet_names:
            if 'DETAILS' in sheet.upper() or 'CLIENT' in sheet.upper():
                target_sheet = sheet
                break
        
        # If no matching sheet found, use the first sheet
        if target_sheet is None:
            target_sheet = sheet_names[0]
            print(f"   DEBUG: No 'DETAILS' sheet found, using first sheet: {target_sheet}")
        else:
            print(f"   DEBUG: Using sheet: {target_sheet}")
        
        # Read the Excel file WITHOUT treating any row as header
        df = pd.read_excel(io.BytesIO(file_content), sheet_name=target_sheet, header=None)
        
        print(f"   DEBUG: Excel shape: {df.shape}")
        print(f"   DEBUG: First 5 rows:\n{df.head()}")
        
        # Search for 'CLIENT NAME' columns in the first 10 rows
        client_name_col_indices = []
        header_row_index = None
        
        for row_idx in range(min(10, len(df))):
            row_values = df.iloc[row_idx].astype(str).str.upper()
            for col_idx, value in enumerate(row_values):
                if 'CLIENT NAME' in value:
                    if header_row_index is None:
                        header_row_index = row_idx
                    client_name_col_indices.append((col_idx, value))
                    print(f"   DEBUG: Found '{value}' at row {row_idx}, column {col_idx}")
            
            if client_name_col_indices:
                break  # Stop after finding the header row
        
        if not client_name_col_indices:
            print("⚠️ Could not find 'CLIENT NAME' column in the first 10 rows.")
            return pd.DataFrame(columns=['Client Name'])
        
        # Extract data starting from the row AFTER the header
        data_start_row = header_row_index + 1
        
        all_names_dfs = []
        
        # Extract from each CLIENT NAME column found
        for col_idx, col_name in client_name_col_indices:
            client_names = df.iloc[data_start_row:, col_idx].dropna()
            names_df_temp = pd.DataFrame(client_names.values, columns=['Client Name'])
            all_names_dfs.append(names_df_temp)
        
        if not all_names_dfs:
            print(f"❌ Parser 'pl_format1' found no client names.")
            return pd.DataFrame(columns=['Client Name'])
        
        # Combine all found names
        combined_df = pd.concat(all_names_dfs, ignore_index=True)
        
        # Clean the data
        combined_df['Client Name'] = combined_df['Client Name'].astype(str).str.strip()
        
        # Remove any leftover header text
        combined_df = combined_df[~combined_df['Client Name'].str.upper().str.contains('CLIENT NAME|CODE|EMAIL|LLD', na=False)]
        
        # Remove empty strings
        combined_df = combined_df[combined_df['Client Name'] != '']
        
        names_df = combined_df.drop_duplicates()
        
        return names_df
                
    except Exception as e:
        print(f"❌ Error in 'parse_pl_format1': {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(columns=['Client Name'])


    
# --- NEW PARSER: Anand Rathi (HTML/.xls) ---
def parse_anand_rathi_format2(file_content: bytes) -> pd.DataFrame:
    """
    Parses Anand Rathi "Format 2", which is an HTML table saved as .xls.
    - Reads the file content as text.
    - Uses pd.read_html() to find the table.
    - Uses the first row (index 0) as the header.
    - Extracts the 'Client Name' column.
    """
    try:
        # Decode bytes to string
        try:
            file_text = file_content.decode('utf-8')
        except UnicodeDecodeError:
            file_text = file_content.decode('latin1') # Fallback

        # Read the HTML table from the decoded text
        # pd.read_html returns a LIST of DataFrames. We want the first one.
        tables = pd.read_html(io.StringIO(file_text), header=0)
        
        if not tables:
            print("⚠️ Parser 'anand_rathi_format2' ran but no tables were found in the file.")
            return pd.DataFrame(columns=['Client Name'])
        
        df = tables[0]
        
        if 'Client Name' in df.columns:
            names_df = df[['Client Name']].dropna().drop_duplicates()
            # Column is already named 'Client Name', no rename needed
            return names_df
        else:
            print("⚠️ Parser 'anand_rathi_format2' ran but 'Client Name' column was not found.")
            return pd.DataFrame(columns=['Client Name'])
            
    except Exception as e:
        print(f"❌ Error in 'parse_anand_rathi_format2': {e}")
        return pd.DataFrame(columns=['Client Name'])

# ==================================================================
# --- The "Smart Router" for Client Lists ---
# ==================================================================
def parse_client_list(filename: str, file_content: bytes) -> pd.DataFrame:
 
    upper_filename = filename.upper()
    df = pd.DataFrame()

    print(f"--- Processing Client List: {filename} ---")

    # --- Router Logic ---
    try:

        if "ANAND RATHI" in upper_filename and "FORMAT 2" in upper_filename:
            print("  -> Using Anand Rathi (HTML/xls) parser.")
            df = parse_anand_rathi_format2(file_content)
        elif "ANAND RATHI" in upper_filename:
            print("   -> Using Anand Rathi (xlsx) parser.")
            df = parse_anand_rathi_format1(file_content)
        
        elif "GEPL" in upper_filename:
            print("   -> Using GEPL (xlsx) parser.")
            df = parse_gepl_format1(file_content)
        
        elif "IIFL" in upper_filename:
            print("   -> Using IIFL (xlsx) parser.")
            df = parse_iifl_format1(file_content)
        
        elif "MOTILAL" in upper_filename and filename.endswith('.csv'):
            print("   -> Using Motilal (csv) parser.")
            df = parse_motilal_format1(file_content)
        
        elif "PL CLIENT" in upper_filename: # Based on 'PL CLIENT LIST.xlsx'
            print("   -> Using PL (xlsx) parser.")
            df = parse_pl_format1(file_content)

        # --- (Add other elif checks for new formats here) ---

        else:
            print(f"⚠️ No client list parser found for file: {filename}")
            return pd.DataFrame(columns=['Client Name'])

    except Exception as e:
        print(f"❌ Unhandled error in client parser router for {filename}: {e}")
        return pd.DataFrame(columns=['Client Name'])

    if df.empty:
        print(f"ℹ️ No client names were extracted from {filename}.")
    else:
        # Clean names immediately after parsing
        df['Client Name'] = df['Client Name'].astype(str).str.strip().str.upper()
        df = df.drop_duplicates()
        print(f"✅ Successfully loaded {len(df)} unique client names from {filename}.")
        
    return df

