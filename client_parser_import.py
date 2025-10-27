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
def parse_pl_format1(file_content: bytes) -> pd.DataFrame:
  
    try:
        df = pd.read_excel(io.BytesIO(file_content), sheet_name='18082025 DETAILS', header=0)
        
        all_names_dfs = []
        
        # Extract from the first potential name column
        if 'CLIENT NAME' in df.columns:
            df1 = df[['CLIENT NAME']].dropna().rename(columns={'CLIENT NAME': 'Client Name'})
            all_names_dfs.append(df1)
        else:
            print("⚠️ Parser 'pl_format1' did not find 'CLIENT NAME' column.")

        # Extract from the second potential name column
        if 'LD-CLIENT NAME' in df.columns:
            df2 = df[['LD-CLIENT NAME']].dropna().rename(columns={'LD-CLIENT NAME': 'Client Name'})
            all_names_dfs.append(df2)
        else:
            print("⚠️ Parser 'pl_format1' did not find 'LD-CLIENT NAME' column.")
        
        if not all_names_dfs:
            print(f"❌ Parser 'pl_format1' found no client name columns.")
            return pd.DataFrame(columns=['Client Name'])
            
        # Combine all found names, drop duplicates
        combined_df = pd.concat(all_names_dfs, ignore_index=True)
        names_df = combined_df.drop_duplicates()
        return names_df
                
    except Exception as e:
        print(f"❌ Error in 'parse_pl_format1': {e}")
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
        if "ANAND RATHI" in upper_filename:
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

