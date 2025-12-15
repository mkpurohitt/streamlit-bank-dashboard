import streamlit as st
import pandas as pd
import os
import io
import re
import gc
from io import BytesIO # Needed for CSV/Excel downloads


CORRECT_PASSWORD = "SSAudit@21"

# --- Import your custom parsers ---
try:
    # --- FIX 1: Changed import names to match your .py filenames ---
    import ban_statemnt_import as bank_parser
    import client_parser_import as client_parser
except ImportError as e:
    st.error(f"Error importing parser files: {e}")
    st.error("Please make sure 'bank_statement_parser.py' and 'client_file_parser.py' are in the same directory as this Streamlit app.")
    st.stop()


# --- (Step 2.B: Keyword List & File Path) ---
RED_FLAG_KEYWORDS = [
    'ADVISORY','ADVISE','MANAGEMENT','BROKER','BROKING','CONSULTANCY','FEES','WEALTH','WEALTH MANAGEMENT'
]

# --- V V V --- IMPORTANT --- V V V ---
# This hardcoded path MUST be correct on the server where the app is running
BROKER_LIST_FILEPATH = r"book2.xlsx"
# --- ^ ^ ^ --- IMPORTANT --- ^ ^ ^ ---
def show_main_dashboard():

    # --- Helper Function for CSV/Excel Download ---
    # REMOVED @st.cache_data to ensure fresh CSVs
    def convert_df_to_csv(df):
        
        return df.to_csv(index=False).encode('utf-8')

    # --- Helper Function for Broker List ---
    # REMOVED @st.cache_data to force re-load
    # --- Helper Function for Broker List ---
    @st.cache_data  # <--- UNCOMMENT THIS to save memory/speed
    def load_broker_list(file_path: str) -> pd.DataFrame:
        if not os.path.exists(file_path):
            return pd.DataFrame(columns=['Broker Name'])
            
        try:
            filename = os.path.basename(file_path)
            if filename.endswith('.csv'):
                try:
                    df = pd.read_csv(file_path, header=0, usecols=[0], encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, header=0, usecols=[0], encoding='latin1')
            elif filename.endswith('.xlsx'):
                df = pd.read_excel(file_path, sheet_name=0, header=0, usecols=[0])
            elif filename.endswith('.xls'):
                df = pd.read_excel(file_path, sheet_name=0, header=0, usecols=[0], engine='xlrd')
            else:
                return pd.DataFrame(columns=['Broker Name'])
            
            df.rename(columns={df.columns[0]: 'Broker Name'}, inplace=True)
            df['Broker Name'] = df['Broker Name'].astype(str).str.strip().str.upper()
            df = df.dropna().drop_duplicates()
            return df
            
        except Exception as e:
            return pd.DataFrame(columns=['Broker Name'])

    # --- (Phase 2: Core Analysis Logic) ---
    # REMOVED @st.cache_data to force re-run
    # --- (Phase 2: Core Analysis Logic - v2 - Split Keyword/Broker) ---
    # REMOVED @st.cache_data to force re-run
    # --- (Phase 2: Core Analysis Logic - OPTIMIZED) ---
    def run_analysis(_transactions_df, _client_df, keywords, _broker_df):
        
        st.write("Starting analysis... (Optimized Mode)")
        
        # Work on the original dataframe directly to save memory if possible, 
        # or just take the necessary columns.
        transactions_df = _transactions_df.copy()
        
        # Pre-calculate Uppercase Narration to avoid doing it repeatedly
        transactions_df['Narration_Upper'] = transactions_df['Narration'].astype(str).str.upper()

        # --- 1. Client Matching (SET INTERSECTION - FAST) ---
        st.write("Tagging Client Transactions...")
        
        # Create a SET of unique client name parts (O(1) lookup speed)
        client_name_parts_set = set()
        client_names_upper = _client_df['Client Name'].astype(str).str.strip().str.upper()
        
        for name in client_names_upper:
            parts = name.split()
            for part in parts:
                if len(part) > 2: # Ignore short parts
                    client_name_parts_set.add(part)

        # Function using Set Intersection instead of Regex
        def find_client_parts_optimized(narration_upper):
            if not narration_upper: return tuple()
            # Split narration into words using non-alphanumeric split
            # This is equivalent to \b word boundaries
            narration_words = set(re.split(r'[^A-Z0-9]+', narration_upper))
            
            # Find the intersection (common words) between narration and clients
            # This is extremely fast
            matches = narration_words.intersection(client_name_parts_set)
            
            if matches:
                return tuple(sorted(list(matches)))
            return tuple()

        transactions_df['Matched_Client_Parts'] = transactions_df['Narration_Upper'].apply(find_client_parts_optimized)

        # --- 2. Keyword/Broker Matching ---
        st.write("Tagging Flagged Transactions...")

        # Optimize Keywords: Use word set intersection for simple keywords
        keyword_set = {k.upper() for k in keywords if k}
        
        def find_keyword_optimized(narration_upper):
            if not narration_upper: return None
            narration_words = set(re.split(r'[^A-Z0-9]+', narration_upper))
            matches = narration_words.intersection(keyword_set)
            return list(matches)[0] if matches else None

        transactions_df['Matched_Keyword'] = transactions_df['Narration_Upper'].apply(find_keyword_optimized)

        # Broker Matching (Must keep Regex or substring for multi-word brokers)
        # But we optimize by compiling it once outside the loop
        broker_list = list(_broker_df['Broker Name'].astype(str).str.strip().str.upper())
        
        if broker_list:
            # We keep regex for brokers because they might be multi-word phrases
            # (e.g. "ANAND RATHI") which set intersection won't catch easily.
            # However, we only compile it if the list isn't empty.
            broker_regex = re.compile("|".join(re.escape(k) for k in broker_list if k))
            
            def find_broker(narration):
                if not narration: return None
                match = broker_regex.search(narration)
                return match.group(0) if match else None
                
            transactions_df['Matched_Broker'] = transactions_df['Narration_Upper'].apply(find_broker)
        else:
            transactions_df['Matched_Broker'] = None

        # --- 3. Amount Check ---
        transactions_df['Is_Over_5k'] = (transactions_df['Withdrawal Amt.'] > 5000) | (transactions_df['Deposit Amt.'] > 5000)
        
        # --- 4. Generate Reports ---
        st.write("Generating final reports...")
        
        # Report 1: Non-Client
        non_client_tx_df = transactions_df[
            (transactions_df['Matched_Client_Parts'].apply(len) == 0) &
            (transactions_df['Is_Over_5k'] == True)
        ].copy()

        # Report 2: Client
        client_tx_df = transactions_df[
            transactions_df['Matched_Client_Parts'].apply(len) > 0
        ].copy()

        # Report 3: Flagged
        flagged_tx_df = transactions_df[
            (transactions_df['Matched_Keyword'].notnull()) |
            (transactions_df['Matched_Broker'].notnull())
        ].copy()
        
        # --- 5. Clean up columns ---
        report1_cols_to_drop = ['Narration_Upper', 'Is_Over_5k', 'Matched_Client_Parts', 'Matched_Keyword', 'Matched_Broker']
        report2_cols_to_drop = ['Narration_Upper', 'Is_Over_5k', 'Matched_Keyword', 'Matched_Broker']
        report3_cols_to_drop = ['Narration_Upper', 'Is_Over_5k', 'Matched_Client_Parts']

        client_tx_df.drop(columns=report2_cols_to_drop, errors='ignore', inplace=True)
        non_client_tx_df.drop(columns=report1_cols_to_drop, errors='ignore', inplace=True)
        flagged_tx_df.drop(columns=report3_cols_to_drop, errors='ignore', inplace=True)
        
        # Get filter options
        all_matched_parts_for_filter = set(part for sublist in client_tx_df['Matched_Client_Parts'] for part in sublist)
        client_name_options = sorted(list(all_matched_parts_for_filter))
        
        # Force Garage Collection to free up RAM immediately
        del transactions_df
        gc.collect()
        
        return client_tx_df, non_client_tx_df, flagged_tx_df, client_name_options 


    # --- Page Configuration ---
    st.set_page_config(
        page_title="Bank Statement Analysis Dashboard",
        page_icon="📊",
        layout="wide"
    )

    st.markdown(
        """
        <h1 style='color: #f97316; font-weight: bold; font-size: 2.25rem; margin-bottom: 0.5rem;'>
            Sanjay Shah <span style='color: #3b82f6;'>& Co. LLP</span>
        </h1>
        <p style='color: #1f2937; font-weight: bold; font-size: 1.75rem; margin-top: 0; margin-bottom: 2rem;'>
            Automated Bank Statement Analysis Dashboard
        </p>
        """,
        unsafe_allow_html=True
    )
    st.write("This tool analyzes bank statements against client and broker lists to generate three reports.")

    # --- (Step 3.A: File Uploaders) ---
    st.header("1. Upload Files")
    st.info("Please upload all bank statements and all client lists. The broker list is loaded automatically from the server.")

    col1, col2 = st.columns(2)

    with col1:
        uploaded_bank_statements = st.file_uploader(
            "Upload Bank Statements (PDFs)",
            type=["pdf"],
            accept_multiple_files=True
        )

    with col2:
        uploaded_client_lists = st.file_uploader(
            "Upload Client Lists (Excel or CSV)",
            type=["xlsx", "csv", "xls"],
            accept_multiple_files=True
        )
        
    # --- Main Processing ---
    if st.button("Process and Analyze Files"):
        
        master_transactions_df = pd.DataFrame()
        master_client_df = pd.DataFrame()
        master_broker_df = pd.DataFrame()
        
        # Clear previous results from session state to ensure a fresh run
        if 'client_tx' in st.session_state: del st.session_state['client_tx']
        if 'non_client_tx' in st.session_state: del st.session_state['non_client_tx']
        if 'flagged_tx' in st.session_state: del st.session_state['flagged_tx']
        if 'client_name_options' in st.session_state: del st.session_state['client_name_options']

        st.header("2. Processing Log")
        
        # --- 1. Process Bank Statements ---
        if uploaded_bank_statements:
            with st.spinner("Processing bank statements... This may take a few minutes."):
                all_tx_dfs = []
                for file in uploaded_bank_statements:
                    try:
                        file_content_bytes = file.getvalue()
                        tx_df = bank_parser.parse_bank_statement(file.name, file_content_bytes)
                        if not tx_df.empty:
                            all_tx_dfs.append(tx_df)
                        else:
                            st.write(f"ℹ️ No transactions were parsed from {file.name}.")
                    except Exception as e:
                        st.warning(f"Failed to process bank statement '{file.name}': {e}")
                
                if all_tx_dfs:
                    master_transactions_df = pd.concat(all_tx_dfs, ignore_index=True)
                    master_transactions_df['Date'] = pd.to_datetime(master_transactions_df['Date'], errors='coerce')
                    master_transactions_df.dropna(subset=['Date'], inplace=True) # Drop bad rows
                    master_transactions_df = master_transactions_df.sort_values(by='Date').reset_index(drop=True)
                    st.success(f"Successfully parsed all the transactions from all the bank statements.")
                    del all_tx_dfs
                    gc.collect()
                    
                else:
                    st.error("Bank statement processing finished, but no transactions were extracted.")
        else:
            st.error("Please upload at least one bank statement.")

        # --- 2. Process Client Lists ---
        if uploaded_client_lists:
            with st.spinner("Processing client lists..."):
                all_client_dfs = []
                for file in uploaded_client_lists:
                    try:
                        file_content_bytes = file.getvalue()
                        client_df = client_parser.parse_client_list(file.name, file_content_bytes)
                        if not client_df.empty:
                            all_client_dfs.append(client_df)
                        else:
                            st.write(f"ℹ️ No client names were extracted from {file.name}.")
                    except Exception as e:
                        st.warning(f"Failed to process client list '{file.name}': {e}")
                
                if all_client_dfs:
                    master_client_df = pd.concat(all_client_dfs, ignore_index=True)
                    master_client_df['Client Name'] = master_client_df['Client Name'].astype(str).str.strip().str.upper()
                    master_client_df = master_client_df.drop_duplicates().reset_index(drop=True)
                    st.success(f"Successfully loaded all the client names.")
                else:
                    st.error("Client list processing finished, but no client names were extracted.")
        else:
            st.error("Please upload at least one client list.")

        # --- 3. Process Broker List (from backend) ---
        with st.spinner("Loading backend broker list..."):
            master_broker_df = load_broker_list(BROKER_LIST_FILEPATH)
            if not master_broker_df.empty:
                st.success(f"Successfully loaded all unique broker names .")
            else:
                st.error("Could not load broker list from backend. 'Flagged' report will be incomplete.")

        # --- 4. Run Analysis & Store Results ---
        if not master_transactions_df.empty and not master_client_df.empty:
            # We run the analysis even if the broker list is empty (it will just be excluded)
            client_tx, non_client_tx, flagged_tx, client_name_options = run_analysis(
                master_transactions_df,
                master_client_df,
                RED_FLAG_KEYWORDS, 
                master_broker_df
            )
            
            # Store results in session state
            st.session_state['client_tx'] = client_tx
            st.session_state['non_client_tx'] = non_client_tx
            st.session_state['flagged_tx'] = flagged_tx
            st.session_state['client_name_options']=client_name_options 
            del master_transactions_df
            del master_client_df
            del master_broker_df
            gc.collect()
            
            st.success("🎉 Analysis complete! Reports are generated below.")
        
        elif uploaded_bank_statements and uploaded_client_lists:
            st.warning("Cannot run analysis. Please check the processing log for errors.")
        else:
            st.warning("Analysis not run. Please upload both bank and client files.")


    # --- 5. Display Reports (Using Session State) ---
    st.header("3. Analysis Reports")

    if 'non_client_tx' in st.session_state:
        st.subheader(f"Report 1: Non-Client Transactions > ₹5,000")
        st.write("Transactions (Deposits or Withdrawals > 5k) that did NOT match a client name.")
        
        df_non_client = st.session_state['non_client_tx'].copy()
        
        search_non_client = st.text_input("Search Non-Client Narrations:", key="search_non")
        if search_non_client:
            df_non_client = df_non_client[df_non_client['Narration'].str.contains(search_non_client, case=False, na=False)]
        
        st.dataframe(df_non_client,hide_index=True)
        st.write(f"Total rows: {len(df_non_client)}")
        
        csv_non_client = convert_df_to_csv(df_non_client)
        st.download_button(
            label="Download Report 1 as CSV",
            data=csv_non_client,
            file_name="non_client_transactions.csv",
            mime="text/csv",
            key="btn_non_client"
        )
    else:
        st.info("Upload all files (Bank Statements, Client Lists) and click 'Process and Analyze Files' to generate Report 1.")

    if 'client_tx' in st.session_state:
        st.subheader(f"Report 2: Client Transactions")
        st.write("Transactions that matched a partial client name.")
        
        df_client = st.session_state['client_tx'].copy()
        
        try:
            options = st.session_state.get('client_name_options', [])
            
            selected_parts = st.multiselect(
                "Filter by Client Name Part (select one or more):",
                options=options,
                key="filter_client"
            )
            
            if selected_parts:
                # --- FIX 2: Changed 'Matched_Parts' to 'Matched_Client_Parts' ---
                df_client = df_client[
                    df_client['Matched_Client_Parts'].apply(lambda parts_list: any(p in selected_parts for p in parts_list))
                ]
                
        except Exception as e:
            st.error(f"Error building filter: {e}")

        search_client = st.text_input("Search Client Narrations:", key="search_client")
        if search_client:
            df_client = df_client[df_client['Narration'].str.contains(search_client, case=False, na=False)]
        
        st.dataframe(df_client, hide_index=True)
        st.write(f"Total rows: {len(df_client)}")

        csv_client = convert_df_to_csv(df_client)
        st.download_button(
            label="Download Report 2 as CSV",
            data=csv_client,
            file_name="client_transactions.csv",
            mime="text/csv",
            key="btn_client"
        )
    else:
        st.info("Upload all files and click 'Process' to generate Report 2.")

    st.divider()

    if 'flagged_tx' in st.session_state:
        st.subheader(f"Report 3: Flagged Transactions (Keywords & Brokers)")
        st.write("Transactions that matched a keyword (e.g., 'LOAN') or a broker name.")

        df_flagged = st.session_state['flagged_tx'].copy()
        
        search_flagged = st.text_input("Search Flagged Narrations:", key="search_flagged")
        if search_flagged:
            df_flagged = df_flagged[df_flagged['Narration'].str.contains(search_flagged, case=False, na=False)]
        
        st.dataframe(df_flagged, hide_index=True)
        st.write(f"Total rows: {len(df_flagged)}")

        csv_flagged = convert_df_to_csv(df_flagged)
        st.download_button(
            label="Download Report 3 as CSV",
            data=csv_flagged,
            file_name="flagged_transactions.csv",
            mime="text/csv",
            key="btn_flagged"
        )
    else:
        st.info("Upload all files and click 'Process' to generate Report 3.")

# ---
# --- PASSWORD CHECK LOGIC (This code is NOT indented) ---
def check_password():
    """Returns `True` if the user is authenticated."""

    # Initialize session state
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if st.session_state["authenticated"]:
        return True

    # --- Configure page for login ---
    st.set_page_config(
        page_title="Analysis Dashboard Login",
        layout="wide" # Use wide layout
    )

    # --- Inject Custom CSS for Login ---
    # --- Inject Custom CSS ---
    # --- Inject Custom CSS ---
    st.markdown("""
    <style>
        /* Remove any unwanted white empty space above the login card */

        /* Hide the very first container Streamlit injects */
        .stApp > div:first-child {
            margin-top: 0 !important;
            padding-top: 0 !important;
        }

        /* Hide the top block container that adds blank space */
        .stApp > div:first-child > .main > div:first-child {
            display: none !important;
            margin: 0 !important;
            padding: 0 !important;
        }

        /* Extra fallback: if Streamlit adds multiple empty containers */
        .stApp > div:first-child > .main > .block-container:has(> div:empty) {
            padding-top: 0 !important;
            margin-top: 0 !important;
        }

        /* Also ensure login container starts flush with top */
        .block-container {
            padding-top: 0 !important;
            margin-top: 0 !important;
        }
    </style>
""", unsafe_allow_html=True)

    # --- Wrap login elements in a custom div ---
    st.markdown('<div class="login-container">', unsafe_allow_html=True)

    # --- Login Form Elements ---
    st.markdown(
        """
        <h1 style='color: #f97316; font-weight: bold;'>
            Sanjay Shah <span style='color: #3b82f6;'>& Co. LLP</span>
        </h1>
        <p style='color: #4b5563; font-size: 1.1rem; margin-top: -10px; font-weight: bold;'>
            Automated Bank Statement Analysis Dashboard
        </p>
        """,
        unsafe_allow_html=True
    )

    password = st.text_input("Password", type="password", label_visibility="collapsed", placeholder="Password")

    login_button = st.button("Login") # Assign button result to variable

    st.markdown('</div>', unsafe_allow_html=True) # Close the custom div

    # --- Handle Login Button Click ---
    if login_button:
        if password == CORRECT_PASSWORD:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.toast("⚠️ Password incorrect. Please try again.", icon="error")

    return False

# --- This runs first ---
if check_password():
    show_main_dashboard() # Show the main app