import pandas as pd
import pypdf
import os
import re
import io


def extract_text_from_pdf(filename: str, file_content: bytes) -> str:
    """
    Extracts text from PDF byte content.
    
    Args:
        filename (str): The name of the file (for logging).
        file_content (bytes): The raw byte content of the PDF file.

    Returns:
        str: The extracted text, or None if extraction fails.
    """
    try:
        
        # Use io.BytesIO to read the in-memory byte content
        pdf_reader = pypdf.PdfReader(io.BytesIO(file_content))
        
        if pdf_reader.is_encrypted:
            print(f"⚠️ Skipping encrypted PDF: {filename}")
            return None
            
        pdf_text = ""
        for page in pdf_reader.pages:
            pdf_text += page.extract_text() + "\n--- PAGE BREAK ---\n"
        
        return pdf_text
    
    except Exception as e:
        print(f"❌ Error reading {filename}: {e}")
        return None

# --- (All previous working parsers are unchanged) ---
# HDFC, Axis 1&2, AU, Bandhan, BoB, BoI, P&S, Canara, CBI, Equitas, Federal, ICICI, IDBI F2, IDFC
# (Code for previous parsers omitted for brevity - Copy the full script below)

def parse_hdfc_bank(text: str) -> pd.DataFrame:
    transactions = []
    pattern = re.compile(r"^(\d{2}/\d{2}/\d{2})\s+(.*?)\s+([\d,.]+)\s+([\d,.]+)($|\s)")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if not re.match(r"^\d{2}/\d{2}/\d{2}", line) and cleaned_lines:
            if "--- PAGE BREAK ---" not in line and "Page No .:" not in line and "Statement Summary" not in line:
                 cleaned_lines[-1] += " " + line
        else:
            cleaned_lines.append(line)
    last_balance = None
    for line in cleaned_lines:
        if "--- PAGE BREAK ---" in line or "Date Narration Chq./Ref.No." in line: continue
        match = pattern.search(line)
        if not match: continue
        try:
            date_str, narration, amount_str, balance_str = match.groups()[:4]
            balance = float(balance_str.replace(',', ''))
            amount = float(amount_str.replace(',', ''))
            withdrawal, deposit = (0.0, 0.0)
            if last_balance is not None:
                if balance > last_balance + 0.001: deposit = amount
                else: withdrawal = amount
            else:
                if "cr" in narration.lower() or "credit" in narration.lower(): deposit = amount
                else: withdrawal = amount
            transactions.append({'Date': pd.to_datetime(date_str, format='%d/%m/%y', errors='coerce'), 'Narration': narration.strip(), 'Withdrawal Amt.': withdrawal, 'Deposit Amt.': deposit, 'Closing Balance': balance})
            last_balance = balance
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    return pd.DataFrame(transactions)

def parse_axis_bank_format1(text: str) -> pd.DataFrame:
    transactions = []
    pattern = re.compile(r"^\d+\s+\d{2}/\d{2}/\d{4}")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if not pattern.match(line) and cleaned_lines:
            if "--- PAGE BREAK ---" not in line and "S.NO Transaction" not in line:
                cleaned_lines[-1] += " " + line
        else:
            cleaned_lines.append(line)
    for line in cleaned_lines:
        if not pattern.match(line): continue
        parts = line.split()
        if len(parts) < 6: continue
        try:
            tran_date, balance_str, credit_str, debit_str = parts[1], "0", "0", "0"
            end_index = len(parts) - 1
            while end_index > 3:
                if re.match(r"^[\d,.]+$", parts[end_index]):
                    balance_str = parts[end_index]; break
                end_index -= 1
            credit_str = parts[end_index - 1]; debit_str = parts[end_index - 2]
            narration_parts = parts[3:end_index - 2]
            transactions.append({'Date': pd.to_datetime(tran_date, format='%d/%m/%Y'), 'Narration': " ".join(narration_parts).strip(), 'Withdrawal Amt.': debit_str, 'Deposit Amt.': credit_str, 'Closing Balance': balance_str})
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    df = pd.DataFrame(transactions)
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip().replace('', '0')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def parse_axis_bank_format2(text: str) -> pd.DataFrame:
    transactions = []
    line_start_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if not line_start_pattern.match(line) and cleaned_lines:
            if "--- PAGE BREAK ---" not in line and "Tran Date Chq No Particulars" not in line:
                cleaned_lines[-1] += " " + line
        else:
            cleaned_lines.append(line)
    last_balance, opening_balance_match = None, re.search(r"OPENING BALANCE\s+([\d,.]+)", text, re.IGNORECASE)
    if opening_balance_match: last_balance = float(opening_balance_match.group(1).replace(',', ''))
    for line in cleaned_lines:
        if not line_start_pattern.match(line): continue
        parts = line.split()
        if len(parts) < 3: continue
        try:
            date_str = parts[0]; balance = float(parts[-1].replace(',', '')); amount = float(parts[-2].replace(',', ''))
            withdrawal, deposit = 0.0, 0.0
            if last_balance is not None:
                if balance > last_balance + 0.001: deposit = amount
                else: withdrawal = amount
            else: withdrawal = amount
            narration = " ".join(parts[1:-2])
            transactions.append({'Date': pd.to_datetime(date_str, format='%d-%m-%Y'), 'Narration': narration.strip(), 'Withdrawal Amt.': withdrawal, 'Deposit Amt.': deposit, 'Closing Balance': balance})
            last_balance = balance
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    return pd.DataFrame(transactions)

def parse_au_bank(text: str) -> pd.DataFrame:
    transactions = []
    line_start_pattern = re.compile(r"^\d{2}\s\w{3}\s\d{4}")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if "Customer ID" in line or "Account Number" in line or "Statement Period" in line: continue
        if not line_start_pattern.match(line) and cleaned_lines:
            if "--- PAGE BREAK ---" not in line and "Description/Narration" not in line:
                cleaned_lines[-1] += " " + line
        else:
            cleaned_lines.append(line)
    for line in cleaned_lines:
        if not line_start_pattern.match(line): continue
        parts = line.split()
        if len(parts) < 6: continue
        try:
            date_str = f"{parts[0]} {parts[1]} {parts[2]}"
            balance_str = parts[-1]; credit_str = parts[-2]; debit_str = parts[-3]
            narration_parts = parts[6:-3]
            transactions.append({'Date': pd.to_datetime(date_str, format='%d %b %Y'), 'Narration': " ".join(narration_parts).strip(), 'Withdrawal Amt.': debit_str, 'Deposit Amt.': credit_str, 'Closing Balance': balance_str})
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    df = pd.DataFrame(transactions)
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip().replace('-', '0')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def parse_bandhan_bank(text: str) -> pd.DataFrame:
    transactions = []
    line_start_pattern = re.compile(r"^[A-Za-z]+\s?\d{1,2},\s\d{4}")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if "Statement Summary" in line or "Disclaimer:" in line: break
        if not line_start_pattern.match(line) and cleaned_lines:
            if "--- PAGE BREAK ---" not in line and "Amount Dr / Cr" not in line:
                cleaned_lines[-1] += " " + line
        else:
            cleaned_lines.append(line)
    for line in cleaned_lines:
        if not line_start_pattern.match(line): continue
        parts = line.split()
        if len(parts) < 5: continue
        try:
            type_index = -1
            if "Cr" in parts: type_index = len(parts) - 1 - parts[::-1].index("Cr")
            elif "Dr" in parts: type_index = len(parts) - 1 - parts[::-1].index("Dr")
            if type_index == -1: continue
            date_str = parts[0] + parts[1]
            balance_str = parts[type_index + 1].replace('INR', '')
            amount_str = parts[type_index - 1].replace('INR', '')
            type_str = parts[type_index]
            narration = " ".join(parts[2:type_index - 1])
            amount = float(amount_str.replace(',', ''))
            withdrawal = amount if type_str == 'Dr' else 0.0
            deposit = amount if type_str == 'Cr' else 0.0
            transactions.append({'Date': pd.to_datetime(date_str, format='%B%d,%Y'), 'Narration': narration.strip(), 'Withdrawal Amt.': withdrawal, 'Deposit Amt.': deposit, 'Closing Balance': float(balance_str.replace(',', ''))})
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    return pd.DataFrame(transactions)

def parse_bank_of_baroda(text: str) -> pd.DataFrame:
    transactions = []
    pattern = re.compile(r"^([\d,.-]+?)(\d+)\s+(\d{2}-\d{2}-\d{4})")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if "Opening Balance" in line or not line: continue
        if not pattern.match(line) and cleaned_lines:
            if "--- PAGE BREAK ---" not in line and "Description Cheque" not in line:
                cleaned_lines[-1] += " " + line
        else:
            cleaned_lines.append(line)
    for line in cleaned_lines:
        match = pattern.search(line)
        if not match: continue
        parts = line.split()
        if len(parts) < 6: continue
        try:
            balance_str = parts[0]; date_str = parts[2]
            credit_str = parts[-1]; debit_str = parts[-2]
            narration = " ".join(parts[4:-2])
            transactions.append({'Date': pd.to_datetime(date_str, format='%d-%m-%Y'), 'Narration': narration.strip(), 'Withdrawal Amt.': debit_str, 'Deposit Amt.': credit_str, 'Closing Balance': balance_str})
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    df = pd.DataFrame(transactions)
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip().replace('-', '0')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def parse_bank_of_india(text: str) -> pd.DataFrame:
    transactions = []
    pattern = re.compile(r"^\d+\s*(\d{2}-\d{2}-\d{4})\s+(.*?)\s+([\d,.]+)\s+₹\s*([\d,.]+)")
    last_balance = None
    cleaned_text = re.sub(r'\n(?!(\d+\s+\d{2}-\d{2}-\d{4}))', ' ', text)
    for line in cleaned_text.split('\n'):
        line = line.strip()
        match = pattern.search(line)
        if not match: continue
        try:
            date_str, narration, amount_str, balance_str = match.groups()
            balance = float(balance_str.replace(',', ''))
            amount = float(amount_str.replace(',', ''))
            withdrawal, deposit = 0.0, 0.0
            if last_balance is not None:
                if balance > last_balance + 0.001: deposit = amount
                else: withdrawal = amount
            else:
                if any(x in narration.upper() for x in ['CWDR', 'DEBIT', 'DR']): withdrawal = amount
                else: deposit = amount
            transactions.append({'Date': pd.to_datetime(date_str, format='%d-%m-%Y'), 'Narration': narration.strip(), 'Withdrawal Amt.': withdrawal, 'Deposit Amt.': deposit, 'Closing Balance': balance})
            last_balance = balance
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    return pd.DataFrame(transactions)

def parse_punjab_sind_bank(text: str) -> pd.DataFrame:
    transactions = []
    line_start_pattern = re.compile(r"^\d{2}/\d{2}/\d{4}")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if not line_start_pattern.match(line) and cleaned_lines:
            if "--- PAGE BREAK ---" not in line and "Remarks Ref. No." not in line:
                cleaned_lines[-1] += " " + line
        else:
            cleaned_lines.append(line)
    for line in cleaned_lines:
        if not line_start_pattern.match(line): continue
        parts = line.split()
        if len(parts) < 5: continue
        try:
            date_str = parts[0]
            balance_str = parts[-1].replace('₹', '')
            deposit_str = parts[-2].replace('₹', '')
            withdraw_str = parts[-3].replace('₹', '')
            narration = " ".join(parts[1:-4])
            transactions.append({'Date': pd.to_datetime(date_str, format='%d/%m/%Y'), 'Narration': narration.strip(), 'Withdrawal Amt.': withdraw_str, 'Deposit Amt.': deposit_str, 'Closing Balance': balance_str})
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    df = pd.DataFrame(transactions)
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip().replace('-', '0')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def parse_canara_bank(text: str) -> pd.DataFrame:
    transactions = []
    line_start_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if "page " in line or "Date Particulars Deposits" in line or "Opening Balance" in line: continue
        if not line_start_pattern.match(line) and cleaned_lines:
            if "--- PAGE BREAK ---" not in line:
                cleaned_lines[-1] += " " + line
        else:
            cleaned_lines.append(line)
    for line in cleaned_lines:
        if not line_start_pattern.match(line): continue
        parts = line.split()
        if len(parts) < 4: continue
        try:
            date_str = parts[0]
            balance_str = parts[-1]; withdrawals_str = parts[-2]; deposits_str = parts[-3]
            narration = " ".join(parts[1:-3])
            transactions.append({'Date': pd.to_datetime(date_str, format='%d-%m-%Y'), 'Narration': narration.strip(), 'Withdrawal Amt.': withdrawals_str, 'Deposit Amt.': deposits_str, 'Closing Balance': balance_str})
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    df = pd.DataFrame(transactions)
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip()
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def parse_central_bank_of_india(text: str) -> pd.DataFrame:
    transactions = []
    line_start_pattern = re.compile(r"^\d{2}/\d{2}/\d{2}")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if "Page /" in line or "POST Date TXN Date" in line: continue
        if not line_start_pattern.match(line) and cleaned_lines:
            if "--- PAGE BREAK ---" not in line:
                cleaned_lines[-1] += " " + line
        else:
            cleaned_lines.append(line)
    for line in cleaned_lines:
        if not line_start_pattern.match(line): continue
        parts = line.split()
        if len(parts) < 5: continue
        try:
            date_str = parts[1]
            balance_str = parts[-1]; credit_str = parts[-2]; debit_str = parts[-3]
            narration = " ".join(parts[2:-3])
            transactions.append({'Date': pd.to_datetime(date_str, format='%d/%m/%y'), 'Narration': narration.strip(), 'Withdrawal Amt.': debit_str, 'Deposit Amt.': credit_str, 'Closing Balance': balance_str})
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    df = pd.DataFrame(transactions)
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip()
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def parse_equitas_bank(text: str) -> pd.DataFrame:
    transactions = []
    line_start_pattern = re.compile(r"^\d{2}-\w{3}-\d{4}")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if "Page " in line or "Date Reference No." in line: continue
        if not line_start_pattern.match(line) and cleaned_lines:
            if "--- PAGE BREAK ---" not in line:
                cleaned_lines[-1] += " " + line
        else:
            cleaned_lines.append(line)
    for line in cleaned_lines:
        if not line_start_pattern.match(line): continue
        parts = line.split()
        if len(parts) < 4: continue
        try:
            date_str = parts[0]
            balance_str = parts[-1]
            deposit_str = parts[-2]
            withdraw_str = parts[-3]
            narration = " ".join(parts[1:-3])
            transactions.append({'Date': pd.to_datetime(date_str, format='%d-%b-%Y'), 'Narration': narration.strip(), 'Withdrawal Amt.': withdraw_str, 'Deposit Amt.': deposit_str, 'Closing Balance': balance_str})
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    df = pd.DataFrame(transactions)
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip().replace('-', '0').replace('INR', '')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def parse_federal_bank(text: str) -> pd.DataFrame:
    transactions = []
    line_start_pattern = re.compile(r"^\d{2}/\d{2}/\d{4}")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if not line_start_pattern.match(line) and cleaned_lines:
            if "--- PAGE BREAK ---" not in line and "Particulars Tran" not in line:
                cleaned_lines[-1] += " " + line
        else:
            cleaned_lines.append(line)
    for line in cleaned_lines:
        if not line_start_pattern.match(line): continue
        parts = line.split()
        if len(parts) < 6: continue
        try:
            date_str = parts[0]
            balance_str = parts[-2]; deposit_str = parts[-3]; withdraw_str = parts[-4]
            tran_type_index = -1
            for i, part in enumerate(parts):
                if part == 'TFR' or len(part) > 15:
                    tran_type_index = i; break
            narration = " ".join(parts[2:tran_type_index]) if tran_type_index != -1 else " ".join(parts[2:-6])
            transactions.append({'Date': pd.to_datetime(date_str, format='%d/%m/%Y'), 'Narration': narration.strip(), 'Withdrawal Amt.': withdraw_str, 'Deposit Amt.': deposit_str, 'Closing Balance': balance_str})
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    df = pd.DataFrame(transactions)
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip()
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def parse_icici_bank(text: str) -> pd.DataFrame:
    transactions = []
    line_start_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if "DATE MODE" in line or line.startswith("B/F"): continue
        if not line_start_pattern.match(line) and cleaned_lines:
            if "--- PAGE BREAK ---" not in line:
                cleaned_lines[-1] += " " + line
        else:
            cleaned_lines.append(line)
    for line in cleaned_lines:
        if not line_start_pattern.match(line): continue
        parts = line.split()
        if len(parts) < 4: continue
        try:
            date_str = parts[0]
            balance_str = parts[-1]
            withdrawals_str = parts[-2]
            deposits_str = parts[-3]
            narration = " ".join(parts[1:-3])
            transactions.append({'Date': pd.to_datetime(date_str, format='%d-%m-%Y'), 'Narration': narration.strip(), 'Withdrawal Amt.': withdrawals_str, 'Deposit Amt.': deposits_str, 'Closing Balance': balance_str})
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    df = pd.DataFrame(transactions)
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip()
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def parse_idbi_bank_format2(text: str) -> pd.DataFrame:
    transactions = []
    line_start_pattern = re.compile(r"^\d+\.\s+\d{2}-\w{3}-\d{2}")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if "Sr Date Description" in line: continue
        if not line_start_pattern.match(line) and cleaned_lines:
             if "--- PAGE BREAK ---" not in line:
                 cleaned_lines[-1] += " " + line
        else:
             cleaned_lines.append(line)
    for line in cleaned_lines:
        if not line_start_pattern.match(line): continue
        parts = line.split()
        if len(parts) < 4: continue
        try:
            date_str = parts[1]
            type_str = parts[-1]
            amount_str = parts[-2]
            narration = " ".join(parts[2:-2])
            amount = float(amount_str.replace(',', ''))
            withdrawal = amount if type_str == 'Dr' else 0.0
            deposit = amount if type_str == 'Cr' else 0.0
            transactions.append({'Date': pd.to_datetime(date_str, format='%d-%b-%y'), 'Narration': narration.strip(), 'Withdrawal Amt.': withdrawal, 'Deposit Amt.': deposit, 'Closing Balance': 0.0 })
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    return pd.DataFrame(transactions)

def parse_idfc_first_bank(text: str) -> pd.DataFrame:
    transactions = []
    line_start_pattern = re.compile(r"^\d{2}-\w{3}-\d{4}")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if "Transaction Date Value Date" in line or "Opening Balance" in line: continue
        if not line_start_pattern.match(line) and cleaned_lines:
             prev_parts = cleaned_lines[-1].split()
             if len(prev_parts) >= 5 and re.match(r"[\d,.-]+", prev_parts[-1]):
                  if "--- PAGE BREAK ---" not in line and "REGISTERED OFFICE:" not in line:
                      cleaned_lines[-1] += " " + line
             else:
                 if "--- PAGE BREAK ---" not in line and "REGISTERED OFFICE:" not in line:
                     cleaned_lines[-1] += " " + line
        else:
            cleaned_lines.append(line)
    for line in cleaned_lines:
        if not line_start_pattern.match(line): continue
        parts = line.split()
        if len(parts) < 5: continue
        try:
            date_str = parts[0]
            balance_str = parts[-1]; credit_str = parts[-2]; debit_str = parts[-3]
            narration_end_index = -1
            for i in range(len(parts) - 3, 1, -1):
                if not parts[i].replace('.','',1).isdigit() and not parts[i].replace(',','',1).isdigit():
                    narration_end_index = i + 1; break
            narration = " ".join(parts[2:narration_end_index]) if narration_end_index != -1 else " ".join(parts[2:-3])
            transactions.append({'Date': pd.to_datetime(date_str, format='%d-%b-%Y'), 'Narration': narration.strip(), 'Withdrawal Amt.': debit_str, 'Deposit Amt.': credit_str, 'Closing Balance': balance_str})
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    df = pd.DataFrame(transactions)
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip().replace('-', '0')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

# --- CORRECTED & FINAL PARSER for Indian Bank ---
def parse_indian_bank(text: str) -> pd.DataFrame:
    transactions = []
    # This regex is designed to find transaction lines, even with merged dates/amounts
    # Group 1: Post Date (dd/mm/yy), Group 2: Value Date (dd/mm/yy)
    # Group 3: Details (Narration) - Non-greedy match, multiline allowed
    # Group 4: Chq.No (optional digits or -) - Made optional and non-capturing group more robust
    # Group 5: Debit Amount (digits, commas, dots, or -)
    # Group 6: Credit Amount (digits, commas, dots, or -)
    # Group 7: Balance (digits, commas, dots) followed by Cr/Dr
    pattern = re.compile(r"(\d{2}/\d{2}/\d{2})(\d{2}/\d{2}/\d{2})\s+(.*?)(?:\s+(-|[\d-]+))?\s+([\d,.-]+)\s*([\d,.-]+)\s*([\d,.]+)(Cr|Dr)")

    # Pre-process text: remove page breaks first
    text = text.replace("--- PAGE BREAK ---\n", "\n")
    # Remove known header/footer lines BEFORE joining narrations
    text = re.sub(r"^\s*Post DateValue Date Details.*?Balance\s*$", "", text, flags=re.MULTILINE | re.IGNORECASE)
    text = re.sub(r"^\s*Brought Forward\s+[\d,.]+cr\s*$", "", text, flags=re.MULTILINE | re.IGNORECASE)
    text = re.sub(r"^\s*Carried Forward.*?Cr\s*$", "", text, flags=re.MULTILINE | re.IGNORECASE)
    text = re.sub(r"^\s*StatementSummary.*?Extra Care\.\s*$", "", text, flags=re.MULTILINE | re.DOTALL | re.IGNORECASE)
    text = re.sub(r"^\s*Page No\..*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*STATEMENT OF ACCOUNT.*?Page No\..*$", "", text, flags=re.MULTILINE | re.DOTALL) # Remove full headers


    # Intelligent line joining specific to this format's extraction errors
    cleaned_lines = []
    current_line = ""
    date_start_pattern = re.compile(r"^\d{2}/\d{2}/\d{2}\d{2}/\d{2}/\d{2}")
    for line in text.split('\n'):
        line = line.strip()
        if not line: continue

        if date_start_pattern.match(line):
            if current_line: # Process previous complete line
                cleaned_lines.append(current_line)
            current_line = line # Start new line
        elif current_line: # Append narration parts
            current_line += " " + line
    if current_line: # Add the last line
        cleaned_lines.append(current_line)


    for line in cleaned_lines:
        match = pattern.search(line)
        if match:
            try:
                _, date_str, narration, _, debit_str, credit_str, balance_str, _ = match.groups()

                transactions.append({
                    'Date': pd.to_datetime(date_str, format='%d/%m/%y'), # Using Value Date
                    'Narration': narration.replace('\n', ' ').strip(),
                    'Withdrawal Amt.': debit_str,
                    'Deposit Amt.': credit_str,
                    'Closing Balance': balance_str
                })
            except (ValueError, IndexError) as e:
                 # print(f"Error parsing Indian Bank line: {line[:100]} -> {e}") # Debugging
                 continue
        # else:
            # print(f"Skipping Indian Bank line (no match): {line[:100]}") # Debugging


    if not transactions: return pd.DataFrame()
    df = pd.DataFrame(transactions)
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip().replace('-', '0')
        df[col] = df[col].replace('', '0')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def parse_indian_overseas_bank(text: str) -> pd.DataFrame:
    transactions = []
    line_start_pattern = re.compile(r"^\d{2}-\w{3}-\d{4}")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if "DATE CHQ NO NARRATION" in line: continue
        if not line_start_pattern.match(line) and cleaned_lines:
            if "--- PAGE BREAK ---" not in line:
                prev_parts = cleaned_lines[-1].split()
                if len(prev_parts) > 3 and re.match(r"[\d,.]+", prev_parts[-1]): # Balance?
                    cleaned_lines.append(line) # Start new line
                else:
                    cleaned_lines[-1] += " " + line # Join narration
        else:
            cleaned_lines.append(line)
    for line in cleaned_lines:
        if not line_start_pattern.match(line): continue
        parts = line.split()
        if len(parts) < 5: continue # Date, Narration..., Debit, Credit, Balance
        try:
            date_str = parts[0]
            balance_str = parts[-1]; credit_str = parts[-2]; debit_str = parts[-3]
            narration_end_index = -4 # default points before debit
            cod_index = -1
            for i in range(1, len(parts) - 3):
                if len(parts[i]) == 3 and parts[i].isupper() and i > 2:
                    cod_index = i
                    break
            if cod_index != -1:
                narration_end_index = cod_index
            if parts[1].isdigit() and len(parts[1]) < 7: # Likely a cheque number
                narration = " ".join(parts[2:narration_end_index])
            else: # No cheque number
                narration = " ".join(parts[1:narration_end_index])
            transactions.append({
                'Date': pd.to_datetime(date_str, format='%d-%b-%Y'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': debit_str,
                'Deposit Amt.': credit_str,
                'Closing Balance': balance_str
            })
        except (ValueError, IndexError): continue
    if not transactions: return pd.DataFrame()
    df = pd.DataFrame(transactions)
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip().replace('-', '0')
        df[col] = df[col].replace('', '0') # Replace empty strings
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

# --- (Parser 18: IndusInd Bank) ---
def parse_indusind_bank(text: str) -> pd.DataFrame:
    transactions = []
    line_start_pattern = re.compile(r"^\d{2}\s\w{3}\s\d{4}")
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if "Date Particulars Chq No/Ref No" in line or \
           "Transaction History" in line or \
           "Account Summary" in line:
            continue
        if not line_start_pattern.match(line) and cleaned_lines:
            if "--- PAGE BREAK ---" not in line:
                cleaned_lines[-1] += " " + line
        else:
            cleaned_lines.append(line)
    for line in cleaned_lines:
        if not line_start_pattern.match(line):
            continue
        parts = line.split()
        if len(parts) < 6:
            continue
        try:
            date_str = f"{parts[0]} {parts[1]} {parts[2]}"
            balance_str = parts[-1]
            deposit_str = parts[-2]
            withdraw_str = parts[-3]
            narration = " ".join(parts[3:-3])
            transactions.append({
                'Date': pd.to_datetime(date_str, format='%d %b %Y'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdraw_str,
                'Deposit Amt.': deposit_str,
                'Closing Balance': balance_str
            })
        except (ValueError, IndexError):
            continue
    if not transactions:
        return pd.DataFrame()
    df = pd.DataFrame(transactions)
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip().replace('-', '0')
        df[col] = df[col].replace('', '0')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

# --- (Parser 19: Kotak Bank - v1) ---
def parse_kotak_bank(text: str) -> pd.DataFrame:
    transactions = []
    # This regex splits the text into blocks, starting with "1 02 Apr 2024"
    # We use a positive lookahead in the split regex to keep the delimiter at the start
    blocks = re.split(r'\n(?=\d+\s+\d{2}\s\w{3}\s\d{4}\n)', text)

    for block in blocks:
        lines = [line.strip() for line in block.split('\n') if line.strip()]
        if not lines:
            continue

        # Check if the first line matches the transaction start pattern
        if not re.match(r"^\d+\s+\d{2}\s\w{3}\s\d{4}", lines[0]):
            continue # Skip header or other non-transaction blocks

        try:
            # First line: "1 02 Apr 2024"
            first_line_parts = lines[0].split()
            date_str = f"{first_line_parts[1]} {first_line_parts[2]} {first_line_parts[3]}"

            # Last line: "UPI-409308686583 -6,000.00 1,13,832.38" or "+68,476.00 1,82,308.38"
            last_line = lines[-1]
            last_line_parts = last_line.split()

            balance_str = last_line_parts[-1]
            amount_str = last_line_parts[-2]

            # Determine withdrawal and deposit from the +/- prefix
            amount = float(amount_str.replace(',', '').replace('+', ''))
            withdrawal = amount if amount_str.startswith('-') else 0.0
            deposit = amount if amount_str.startswith('+') else 0.0

            # --- Build Narration from all other lines ---
            narration_parts = []

            # Find the value date line (e.g., "02 Apr 2024 UPI/JUGANU")
            value_date_line_index = -1
            for i, line in enumerate(lines[1:], start=1): # Start search from 2nd line
                if re.match(r"^\d{2}\s\w{3}\s\d{4}", line):
                    value_date_line_index = i
                    break

            if value_date_line_index != -1:
                # Narration starts *after* the value date on its line
                v_date_line_parts = lines[value_date_line_index].split()
                # Check if there's text after the date on the value date line
                if len(v_date_line_parts) > 3:
                     narration_parts.append(" ".join(v_date_line_parts[3:])) # Add rest of line

                # Add all lines *after* the value date line, up to the last line
                narration_parts.extend(lines[value_date_line_index+1 : -1])
            else:
                # Fallback: just use all middle lines if no value date line found
                narration_parts.extend(lines[1:-1])

            # Add the start of the last line (e.g., "UPI-409308686583")
            # Ensure not to add amount/balance back
            if len(last_line_parts) > 2:
                 narration_parts.append(" ".join(last_line_parts[:-2]))

            narration = " ".join(part for part in narration_parts if part) # Join non-empty parts

            transactions.append({
                'Date': pd.to_datetime(date_str, format='%d %b %Y'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance_str
            })
        except (ValueError, IndexError, TypeError):
            # print(f"Skipping Kotak block due to error: {e}\n{block[:100]}") # Debug
            continue

    if not transactions:
        return pd.DataFrame()

    df = pd.DataFrame(transactions)
    # Closing balance needs cleaning (it has commas)
    df['Closing Balance'] = df['Closing Balance'].astype(str).str.replace(',', '', regex=False).str.strip()
    df['Closing Balance'] = pd.to_numeric(df['Closing Balance'], errors='coerce').fillna(0)

    return df

# --- (REFINED Parser: SBI Bank - v8 - Merges v5 and v7 for Final Fix) ---
def parse_sbi_bank(text: str) -> pd.DataFrame:
    transactions = []
    
    # Pattern to find transaction dates (d Mmm yyyy)
    date_pattern = re.compile(r"(\d{1,2}\s\w{3}\s\d{4})")
    
    # Pattern from v5: Finds the LAST TWO numbers (Amount, Balance) in a string
    money_pattern_end = re.compile(r"([\d,.-]+)\s+([\d,.]+)$")
    
    # Pattern to check if a string looks like a number
    number_like_pattern = re.compile(r"^-?[\d,.]+$")
    
    # --- 1. Find Opening Balance (from v5) ---
    last_balance = None
    ob_match = re.search(r"Balance as on .*?: ([\d,.]+)", text, re.IGNORECASE)
    if ob_match:
        try:
            last_balance = float(ob_match.group(1).replace(',', ''))
        except ValueError:
            pass

    # --- 2. Clean Text into One Giant Line (from v7) ---
    
    # Remove all page headers and footers
    text = re.sub(r"--- PAGE \d+ ---", " ", text)
    text = re.sub(r"^\s*Account Name.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*Address.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*Date.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*Account Number.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*Account Description.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*Branch.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*Drawing Power.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*Interest Rate.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*MOD Balance.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*CIF No.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*CKYCR Number.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*IFS Code.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*\(Indian Financial System\).*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*MICR Code.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*\(Magnetic Ink Character Recognition\).*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*Nomination Registered.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*Balance as on.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*Account Statement from.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*Txn Date.*Balance.*$", "", text, flags=re.MULTILINE) # The main header
    text = re.sub(r"^\s*Please check.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*State Bank of India.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*Page \d+ of \d+.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*Description.*$", "", text, flags=re.MULTILINE) # Header on new pages

    # Replace all newlines with spaces to create one giant text block per page
    text = text.replace('\n', ' ')
    text = re.sub(r'\s+', ' ', text).strip() # Consolidate spaces

    # --- 3. Split Text by Date and Process (NEW Logic) ---
    
    # Find all transaction dates. These are our split points.
    date_matches = list(date_pattern.finditer(text))
    
    if not date_matches:
        return pd.DataFrame() # No transactions found

    for i in range(len(date_matches)):
        current_date_match = date_matches[i]
        date_str = current_date_match.group(1)
        
        # Define the start and end of this transaction's text block
        start_index = current_date_match.start()
        
        end_index = None
        if i + 1 < len(date_matches):
            # If not the last match, end at the start of the next date
            end_index = date_matches[i+1].start()
        else:
            # If it is the last match, go to the end of the text
            end_index = len(text)
            
        # Get the full text for this single transaction
        transaction_block = text[start_index:end_index].strip()
        
        # --- 4. Process Each Block (using v5's logic) ---
        try:
            # Find the LAST two numbers in this block
            money_match = money_pattern_end.search(transaction_block)
            
            if not money_match:
                continue # Skip if no money found

            raw_amount, balance_str_raw = money_match.groups()
            
            # Check if balance is valid
            if not number_like_pattern.match(balance_str_raw.replace(',', '')):
                continue
                
            balance_str = balance_str_raw
            amount_str = raw_amount.strip() if raw_amount.strip() and raw_amount.strip() != '-' else "0"
            
            # The narration is everything from the end of the first date
            # to the start of the money match
            narration_start_index = current_date_match.end() - start_index
            narration_end_index = money_match.start()
            
            narration = transaction_block[narration_start_index:narration_end_index].strip()
            
            # Clean the narration (remove the Value Date)
            narration = re.sub(r"^\d{1,2}\s\w{3}\s\d{4}\s*", "", narration).strip()
            narration = narration.replace("Ref No./Cheque No.", "").strip()

            # --- 5. Infer Deposit/Withdrawal (from v5) ---
            withdrawal = 0.0
            deposit = 0.0
            
            amount_cleaned = amount_str.replace(',', '')
            balance_cleaned = balance_str.replace(',', '')
            
            amount = float(amount_cleaned) if amount_cleaned else 0.0
            current_balance = float(balance_cleaned) if balance_cleaned else 0.0

            if last_balance is not None:
                if current_balance > last_balance + 0.001:
                    deposit = amount
                elif current_balance < last_balance - 0.001:
                    withdrawal = amount
                # else: balance is same
            else:
                # Fallback for first transaction
                narration_upper = narration.upper()
                if "BY TRANSFER" in narration_upper or "NEFT " in narration_upper or "CR " in narration_upper:
                    deposit = amount
                else: 
                    withdrawal = amount

            transactions.append({
                'Date': pd.to_datetime(date_str, format='%d %b %Y'),
                'Narration': narration,
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': current_balance
            })
            
            last_balance = current_balance # Update for next loop

        except Exception as e:
            # print(f"Error processing block: {transaction_block} -> {e}")
            continue

    if not transactions:
        return pd.DataFrame()

    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    
    return df

# --- (NEW Parser: UCO Bank) ---
# --- (REFINED Parser: UCO Bank - v2) ---
def parse_uco_bank(text: str) -> pd.DataFrame:
    transactions = []
    # Header pattern - allow optional space before Chq.
    header_pattern = re.compile(r"Date\s+Particulars\s+Withdrawals\s+Deposits\s+Balance\s*Chq\.\s+No\.")
    # Transaction start pattern (dd-MM-yyyy)
    date_pattern = re.compile(r"^(\d{2}-\d{2}-\d{4})")
    # Money pattern: Find LAST two number-like groups on the line.
    # Group 1: Second-to-last number (Withdrawal or Deposit) - allow empty/dash
    # Group 2: Last number (Balance) - MUST exist
    money_pattern_last_two = re.compile(r"([\d,.-]*)\s+([\d,.]+)$")
    # Pattern to check if a string looks like a valid number for balance
    balance_like_pattern = re.compile(r"^[\d,.]+$") # Balance cannot be negative or just '-'

    lines = text.split('\n')
    data_started = False
    header_found = False
    current_transaction_lines = []
    start_line_index = -1

    # --- Find Header ---
    for i, line in enumerate(lines):
        line_strip = line.strip()
        if header_pattern.search(line_strip):
            header_found = True
            start_line_index = i + 1
            # print(f"DEBUG UCO v2: Header found at line {i}, data starts {start_line_index}") # Debug
            break

    if not header_found or start_line_index >= len(lines):
        # print("DEBUG UCO v2: Header not found or no lines after header.") # Debug
        return pd.DataFrame() # Header not found
    # --- End Find Header ---

    # --- Helper function ---
    def process_block(block_lines):
        if not block_lines: return None
        
        full_block = " ".join(block_lines)
        full_block = re.sub(r'\s{2,}', ' ', full_block).strip() # Consolidate spaces
        
        date_match = date_pattern.match(block_lines[0])
        if not date_match: return None
        date_str = date_match.group(1)

        # Find the *last two* number-like words
        words = full_block.split()
        raw_amount = ""
        balance_str = ""
        money_start_index = -1
        
        if len(words) >= 2:
            # Check if last word is a valid balance
            if balance_like_pattern.match(words[-1].replace(',','')):
                 balance_str = words[-1]
                 # Check if second-to-last word looks like an amount (can be number or '-')
                 if re.match(r"^-?[\d,.]+$", words[-2].replace(',','')):
                      raw_amount = words[-2]
                      # Estimate start index of the amount value for narration slicing
                      search_str = f" {raw_amount} {balance_str}"
                      money_start_index = full_block.rfind(search_str)
                      if money_start_index == -1: # Try without leading space if not found
                           search_str = f"{raw_amount} {balance_str}"
                           money_start_index = full_block.rfind(search_str)

        if balance_str and money_start_index != -1: # If balance and potential amount found
            try:
                narration = full_block[date_match.end():money_start_index].strip()
                
                # Determine if raw_amount is Debit or Credit based on balance change
                # (Requires tracking previous balance - skipping for simplicity now,
                # assigning based on value presence - less accurate but functional)
                
                # Treat raw_amount as deposit if positive, withdrawal if negative or if it's the only value besides balance
                # Note: This is an assumption. UCO seems to use separate columns.
                # Let's refine based on the example: 'Withdrawals Deposits Balance'
                # So the second last number is Deposits, third last is Withdrawals.
                
                withdraw_str = "0"
                deposit_str = "0"
                
                # Try finding 3 numbers first (Withdrawal Deposit Balance)
                money_match_3 = re.search(r"([\d,.-]*)\s+([\d,.-]*)\s+([\d,.]+)$", full_block)
                if money_match_3 and len(money_match_3.groups()) == 3:
                     w_str, d_str, b_str = money_match_3.groups()
                     # Check if b_str matches the balance we found with the 2-word check
                     if b_str == balance_str:
                         withdraw_str = w_str.strip() if w_str.strip() and w_str.strip() != '-' else "0"
                         deposit_str = d_str.strip() if d_str.strip() and d_str.strip() != '-' else "0"
                         # Recalculate money_start_index based on 3 values
                         money_start_index = money_match_3.start()
                         narration = full_block[date_match.end():money_start_index].strip()
                     else: # 3-number match seemed wrong, stick with 2-number assumption
                           # Assume raw_amount is withdrawal if negative, deposit otherwise (less ideal)
                           # A better guess: UCO format is W D B. If only 2 found, assume it's D B or W B.
                           # Check if raw_amount looks like Withdrawal (often larger numbers, or explicit debits)
                           # Let's assume raw_amount is Withdrawal if present, else 0
                           # Let's assume Deposit is always 0 in the 2-number case (needs verification)
                           if raw_amount and raw_amount != '-':
                                withdraw_str = raw_amount
                           # Need a way to distinguish W B from D B...
                           # Fallback: Assume Withdrawals if present, else assume Deposits. Check sample:
                           # 03-04-2024 ... BROKE 20,113.13 11,440.00 -> Deposit, Balance (Withdrawal=0)
                           # 05-04-2024 ... ybl/Pa 11,000.00 9,113.13 -> Withdrawal, Balance (Deposit=0)
                           # 23-05-2024 ... 2023-24 5.60 10,648.53 -> Withdrawal, Balance (Deposit=0)
                           # Conclusion: Need to infer W vs D. Simple approach: If raw_amount, assume W=raw_amount, D=0. This will be wrong for deposits.
                           # Let's refine: Use balance change if possible (more complex), OR
                           # Check if BOTH W and D seem missing. If only 2 numbers, assume W=0 D=amount OR W=amount D=0.
                           # How to decide? Look for keywords? 'CWDR'? 'NEFT'?
                           # Let's try matching only 2 numbers explicitly: W B or D B
                           money_match_WB = re.search(r"([\d,.-]+)\s+([\d,.]+)$", full_block) # Withdrawal + Balance
                           # We already matched raw_amount and balance_str which represents this case
                           
                           # We cannot reliably distinguish W B from D B without more context (like prev balance)
                           # Simplest assumption: If 2 numbers, W=raw_amount, D=0. Will fail for deposits.
                           # Alternative: If 2 numbers, W=0, D=raw_amount. Will fail for withdrawals.
                           # Let's try checking keywords:
                           if "CWDR" in narration or "UPI/TRTR" in narration: # Likely withdrawal
                               withdraw_str = raw_amount
                           else: # Assume deposit otherwise (e.g., NEFT credits)
                               deposit_str = raw_amount


                elif balance_str: # Only balance found? Maybe a header remnant or parse error
                    return None

                # Clean narration again
                narration = re.sub(r'\s+', ' ', narration).strip()

                return {
                    'Date': pd.to_datetime(date_str, format='%d-%m-%Y'),
                    'Narration': narration,
                    'Withdrawal Amt.': withdraw_str,
                    'Deposit Amt.': deposit_str,
                    'Closing Balance': balance_str
                }
            except Exception as e:
                # print(f"UCO v2 Error processing: {e} | Block: {full_block}") # Debug
                return None
        else:
             # print(f"UCO v2 Money pattern failed | Block: {full_block}") # Debug
             return None
    # --- End helper function ---

    # --- Process lines after header ---
    for i in range(start_line_index, len(lines)):
        line = lines[i].strip()

        if not line: continue # Skip empty lines

        date_match = date_pattern.match(line)
        if date_match:
            # Process previous block
            parsed = process_block(current_transaction_lines)
            if parsed: transactions.append(parsed)
            # Start new block
            current_transaction_lines = [line]
        elif current_transaction_lines:
            # Append narration line
            current_transaction_lines.append(line)
    
    # Process the last block
    parsed = process_block(current_transaction_lines)
    if parsed: transactions.append(parsed)
    # --- End process lines ---

    if not transactions:
        return pd.DataFrame()

    df = pd.DataFrame(transactions)
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.strip().replace('-', '0', regex=False)
        df[col] = df[col].str.replace(',', '', regex=False)
        df[col] = df[col].replace('', '0')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df
# --- (REFINED Parser: Union Bank - v2 - Corrected Logic) ---
# --- (REFINED Parser: Union Bank - v3.1 - Bug Fix) ---
def parse_union_bank(text: str) -> pd.DataFrame:
    transactions = []
    # Header pattern
    header_pattern = re.compile(r"Date\s+Tran Id-1Remarks\s+UTR Number\s+Instr\. ID\s+Withdrawals\s+Deposits\s+Balance")
    # Transaction start pattern: dd-MM-yyyy (must be at start of line)
    date_pattern = re.compile(r"^(\d{2}-\d{2}-\d{4})")
    # Time pattern (must be at start of line, immediately after date line)
    time_pattern = re.compile(r"^\d{2}:\d{2}:\d{2}")
    # Money pattern: Two number-like values at the end of a line
    money_pattern_end = re.compile(r"([\d,.-]+)\s+([\d,.]+)$")
    # Pattern to check if a word looks like a number/amount
    number_like_pattern = re.compile(r"^-?[\d,.]+$")

    lines = text.split('\n')
    data_started = False
    current_transaction_lines = []
    current_date_str = None
    last_balance = None # --- ADDED: To track balance ---

    # --- Find Header ---
    start_line_index = -1
    for i, line in enumerate(lines):
         line_strip = line.strip()
         if header_pattern.search(line_strip):
             start_line_index = i + 1
             data_started = True
             # print(f"DEBUG Union v3: Header found at line {i}, data starts {start_line_index}") # Debug
             break
    
    if not data_started:
         return pd.DataFrame()
    # --- End Find Header ---

    # --- Helper function ---
    def process_block(block_lines, date_str, prev_balance):
        # *** BUG FIX HERE: Must return a tuple, even when block is empty ***
        if not block_lines: return None, prev_balance 
        # *** END BUG FIX ***

        full_block = " ".join(block_lines)
        full_block = re.sub(r'\s{2,}', ' ', full_block).strip()

        # Find money values
        words = full_block.split()
        amount_str, balance_str = None, None
        money_start_index = -1

        if len(words) >= 2:
            w1, w2 = words[-2], words[-1]
            if number_like_pattern.match(w1.replace(',','')) and number_like_pattern.match(w2.replace(',','')):
                amount_str, balance_str = w1, w2
                search_str = f" {amount_str} {balance_str}"
                money_start_index = full_block.rfind(search_str)
                if money_start_index == -1:
                    search_str = f"{amount_str} {balance_str}"
                    money_start_index = full_block.rfind(search_str)
        
        if money_start_index != -1:
            try:
                narration = full_block[:money_start_index].strip()
                first_line_time_match = time_pattern.match(block_lines[0])
                if first_line_time_match:
                    narration = narration[first_line_time_match.end():].strip()
                
                narration = re.sub(r'\s+', ' ', narration).strip()

                # --- Infer Debit/Credit using Balance ---
                withdrawal = 0.0
                deposit = 0.0
                amount = float(amount_str.replace(',', ''))
                current_balance = float(balance_str.replace(',', ''))

                if prev_balance is not None:
                    # Use a small tolerance for floating point comparison
                    if current_balance > prev_balance + 0.001:
                        deposit = amount
                    elif current_balance < prev_balance - 0.001:
                        withdrawal = amount
                    else: # Balance is same, amount is likely 0
                        pass # Both are 0.0
                else:
                    # First transaction, fall back to simple keyword check
                    narration_upper = narration.upper()
                    if "UPIAR/DR/" in narration_upper or "APY-SI-" in narration_upper or "CWDR/" in narration_upper:
                        withdrawal = amount
                    else: # Default to deposit for first transaction if not a clear debit
                        deposit = amount

                return {
                    'Date': pd.to_datetime(date_str, format='%d-%m-%Y'),
                    'Narration': narration,
                    'Withdrawal Amt.': withdrawal,
                    'Deposit Amt.': deposit,
                    'Closing Balance': current_balance # Return as float
                }, current_balance # Return the new balance to update last_balance

            except Exception as e:
                # print(f"Union v3 (process): Error: {e} | Block: {full_block}") # Debug
                return None, prev_balance # Return None and old balance
        else:
             # print(f"Union v3 (process): Money pattern failed | Block: {full_block}") # Debug
             return None, prev_balance # Return None and old balance
    # --- End helper function ---

    # --- Main Loop ---
    for i in range(start_line_index, len(lines)):
        line = lines[i].strip()
        if not line: continue

        date_match = date_pattern.match(line)
        time_match_on_next = (i + 1 < len(lines)) and time_pattern.match(lines[i+1].strip())

        if date_match and time_match_on_next:
            # Process the previous block
            parsed_data, new_balance = process_block(current_transaction_lines, current_date_str, last_balance)
            if parsed_data:
                transactions.append(parsed_data)
                last_balance = new_balance # *** UPDATE LAST BALANCE ***
            
            # Start new block
            current_date_str = date_match.group(1)
            current_transaction_lines = [] # Reset buffer
        elif current_date_str and not date_match:
             if not line.startswith("Statement Date :"): # Skip footer
                 current_transaction_lines.append(line)
    # --- End Main Loop ---

    # Process the last block
    parsed_data, new_balance = process_block(current_transaction_lines, current_date_str, last_balance)
    if parsed_data:
        transactions.append(parsed_data)

    if not transactions:
        return pd.DataFrame()

    df = pd.DataFrame(transactions)
    # Money cols already floats, just return
    return df

# --- (NEW Parser: YES Bank) ---
def parse_yes_bank(text: str) -> pd.DataFrame:
    transactions = []
    # Header pattern
    header_pattern = re.compile(r"Date\s+Value\s+Date\s+Cheque\s+No/Reference\s+No\s+Description\s+Withdrawals\s+Deposits\s+Running\s+Balance")
    # Transaction start pattern: dd Mmm yyyy dd Mmm yyyy
    date_pattern = re.compile(r"^(\d{2}\s\w{3}\s\d{4})\s+(\d{2}\s\w{3}\s\d{4})")
    # Money pattern: Two number-like values at the end of a line
    money_pattern_end = re.compile(r"([\d,.-]+)\s+([\d,.]+)$")
    # Pattern to check if a word looks like a number/amount
    number_like_pattern = re.compile(r"^-?[\d,.]+$")

    lines = text.split('\n')
    data_started = False
    current_transaction_lines = []
    current_date_str = None
    last_balance = None

    # --- Find Header ---
    start_line_index = -1
    for i, line in enumerate(lines):
         line_strip = re.sub(r'\s+', ' ', line.strip()) # Consolidate spaces
         if header_pattern.search(line_strip):
             start_line_index = i + 1
             data_started = True
             # print(f"DEBUG YES: Header found at line {i}, data starts {start_line_index}") # Debug
             break

    if not data_started:
         # Fallback: check for "Transaction details for your account number"
         for i, line in enumerate(lines):
             if "Transaction details for your account number" in line:
                  # Try to find header again in the next few lines
                  for j in range(i + 1, min(i + 10, len(lines))):
                       line_strip = re.sub(r'\s+', ' ', lines[j].strip())
                       if header_pattern.search(line_strip):
                           start_line_index = j + 1
                           data_started = True
                           # print(f"DEBUG YES: Fallback Header found at line {j}, data starts {start_line_index}") # Debug
                           break
             if data_started: break

    if not data_started or start_line_index >= len(lines):
         # print("DEBUG YES: Header not found.") # Debug
         return pd.DataFrame()
    # --- End Find Header ---

    # --- Helper function (Identical to Union Bank v3.1) ---
    def process_block(block_lines, date_str, prev_balance):
        if not block_lines: return None, prev_balance 

        full_block = " ".join(block_lines)
        full_block = re.sub(r'\s{2,}', ' ', full_block).strip()

        words = full_block.split()
        amount_str, balance_str = None, None
        money_start_index = -1

        if len(words) >= 2:
            w1, w2 = words[-2], words[-1]
            if number_like_pattern.match(w1.replace(',','')) and number_like_pattern.match(w2.replace(',','')):
                amount_str, balance_str = w1, w2
                search_str = f" {amount_str} {balance_str}"
                money_start_index = full_block.rfind(search_str)
                if money_start_index == -1:
                    search_str = f"{amount_str} {balance_str}"
                    money_start_index = full_block.rfind(search_str)

        if money_start_index != -1:
            try:
                narration = full_block[:money_start_index].strip()
                narration = re.sub(r'\s+', ' ', narration).strip()

                withdrawal = 0.0
                deposit = 0.0
                # Handle amount being empty string or dash (though unlikely here)
                amount_str_cleaned = amount_str.replace(',', '').strip()
                amount = float(amount_str_cleaned) if amount_str_cleaned and amount_str_cleaned != '-' else 0.0
                current_balance = float(balance_str.replace(',', ''))

                if prev_balance is not None:
                    if current_balance > prev_balance + 0.001:
                        deposit = amount
                    elif current_balance < prev_balance - 0.001:
                        withdrawal = amount
                    else: # Balance same
                         # Check if amount is non-zero (like a fee that was reversed)
                         if amount > 0: # Could be either, default to withdrawal?
                             # print(f"YES Bank: Zero balance change with amount {amount}. Defaulting to withdrawal.") # Debug
                             withdrawal = amount 
                else:
                    # First transaction, fall back to simple keyword check
                    narration_upper = narration.upper()
                    if "ACH DR" in narration_upper:
                        withdrawal = amount
                    else: # Default to deposit for first transaction if not a clear debit
                        deposit = amount

                return {
                    'Date': pd.to_datetime(date_str, format='%d %b %Y'),
                    'Narration': narration,
                    'Withdrawal Amt.': withdrawal,
                    'Deposit Amt.': deposit,
                    'Closing Balance': current_balance
                }, current_balance

            except Exception as e:
                # print(f"YES (process): Error: {e} | Block: {full_block}") # Debug
                return None, prev_balance
        else:
             # print(f"YES (process): Money pattern failed | Block: {full_block}") # Debug
             return None, prev_balance
    # --- End helper function ---

    # --- Main Loop ---
    for i in range(start_line_index, len(lines)):
        line = lines[i].strip()
        if not line: continue

        date_match = date_pattern.match(line)

        if date_match:
            # Process the previous block
            parsed_data, new_balance = process_block(current_transaction_lines, current_date_str, last_balance)
            if parsed_data:
                transactions.append(parsed_data)
                last_balance = new_balance # *** UPDATE LAST BALANCE ***

            # Start new block
            current_date_str = date_match.group(1) # Txn Date
            narration_start_on_date_line = line[date_match.end():].strip()
            current_transaction_lines = [narration_start_on_date_line] # Start buffer with rest of line
        elif current_date_str: # If we are inside a transaction block
             if not line.startswith("Page ") and not line.startswith("Statement of account:"): # Skip footers/headers
                 current_transaction_lines.append(line)
    # --- End Main Loop ---

    # Process the last block
    parsed_data, new_balance = process_block(current_transaction_lines, current_date_str, last_balance)
    if parsed_data:
        transactions.append(parsed_data)

    if not transactions:
        return pd.DataFrame()

    df = pd.DataFrame(transactions)
    return df

def parse_bank_of_baroda_format2(text: str) -> pd.DataFrame:
    transactions = []
    # Transaction line starts with DD-MM-YYYY
    line_start_pattern = re.compile(r"^(\d{2}-\d{2}-\d{4})")
    # Header line to skip
    header_pattern = re.compile(r"DATE\s+PARTICULARS\s+CHQ\.NO\.\s+WITHDRAWALS\s+DEPOSITS\s+BALANCE")
    # Pattern to find 3 money values at the end
    money_pattern_end = re.compile(r"([\d,.-]+)\s+([\d,.-]+)\s+([\d,.]+)$")
    
    cleaned_lines = []
    for line in text.split('\n'):
        line = line.strip()
        # Skip empty lines, headers, and separators
        if not line or header_pattern.match(line) or line.startswith("---"):
            continue
        # Skip lines clearly part of the top header block
        if "IFSC CODE:" in line or "A/C Name" in line or "Statement of account" in line:
            continue
            
        # Join multi-line narrations
        if not line_start_pattern.match(line) and cleaned_lines:
            # Check if the previous line looks like a complete transaction ending in amounts
            prev_line_suffix = cleaned_lines[-1][-50:] # Check last 50 chars
            if money_pattern_end.search(prev_line_suffix):
                 cleaned_lines.append(line) # Start a new line if prev seemed complete
            else:
                 cleaned_lines[-1] += " " + line # Append narration part
        else:
            cleaned_lines.append(line)

    for line in cleaned_lines:
        if not line_start_pattern.match(line):
            continue
            
        date_match = line_start_pattern.match(line)
        date_str = date_match.group(1)
        
        # Find the money parts at the end
        money_match = money_pattern_end.search(line)
        if not money_match:
            continue
            
        try:
            withdraw_str = money_match.group(1).strip()
            deposit_str = money_match.group(2).strip()
            balance_str = money_match.group(3).strip()
            
            # Extract narration (everything between date and first amount)
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            narration = line[narration_start_index:narration_end_index].strip()
            # Remove potential Chq.No. if it's the first part of narration
            narration = re.sub(r"^\d+\s+", "", narration).strip()
            
            transactions.append({
                'Date': pd.to_datetime(date_str, format='%d-%m-%Y'),
                'Narration': narration,
                'Withdrawal Amt.': withdraw_str,
                'Deposit Amt.': deposit_str,
                'Closing Balance': balance_str
            })
        except (ValueError, IndexError):
            continue
            
    if not transactions:
        return pd.DataFrame()
        
    df = pd.DataFrame(transactions)
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip().replace('-', '0')
        df[col] = df[col].replace('', '0') # Handle potentially empty strings for amounts
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
    return df

# --- (NEW Parser: Dhanlaxmi Bank) ---
def parse_dhanlaxmi_bank(text: str) -> pd.DataFrame:
    transactions = []
    
    # This pattern identifies the START of a new transaction line
    line_start_pattern = re.compile(r"^(\d{2}-\w{3}-\d{4})")
    
    # This pattern finds the three money amounts at the END of a block
    money_pattern_end = re.compile(r"([\d,.-]+)\s+([\d,.-]+)\s+([\d,.]+)$")

    # This helper function processes a single transaction block
    def process_block(block_lines):
        try:
            if not block_lines:
                return None
            
            # Join all lines in the block into one string
            full_block = " ".join(block_lines).replace('\n', ' ').strip()
            
            # --- 1. Find Date from the start ---
            date_match = line_start_pattern.match(full_block)
            if not date_match:
                return None # Not a transaction
            
            date_str = date_match.group(1)
            
            # --- 2. Find Money at the end ---
            money_match = money_pattern_end.search(full_block)
            if not money_match:
                return None # Not a transaction
            
            debit_str = money_match.group(1)
            credit_str = money_match.group(2)
            balance_str = money_match.group(3)
            
            # --- 3. Extract Narration ---
            # Narration is between the Value Date and the money
            parts = full_block.split()
            if len(parts) < 6: # Date, ValDate, Narration..., Debit, Credit, Balance
                return None
            
            value_date_str = parts[1]
            narration_start_index = full_block.find(value_date_str) + len(value_date_str)
            narration_end_index = money_match.start()
            
            narration_block = full_block[narration_start_index:narration_end_index].strip()
            
            # The Cheque No. is at the end of the narration block
            cheque_match = re.search(r"\s+([.\d]+)$", narration_block)
            narration = ""
            
            if cheque_match:
                # Cheque number found
                narration = narration_block[:cheque_match.start()].strip()
            else:
                # No cheque number found
                narration = narration_block.strip()
            
            # Handle the "B/F ..." line
            if "B/F ..." in narration:
                narration = "B/F"
                
            return {
                'Date': pd.to_datetime(date_str, format='%d-%b-%Y', errors='coerce'),
                'Narration': narration,
                'Withdrawal Amt.': debit_str,
                'Deposit Amt.': credit_str,
                'Closing Balance': balance_str
            }
        except Exception as e:
            return None

    # --- Main Loop (State Machine) ---
    current_block_lines = []
    data_started = False

    for line in text.split('\n'):
        line = line.strip()

        # Skip lines until we find the header
        if not data_started:
            if "DATE VALUE DATE DESCRIPTION" in line:
                data_started = True
            continue
        
        # We are after the header now
        if not line or "--- PAGE BREAK ---" in line or "Page No:" in line or "STATEMENT OF ACCOUNT" in line:
            continue
            
        # Check if this line is the START of a new transaction
        if line_start_pattern.match(line):
            # Process the *previous* block first.
            if current_block_lines:
                parsed_txn = process_block(current_block_lines)
                if parsed_txn:
                    transactions.append(parsed_txn)
            
            # Now, start the new block
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is not a start line, so it's a continuation
            # (narration or amounts). Append it to the current block.
            current_block_lines.append(line)

    # After the loop, process the very last block
    if current_block_lines:
        parsed_txn = process_block(current_block_lines)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of Main Loop ---

    if not transactions:
        return pd.DataFrame()

    df = pd.DataFrame(transactions)
    
    # 4. Clean the money columns
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip().replace('-', '0')
        df[col] = df[col].replace('', '0')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
    df = df.dropna(subset=['Date'])
    return df

# --- (NEW Parser: IndusInd Bank - Format 3) ---
def parse_indusind_bank_format3(text: str) -> pd.DataFrame:
    transactions = []
    
    # This pattern identifies the START of a new transaction line
    line_start_pattern = re.compile(r"^(\d{2}\s\w{3}\s\d{4})")
    
    # This pattern finds the three money amounts at the END of a block
    # It must handle the "-" character for zero values.
    money_pattern_end = re.compile(r"([\d,.-]+)\s+([\d,.-]+)\s+([\d,.]+)$")

    # This helper function processes a single transaction block
    def process_block(block_lines):
        try:
            if not block_lines:
                return None
            
            # Join all lines in the block into one string
            full_block = " ".join(block_lines).replace('\n', ' ').strip()
            
            # --- 1. Find Date from the start ---
            date_match = line_start_pattern.match(full_block)
            if not date_match:
                return None # Not a transaction
            
            date_str = date_match.group(1)
            
            # --- 2. Find Money at the end ---
            money_match = money_pattern_end.search(full_block)
            if not money_match:
                return None # Not a transaction
            
            debit_str = money_match.group(1)
            credit_str = money_match.group(2)
            balance_str = money_match.group(3)
            
            # --- 3. Extract Narration ---
            # Narration is between the date and the money
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            narration = full_block[narration_start_index:narration_end_index].strip()
            
            # The narration block contains the "Type" and "Description"
            # We can split it once by whitespace to get the "Type"
            parts = narration.split(None, 1)
            if len(parts) == 2:
                # Type = parts[0], Narration = parts[1]
                narration = f"{parts[0]} {parts[1]}" # Combine them
            else:
                # Just use the whole block
                narration = narration
                
            return {
                'Date': pd.to_datetime(date_str, format='%d %b %Y', errors='coerce'),
                'Narration': narration,
                'Withdrawal Amt.': debit_str,
                'Deposit Amt.': credit_str,
                'Closing Balance': balance_str
            }
        except Exception as e:
            # print(f"Error in process_block: {e}")
            return None

    # --- Main Loop (State Machine) ---
    current_block_lines = []
    data_started = False

    for line in text.split('\n'):
        line = line.strip()

        # Skip lines until we find the header
        if not data_started:
            if "Date Type Description Debit Credit Balance" in line:
                data_started = True
            continue
        
        # We are after the header now
        if not line or "--- PAGE BREAK ---" in line or "Page " in line or "This is a computer generated statement" in line:
            continue
            
        # Check if this line is the START of a new transaction
        if line_start_pattern.match(line):
            # Process the *previous* block first.
            if current_block_lines:
                parsed_txn = process_block(current_block_lines)
                if parsed_txn:
                    transactions.append(parsed_txn)
            
            # Now, start the new block
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is not a start line, so it's a continuation
            # (narration or amounts). Append it to the current block.
            current_block_lines.append(line)

    # After the loop, process the very last block
    if current_block_lines:
        parsed_txn = process_block(current_block_lines)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of Main Loop ---

    if not transactions:
        return pd.DataFrame()

    df = pd.DataFrame(transactions)
    
    # 4. Clean the money columns
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip().replace('-', '0')
        df[col] = df[col].replace('', '0')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
    df = df.dropna(subset=['Date'])
    return df

# --- (The Smart Router is unchanged) ---
def parse_bank_statement(filename: str, file_content: bytes) -> pd.DataFrame:
    """
    Main router function. Extracts text from PDF bytes and routes to the correct parser.
    
    Args:
        filename (str): The name of the uploaded file.
        file_content (bytes): The raw byte content of the file.

    Returns:
        pd.DataFrame: A DataFrame of parsed transactions.
    """
    
    # 1. Extract text from bytes
    # We pass both filename (for logging) and file_content
    text = extract_text_from_pdf(filename, file_content)
    
    # 2. If text extraction failed (scanned, encrypted, etc.), return empty
    if not text:
        return pd.DataFrame()
    
    # 3. Proceed with your existing, working router logic
    # (This logic below is copied exactly from your provided code)
    print(f"--- Processing: {filename} ---")
    upper_filename = filename.upper()
    upper_text = text[:1500].upper()
    clean_upper_text = re.sub(r'\s+', '', upper_text) # Text without spaces

    # --- Bank Identification Logic (Order is important, most specific first) ---
    if "CENTRAL BANK" in upper_filename or "CENTRALBANKOFINDIA" in clean_upper_text:
        print("Bank identified as: Central Bank of India. Using CBI parser.")
        return parse_central_bank_of_india(text)
    elif "HDFC" in upper_filename:
        print("Bank identified as: HDFC. Using HDFC parser.")
        return parse_hdfc_bank(text)
    elif "AXIS" in upper_filename:
        print("Bank identified as: Axis.")
        if "S.NOTRANSACTION" in clean_upper_text:
            print("Using Axis Format 1 parser.")
            return parse_axis_bank_format1(text)
        elif "TRANDATECHQNO" in clean_upper_text:
            print("Using Axis Format 2 parser.")
            return parse_axis_bank_format2(text)
        else: # Fallback
            df = parse_axis_bank_format1(text)
            if not df.empty: return df
            return parse_axis_bank_format2(text)
    elif "AU" in upper_filename:
        print("Bank identified as: AU Small Finance Bank. Using AU parser.")
        return parse_au_bank(text)
    elif "BANDHAN" in upper_filename:
        print("Bank identified as: Bandhan Bank. Using Bandhan parser.")
        return parse_bandhan_bank(text)
    
    elif "BARODA" in upper_filename:
        # --- REMOVED the "SCANNED" filename check here ---
            
        print("Bank identified as: Bank of Baroda.")
        # Try the original parser first (Format 1)
        print(" -> Trying BoB Format 1...")
        df = parse_bank_of_baroda(text) # Assuming this is your original BoB parser
        if not df.empty:
            print(" -> BoB Format 1 SUCCEEDED.")
            return df
        
        # If Format 1 failed, try the new parser (Format 2)
        print(" -> BoB Format 1 failed, trying Format 2...")
        # Make sure the function name below matches the one you added
        df = parse_bank_of_baroda_format2(text) 
        if not df.empty:
            print(" -> BoB Format 2 SUCCEEDED.")
            return df
            
        # If both failed
        print(" -> Both BoB formats failed.")
        return pd.DataFrame() # Return empty if neither worked
    
    elif ("BANK OF INDIA" in upper_filename and "CENTRAL" not in upper_filename) or "SRNODATEREMARKS" in clean_upper_text:
        print("Bank identified as: Bank of India. Using BoI parser.")
        if "CENTRALBANKOFINDIA" in clean_upper_text:
            print(f"⚠️ False positive for BoI, skipping.")
            return pd.DataFrame()
        return parse_bank_of_india(text)
    elif "PUNJAB & SIND" in upper_filename or "PSIB" in upper_text:
        print("Bank identified as: Punjab & Sind Bank. Using P&S parser.")
        return parse_punjab_sind_bank(text)
    elif "CANARA BANK" in upper_filename or "CNRB" in upper_text:
        print("Bank identified as: Canara Bank. Using Canara parser.")
        return parse_canara_bank(text)
    elif "EQUITAS" in upper_filename or "ESFB" in upper_text:
        print("Bank identified as: Equitas Small Finance Bank. Using Equitas parser.")
        return parse_equitas_bank(text)
    elif "FEDERAL BANK" in upper_filename or "FDRL" in upper_text:
        print("Bank identified as: Federal Bank. Using Federal parser.")
        return parse_federal_bank(text)
    elif "ICICI BANK" in upper_filename or "DATEMODE**PARTICULARS" in clean_upper_text:
        print("Bank identified as: ICICI Bank. Using ICICI parser.")
        return parse_icici_bank(text)
    
   
        
    elif "IDBI BANK FORMAT 2" in upper_filename or ("SRDATEDESCRIPTIONAMOUNT" in clean_upper_text and "IBKL" in upper_text):
        print("Bank identified as: IDBI Bank (Format 2). Using IDBI F2 parser.")
        return parse_idbi_bank_format2(text)
    elif "IDFCFIRST" in upper_filename or ("TRANSACTIONDATEVALUEDATEPARTICULARS" in clean_upper_text and "IDFB" in upper_text):
        print("Bank identified as: IDFC First Bank. Using IDFC parser.")
        return parse_idfc_first_bank(text)
    elif "INDIAN BANK" in upper_filename or "IDIB" in upper_text: # Using filename or IFSC
        print("Bank identified as: Indian Bank. Using Indian Bank parser.")
        return parse_indian_bank(text)
    elif "INDIAN OVERSEAS" in upper_filename or "IOBA" in clean_upper_text:
        print("Bank identified as: Indian Overseas Bank. Using IOB parser.")
        return parse_indian_overseas_bank(text)
    
    # --- ADDED ROUTE FOR SARASWAT BANK ---
    # We'll use a larger text sample for the IFSC check
    elif "SARASWAT" in upper_filename or "SRCB" in text[:2500].upper(): 
        print("Bank identified as: Saraswat Bank. Using Saraswat parser (v7).")
        # NOTE: This assumes you have parse_saraswat_bank defined above
        # We skipped this, so it will fail if not defined.
        # return parse_saraswat_bank(text) 
        pass # Skipping as per our last agreement
    # --- End Saraswat Check ---

        """        elif "INDUSLAND" in upper_filename or "INDB" in clean_upper_text: # Using filename or IFSC
                    print("Bank identified as: IndusInd Bank. Using IndusInd parser.")
                    return parse_indusind_bank(text)"""
        
    elif "INDUSLAND" in upper_filename or "INDB" in clean_upper_text: # Using filename or IFSC
        print("Bank identified as: IndusInd Bank.")

        if "DATE TYPE DESCRIPTION DEBIT CREDIT BALANCE" in re.sub(r'\s+', ' ', upper_text):
            print(" -> Using IndusInd Format 3 (Block-Logic) parser.")
            return parse_indusind_bank_format3(text)

            # Check for Format 2's unique header
        elif "BANKREFERENCE" in clean_upper_text or "PAYMENTNARRATION" in clean_upper_text:
            print(" -> Using IndusInd Format 2 (CSV-style) parser.")

            print(" -> Format 2 parser is known to be failing, skipping.")
            df_f1 = parse_indusind_bank(text) # Try F1 as a fallback
            if not df_f1.empty:
                print(" -> Format 2 failed, but Format 1 worked.")
                return df_f1
            else:
                return pd.DataFrame() # All failed
        else:

            print(" -> Using IndusInd Format 1 (Original) parser.")
            return parse_indusind_bank(text)    
    elif "KOTAK" in upper_filename or "KKBK" in clean_upper_text:
        print("Bank identified as: Kotak Bank. Using Kotak parser (v1).")
        return parse_kotak_bank(text)
    elif "SBI" in upper_filename or "SBIN" in clean_upper_text:
       print("Bank identified as: SBI. Using SBI parser.")
       return parse_sbi_bank(text)
    elif "UCO" in upper_filename or "UCBA" in clean_upper_text:
        print("Bank identified as: UCO Bank. Using UCO parser.")
        return parse_uco_bank(text)
    # --- UNION Check ---
    elif "UNION" in upper_filename or "UBIN" in clean_upper_text:
        print("Bank identified as: UNION Bank. Using UNION parser.")
        return parse_union_bank(text)
    
        # --- YES Bank Check ---
    elif "YES BANK" in upper_filename or "YESB" in clean_upper_text:
        print("Bank identified as: YES Bank. Using YES parser.")
        return parse_yes_bank(text)
    
    elif "DHANLAXMI" in upper_filename or "DLXB" in upper_text:
        print("Bank identified as: Dhanlaxmi Bank. Using Dhanlaxmi parser.")
        return parse_dhanlaxmi_bank(text)
    
    else:
        print(f"⚠️ No specific parser found for this bank. Skipping.")
        return pd.DataFrame()
