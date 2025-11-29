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
    print("--- Starting HDFC Bank (Format 1) Parser ---")
    
    lines = text.split('\n')
    data_started = False
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Find the header
        if not data_started:
            if "Date" in line and "Narration" in line and "Withdrawal" in line:
                print(f"DEBUG: Found header at line {i}")
                data_started = True
                i += 1
                continue
            i += 1
            continue
        
        # Skip empty lines and footers
        if not line or "HDFC BANK LIMITED" in line or "Page No" in line or "Statement of account" in line:
            i += 1
            continue
        
        # Check if line starts with a date
        if re.match(r'^\d{2}/\d{2}/\d{2}\s', line):
            # This is a transaction line
            parsed = parse_hdfc_single_transaction(line)
            if parsed:
                transactions.append(parsed)
        
        i += 1
    
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()
    
    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    return pd.DataFrame(transactions)

def parse_hdfc_single_transaction(line: str):
    """
    Parses a single HDFC transaction line - DEBUG VERSION
    """
    try:
        # Extract the transaction date
        date_match = re.match(r'^(\d{2}/\d{2}/\d{2})', line)
        if not date_match:
            return None
        date_str = date_match.group(1)
        
        # Find all dates in the line
        value_date_pattern = re.compile(r'\b(\d{2}/\d{2}/\d{2})\b')
        all_dates = list(value_date_pattern.finditer(line))
        
        if len(all_dates) < 2:
            return None
        
        # The last date is the value date
        value_date_match = all_dates[-1]
        value_date_end = value_date_match.end()
        
        # Get the section after value date (amounts only)
        amounts_section = line[value_date_end:].strip()
        
        # DEBUG: Print what we're working with
        narration_section = line[len(date_str):value_date_match.start()].strip()
        print(f"\nDEBUG Line: {line[:80]}...")
        print(f"  Narration section: {narration_section[:50]}...")
        print(f"  Amounts section: {amounts_section}")
        
        # Extract numbers from amounts section only
        number_pattern = re.compile(r'(\d{1,3}(?:,\d{3})*\.\d{2})')
        amounts = number_pattern.findall(amounts_section)
        
        print(f"  Found amounts: {amounts}")
        
        if len(amounts) < 2:
            print(f"  SKIP: Not enough amounts")
            return None
        
        # Parse amounts
        if len(amounts) == 3:
            withdrawal = float(amounts[0].replace(',', ''))
            deposit = float(amounts[1].replace(',', ''))
            balance = float(amounts[2].replace(',', ''))
        elif len(amounts) == 2:
            amount = float(amounts[0].replace(',', ''))
            balance = float(amounts[1].replace(',', ''))
            
            # Check narration for credit keywords
            if any(kw in narration_section.upper() for kw in ['CR', 'CREDIT', 'DEPOSIT', 'TRANSFER CR']):
                withdrawal = 0.0
                deposit = amount
            else:
                withdrawal = amount
                deposit = 0.0
        else:
            # More than 3 - take last 3
            withdrawal = float(amounts[-3].replace(',', ''))
            deposit = float(amounts[-2].replace(',', ''))
            balance = float(amounts[-1].replace(',', ''))
        
        # Clean narration
        narration_text = re.sub(r'\b(MB\w+|0{4}\d{12,16})\b', '', narration_section)
        narration_text = re.sub(r'\s+', ' ', narration_text).strip()
        
        print(f"  RESULT: W={withdrawal}, D={deposit}, B={balance}")
        
        return {
            'Date': pd.to_datetime(date_str, format='%d/%m/%y', errors='coerce'),
            'Narration': narration_text,
            'Withdrawal Amt.': withdrawal,
            'Deposit Amt.': deposit,
            'Closing Balance': balance
        }
    
    except Exception as e:
        print(f"DEBUG: Error: {e}")
        return None

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
# --- (REVISED Parser: Indian Bank - v6) ---
def parse_indian_bank_v6(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting Indian Bank (v6) Parser ---")

    # Looks for (a number ending in .XX) followed by (a balance string)
    merge_fix_pattern = re.compile(r'([\d,.-]+\.\d{2})([\d,.]+(?:Cr|Dr))')
    
    # --- THIS IS THE FIX ---
    # G3 (Narration) is now (.*?) - NON-GREEDY
    txn_pattern = re.compile(
        r"^(\d{2}/\d{2}/\d{2})" +             # G1: Post Date
        r"(\d{2}/\d{2}/\d{2})" +             # G2: Value Date
        r"\s+(.*?)\s+" +                     # G3: Narration (non-greedy)
        r"([\d,.-]+)\s*" +                   # G4: Amount
        r"([\d,.]+(?:Cr|Dr))$"               # G5: Balance
    )
    # --- END OF FIX ---
    
    # This is the pattern that marks the START of a new line
    date_start_pattern = re.compile(r"^\d{2}/\d{2}/\d{2}\d{2}/\d{2}/\d{2}")

    # Find Opening Balance
    last_balance = None
    ob_match = re.search(r"Brought Forward\s+([\d,.]+(?:Cr|Dr))", text, re.IGNORECASE)
    if ob_match:
        try:
            bal_str = ob_match.group(1).replace(',', '').replace('Cr','').replace('Dr','')
            last_balance = float(bal_str)
            print(f"Found Opening Balance: {last_balance}")
        except Exception:
            pass 

    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        # Fix merged numbers
        full_block = merge_fix_pattern.sub(r'\1 \2', full_block)
        
        match = txn_pattern.search(full_block)
        if not match:
            # print(f"Block skipped (no match): {full_block[:50]}...") # Debug
            return None, prev_balance
            
        try:
            date_str = match.group(2) # Value Date
            narration = match.group(3).strip()
            amount_str = match.group(4)
            balance_str = match.group(5)
            
            balance = float(balance_str.replace('Cr','').replace('Dr','').replace(',',''))
            amount = float(amount_str.replace(',',''))
            
            withdrawal, deposit = 0.0, 0.0

            if prev_balance is not None:
                if balance > prev_balance + 0.001:
                    deposit = amount
                elif balance < prev_balance - 0.001:
                    withdrawal = amount
            else:
                if "Cr" in balance_str: deposit = amount
                else: withdrawal = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d/%m/%y', errors='coerce'),
                'Narration': narration,
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:50]}...") # Debug
            return None, prev_balance

    # --- State Machine (Unchanged from v4/v5) ---
    current_block_lines = []
    data_started = False

    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if "Post DateValue" in line: # This is the correct header fix
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or "Brought Forward" in line or "Date Details Chq.No." in line:
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance 
            
            current_block_lines = [line]
        
        elif current_block_lines:
            current_block_lines.append(line)
    
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
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
def parse_dhanlaxmi_bank_v2(text: str) -> pd.DataFrame:
    transactions = []
    
    line_start_pattern = re.compile(r"^(\d{2}-\w{3}-\d{4})")
    money_pattern_end = re.compile(r"([\d,.-]+)\s+([\d,.-]+)\s+([\d,.]+)$")

    print("--- Starting Dhanlaxmi Bank (v2) Parser ---")

    def process_block(block_lines):
        try:
            if not block_lines:
                return None
            
            full_block = " ".join(block_lines).replace('\n', ' ').strip()
            full_block = re.sub(r'\s+', ' ', full_block)
            
            date_match = line_start_pattern.match(full_block)
            if not date_match:
                return None
            date_str = date_match.group(1)
            
            money_match = money_pattern_end.search(full_block)
            if not money_match:
                return None
            
            debit_str = money_match.group(1)
            credit_str = money_match.group(2)
            balance_str = money_match.group(3)

            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                return None 

            narration_block = full_block[narration_start_index:narration_end_index].strip()
            narration_parts = narration_block.split()
            
            if len(narration_parts) < 2: 
                return None
            
            narration_and_cheque_block = " ".join(narration_parts[1:])
            cheque_match = re.search(r"\s+([.\d]+)$", narration_and_cheque_block)
            narration = ""
            
            if cheque_match and len(narration_and_cheque_block) > len(cheque_match.group(0)) + 2:
                narration = narration_and_cheque_block[:cheque_match.start()].strip()
            else:
                narration = narration_and_cheque_block.strip()
            
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

    current_block_lines = []
    data_started = False

    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if "DATE VALUE DATE DESCRIPTION" in line:
                data_started = True
            continue
        
        if not line or "--- PAGE BREAK ---" in line or "Page No:" in line or "STATEMENT OF ACCOUNT" in line:
            continue
            
        if line_start_pattern.match(line):
            if current_block_lines:
                parsed_txn = process_block(current_block_lines)
                if parsed_txn:
                    transactions.append(parsed_txn)
            
            current_block_lines = [line]
        
        elif current_block_lines:
            current_block_lines.append(line)

    if current_block_lines:
        parsed_txn = process_block(current_block_lines)
        if parsed_txn:
            transactions.append(parsed_txn)

    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    
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

# --- (NEW Parser: ICICI Bank - Format 2 - Money-Ending Logic) ---
# --- (NEW Parser: ICICI Bank - Format 2 - Money-Ending Logic) ---
def parse_icici_bank_format2(text: str) -> pd.DataFrame:
    transactions = []
    
    # This pattern finds the three money amounts at the END of a line
    money_pattern_end = re.compile(r"([\d,.-]+)\s+([\d,.-]+)\s+([\d,.]+)$")
    
    # This pattern finds the first date in a block
    date_pattern = re.compile(r"(\d{2}/\d{2}/\d{4})")

    # This helper function processes a single transaction block
    def process_block(block_lines):
        try:
            if not block_lines:
                return None
            
            # Join all lines in the block into one string
            full_block = " ".join(block_lines).replace('\n', ' ').strip()
            
            # --- 1. Find Money at the end (we know it's here) ---
            money_match = money_pattern_end.search(full_block)
            if not money_match:
                return None
            
            debit_str = money_match.group(1)
            credit_str = money_match.group(2)
            balance_str = money_match.group(3)
            
            # --- 2. Find the FIRST date in the block ---
            date_match = date_pattern.search(full_block)
            if not date_match:
                return None # Not a transaction
            
            # We use the first date found (which is the Value Date)
            date_str = date_match.group(1) 
            
            # --- 3. Extract Narration ---
            # Narration is everything between the start and the money
            narration_end_index = money_match.start()
            narration_block = full_block[:narration_end_index].strip()
            
            # Remove the S.No, Value Date, and Txn Date from the start
            # e.g., "5 03/04/2024 03/04/2024"
            narration = re.sub(r"^\s*\d+\s+\d{2}/\d{2}/\d{4}\s+\d{2}/\d{2}/\d{4}\s*", "", narration_block).strip()
            # Also remove just the S.No if it's on its own line
            narration = re.sub(r"^\s*\d+\s+", "", narration).strip()
                
            return {
                'Date': pd.to_datetime(date_str, format='%d/%m/%Y', errors='coerce'),
                'Narration': narration,
                'Withdrawal Amt.': debit_str,
                'Deposit Amt.': credit_str,
                'Closing Balance': balance_str
            }
        except Exception as e:
            # print(f"Error in process_block: {e} | Block: {full_block}")
            return None

    # --- Main Loop (State Machine) ---
    current_block_lines = []
    data_started = False

    for line in text.split('\n'):
        line = line.strip()

        # Skip lines until we find the header
        if not data_started:
            # The header is "S No. Value Date Transaction Date..."
            # We must use re.search because of extra spaces
            if re.search(r"S\s+No\.\s+Value\s+Date\s+Transaction\s+Date", line):
                data_started = True
            continue
        
        # We are after the header now
        if not line or "--- PAGE BREAK ---" in line or "Transactions List - " in line or "DETAILED STATEMENT" in line:
            continue
            
        # Add the line to our buffer
        current_block_lines.append(line)
        
        # Check if this line is the END of a transaction
        if money_pattern_end.search(line):
            # We found the money, so this block is complete
            parsed_txn = process_block(current_block_lines)
            if parsed_txn:
                transactions.append(parsed_txn)
            
            # Clear the buffer for the next transaction
            current_block_lines = []

    # After the loop, if there's anything left in the buffer,
    # it's likely an incomplete fragment, so we ignore it.
    
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


# --- (NEW Parser: IndusInd Bank - Format 2 - v5 State Machine Logic) ---
def parse_indusind_bank_format2(text: str) -> pd.DataFrame:
    transactions = []
    
    # --- New Patterns ---
    # Main line: Ref No, Date, Value Date/Time, Type, Narration Start
    main_line_pattern = re.compile(
        r"^(S\d+|'\d+)\s+(\d{2}\s\w{3}\s\d{4})\s+'(\d{2}-\w{3}-\d{2}\s\d{2}:\d{2}:\d{2})\s+(Debit|Credit)\s+(.*)$"
    )
    # Money line: Amount, Balance (and nothing else)
    money_line_pattern = re.compile(r"^([\d,.-]+)\s+([\d,.]+)$")
    # --- End New Patterns ---
    
    print("--- Starting IndusInd Bank (Format 2) v5 Parser ---")

    current_date = None
    current_type = None
    narration_buffer = []
    
    lines = text.split('\n')
    
    for line in lines:
        line = line.strip()

        # Skip empty lines, page breaks, and page numbers
        if not line or \
           line.startswith("--- PAGE BREAK ---") or \
           line.startswith("Account Statement Customer Name") or \
           re.fullmatch(r"\d+", line):
            continue
            
        main_match = main_line_pattern.match(line)
        money_match = money_line_pattern.match(line)
        
        if main_match:
            # --- Found a Main Transaction Line ---
            if narration_buffer and current_date: # Check if we have a pending transaction
                try:
                    last_line = narration_buffer.pop()
                    money_match_prev = money_line_pattern.match(last_line)
                    
                    if money_match_prev:
                        amount_str = money_match_prev.group(1).strip()
                        balance_str = money_match_prev.group(2).strip()
                        
                        amount = float(amount_str.replace(',', ''))
                        debit_amt = amount if current_type == 'DEBIT' else 0.0
                        credit_amt = amount if current_type == 'CREDIT' else 0.0
                        
                        full_narration = " ".join(narration_buffer)
                        full_narration = re.sub(r'\s+', ' ', full_narration).strip()
                        
                        transactions.append({
                            'Date': pd.to_datetime(current_date, format='%d %b %Y', errors='coerce'),
                            'Narration': full_narration,
                            'Withdrawal Amt.': debit_amt,
                            'Deposit Amt.': credit_amt,
                            'Closing Balance': balance_str
                        })
                    
                except Exception as e:
                    pass # Discard this block

            # --- Now, start the NEW transaction ---
            current_date = main_match.group(2)
            current_type = main_match.group(4).strip().upper()
            narration_buffer = [main_match.group(5).strip()] # Add first part of narration
            
        elif money_match and not current_date:
            narration_buffer.append(line)
            
        elif current_date:
            narration_buffer.append(line)
        
        else:
            pass

    # --- Process the last transaction after the loop ---
    if narration_buffer and current_date:
        try:
            last_line = narration_buffer.pop()
            money_match_prev = money_line_pattern.match(last_line)
            
            if money_match_prev:
                amount_str = money_match_prev.group(1).strip()
                balance_str = money_match_prev.group(2).strip()
                
                amount = float(amount_str.replace(',', ''))
                debit_amt = amount if current_type == 'DEBIT' else 0.0
                credit_amt = amount if current_type == 'CREDIT' else 0.0
                
                full_narration = " ".join(narration_buffer)
                full_narration = re.sub(r'\s+', ' ', full_narration).strip()
                
                transactions.append({
                    'Date': pd.to_datetime(current_date, format='%d %b %Y', errors='coerce'),
                    'Narration': full_narration,
                    'Withdrawal Amt.': debit_amt,
                    'Deposit Amt.': credit_amt,
                    'Closing Balance': balance_str
                })
        except Exception as e:
            pass

    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    
    # Clean the money columns
    money_cols = ['Withdrawal Amt.', 'Deposit Amt.', 'Closing Balance']
    for col in money_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip().replace('-', '0')
        df[col] = df[col].replace('', '0')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
    df = df.dropna(subset=['Date'])
    
    # Reverse the DataFrame so transactions are in chronological order
    df = df.iloc[::-1].reset_index(drop=True)
    return df

def parse_saraswat_bank_v6(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting Saraswat Bank (v6) Parser ---")

    # --- THIS IS THE FIX ---
    # G1: Amount (e.g., "- 850.00", "6,000.00", or "-")
    # G2: Balance
    money_pattern_end = re.compile(r"((?:-?\s*[\d,.]+)|-)\s+([\d,.]+)$")
    # --- END OF FIX ---
    
    date_start_pattern = re.compile(r"^(\d{2}\s\w{3}\s\d{4})")

    last_balance = None
    
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) 
        
        date_match = date_start_pattern.match(full_block)
        if not date_match:
            return None, prev_balance
            
        money_match = money_pattern_end.search(full_block)
        if not money_match:
            return None, prev_balance
            
        try:
            date_str = date_match.group(1)
            
            amount_str = money_match.group(1).strip()
            balance_str = money_match.group(2).strip()
            
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                return None, prev_balance

            narration = full_block[narration_start_index:narration_end_index].strip()
            
            # --- Balance Logic ---
            # Clean the amount string
            amount_str_cleaned = amount_str.replace(',', '').replace(' ', '').replace('-', '')
            if not amount_str_cleaned: 
                amount = 0.0
            else:
                amount = float(amount_str_cleaned)
                
            balance = float(balance_str.replace(',', ''))
            
            withdrawal, deposit = 0.0, 0.0
            
            if prev_balance is not None:
                if balance > prev_balance + 0.001:
                    deposit = amount
                elif balance < prev_balance - 0.001:
                    withdrawal = amount
            else:
                if amount > 0:
                    deposit = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d %b %Y', errors='coerce'),
                'Narration': narration,
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:50]}...") # Debug
            return None, prev_balance

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"Debit\s+Credit\s+Balance")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or header_pattern.search(line) or "Page " in line or "Generated on :" in line:
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance 
            
            current_block_lines = [line]
        
        elif current_block_lines:
            current_block_lines.append(line)
    
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    
    df['Closing Balance'] = pd.to_numeric(df['Closing Balance'], errors='coerce').fillna(0)
        
    df = df.dropna(subset=['Date'])
    return df

def parse_idbi_bank_v4(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting IDBI Bank (v4) Parser ---")

    # --- THIS IS THE FIX ---
    # G4 (Amount) now requires exactly 2 decimal places
    txn_pattern = re.compile(
        r"^(\d{2}/\d{2}/\d{4})" +      # G1: Txn Date
        r"\s+(.*?)\s+" +              # G2: Narration
        r"(Cr|Dr)\." +                # G3: Type
        r"\s+INR\s+([\d,]+\.\d{2})" + # G4: Amount (e.g., 1,234.56)
        r".*?" +                      # Skip the junk in the middle
        r"([\d,.]+)$"                 # G5: Balance (at end of line)
    )
    # --- END OF FIX ---
    
    # --- State Machine ---
    data_started = False
    
    header_pattern = re.compile(r"Balance\s+\(INR\)Amount\s+\(INR\)")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            clean_line = re.sub(r'\s+', ' ', line)
            if header_pattern.search(clean_line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line:
            continue
            
        match = txn_pattern.search(line)
        if not match:
            continue
            
        try:
            date_str = match.group(1)
            narration = match.group(2).strip()
            type_str = match.group(3)
            amount_str = match.group(4)
            balance_str = match.group(5)
            
            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str.replace(',', ''))
            
            withdrawal, deposit = 0.0, 0.0
            
            if type_str == 'Dr':
                withdrawal = amount
            else:
                deposit = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d/%m/%Y', errors='coerce'),
                'Narration': narration,
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            transactions.append(txn_data)
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {line[:50]}...") # Debug
            continue
    
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
        
    df = df.dropna(subset=['Date'])
    # The transactions are in reverse-chronological order, so we reverse them
    df = df.iloc[::-1].reset_index(drop=True)
    return df

def parse_punjab_national_bank_v1(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting Punjab National Bank (v1) Parser ---")

    # This regex is the key:
    # G1: Date (dd/mm/yyyy)
    # G2: Amount (the first number, 3,500.00 or 18,906.08)
    # G3: Balance (the number ending in Cr.)
    # G4: Narration (everything after)
    txn_pattern = re.compile(
        r"^(\d{2}/\d{2}/\d{4})" +      # G1: Date
        r"\s+([\d,.-]+)" +            # G2: Amount
        r"\s+([\d,.]+\s+Cr\.)" +      # G3: Balance
        r"\s+(.*)"                    # G4: Narration
    )
    
    # This is the pattern that marks the START of a new line
    date_start_pattern = re.compile(r"^(\d{2}/\d{2}/\d{4})")
    
    # We will track the balance.
    last_balance = None
    
    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        match = txn_pattern.search(full_block)
        if not match:
            # print(f"Block skipped (no match): {full_block[:50]}...") # Debug
            return None, prev_balance
            
        try:
            date_str = match.group(1)
            amount_str = match.group(2)
            balance_str = match.group(3)
            narration = match.group(4).strip()
            
            balance = float(balance_str.replace('Cr.','').replace(',','').strip())
            amount = float(amount_str.replace(',',''))
            
            withdrawal, deposit = 0.0, 0.0

            # Since the file is reverse-chronological, the logic is flipped
            if prev_balance is not None:
                if balance > prev_balance + 0.001:
                    deposit = amount
                elif balance < prev_balance - 0.001:
                    withdrawal = amount
            else:
                # First transaction (at top of file)
                # We have to guess or wait for the next line
                # Let's use the narration
                if "UPI/DR" in narration or "WITHDRAWAL" in narration.upper():
                    withdrawal = amount
                else:
                    deposit = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d/%m/%Y', errors='coerce'),
                'Narration': narration,
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:50]}...") # Debug
            return None, prev_balance

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    # Header: Look for the unique column order
    header_pattern = re.compile(r"Withdrawal\s+Deposit\s+Balance\s+Narration")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or "Page No" in line:
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance # Update balance
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a narration spill-over
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
        
    df = df.dropna(subset=['Date'])
    
    # --- IMPORTANT: Reverse the DataFrame to be in chronological order ---
    df = df.iloc[::-1].reset_index(drop=True)
    
    return df

# --- (NEW Parser: AU Small Finance Bank - v9) ---
def parse_au_bank_format3(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting AU Bank (Format 9) Parser ---")

    # G1: Txn Date, G2: Value Date, G3: Narration, G4: (Optional) Chq/Ref
    # G5: Type, G6: Amount, G7: Balance
    txn_pattern = re.compile(
        r"(\d{2}-\w{3}-\d{4})" +      # G1: Txn Date
        r"\s+(\d{2}-\w{3}-\d{4})" +   # G2: Value Date
        r"\s+(.*?)\s+" +              # G3: Narration
        r"([A-Z0-9-]{10,}\s+)?" +     # G4: Optional Cheq/Ref.No.
        r"(C|D)\s+Rs\.\s+" +          # G5: Type
        r"([\d,.]+)\s+" +             # G6: Amount
        r"Rs\.\s+([\d,.]+)$"          # G7: Balance
    )
    
    # Simpler pattern for lines without a Cheq/Ref.No. (like interest)
    txn_pattern_simple = re.compile(
        r"(\d{2}-\w{3}-\d{4})" +      # G1: Txn Date
        r"\s+(\d{2}-\w{3}-\d{4})" +   # G2: Value Date
        r"\s+(.*?)\s+" +              # G3: Narration (non-greedy)
        r"(C|D)\s+Rs\.\s+" +          # G4: Type
        r"([\d,.]+)\s+" +             # G5: Amount
        r"Rs\.\s+([\d,.]+)$"          # G6: Balance
    )
    
    # --- THIS IS THE FIX ---
    # It now matches "01-May-", "01May--", AND "01-Jul-2024"
    date_start_pattern = re.compile(r"^(\d{2}-\w{3}-\d{4}|\d{2}-\w{3}-|\d{2}\w{3}--)")
    
    # --- Helper function to process a finished block ---
    def process_block(block_lines):
        if not block_lines:
            return None
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        
        # --- NEW ROBUST DATE FIXES ---
        # Fixes: "01-Jun- 2024" -> "01-Jun-2024"
        full_block = re.sub(r'(\d{2}-\w{3})- (\d{4})', r'\1-\2', full_block)
        # Fixes: "01May-- 2024" -> "01-May-2024"
        full_block = re.sub(r'(\d{2}\w{3})-- (\d{4})', r'\1-\2', full_block)
        # Fixes: "30Apr-- 2024" -> "30-Apr-2024" (Handles the value date)
        full_block = re.sub(r'(\d{2}\w{3})--\s', r'\1- ', full_block)
        
        full_block = re.sub(r'\s+', ' ', full_block) 
        
        match = txn_pattern.search(full_block)
        narration = ""
        if not match:
            match = txn_pattern_simple.search(full_block)
            if not match:
                # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
                return None
            date_str, _, narration_raw, type_str, amount_str, balance_str = match.groups()
            narration = narration_raw.strip()
        else:
            date_str, _, narration_raw, cheq_no, type_str, amount_str, balance_str = match.groups()
            narration = f"{narration_raw.strip()} {cheq_no.strip() if cheq_no else ''}".strip()

        try:
            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str.replace(',', ''))
            
            withdrawal, deposit = 0.0, 0.0
            
            if type_str == 'D':
                withdrawal = amount
            else:
                deposit = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d-%b-%Y', errors='coerce'),
                'Narration': narration,
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            return txn_data
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None

    # --- State Machine (v8 logic, which is correct) ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"Date\s+Description\s+Chq\./Ref\.No\.")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or "Account Mini Statement" in line or "Txn Date Value" in line:
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn = process_block(current_block_lines)
                if parsed_txn:
                    transactions.append(parsed_txn)
            
            current_block_lines = [line]
        
        elif current_block_lines:
            current_block_lines.append(line)
    
    if current_block_lines:
        parsed_txn = process_block(current_block_lines)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df

def parse_bank_of_baroda_format4(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting Bank of Baroda (Format 3) Parser ---")

    # This pattern finds the *start* of a new transaction line
    date_start_pattern = re.compile(r"^(\d{2}-\d{2}-\d{4})")
    
    # This pattern finds the LAST TWO numbers on the line
    # G1: Amount (can be debit or credit), G2: Balance (with " Cr")
    money_pattern_end = re.compile(r"([\d,.]+)\s+([\d,.]+\s+Cr)$")

    # Find Opening Balance
    last_balance = None
    ob_match = re.search(r"Opening\s+Balance\s+:\s+([\d,.]+)(Cr|Dr)", text, re.IGNORECASE)
    if ob_match:
        try:
            bal_str = ob_match.group(1).replace(',', '')
            last_balance = float(bal_str)
            print(f"Found Opening Balance: {last_balance}")
        except Exception:
            pass 

    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        date_match = date_start_pattern.match(full_block)
        if not date_match:
            return None, prev_balance
            
        money_match = money_pattern_end.search(full_block)
        if not money_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None, prev_balance
            
        try:
            date_str = date_match.group(1)
            
            amount_str = money_match.group(1).strip()
            balance_str = money_match.group(2).strip().replace(' Cr', '')
            
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                return None, prev_balance

            narration = full_block[narration_start_index:narration_end_index].strip()
            
            # --- Balance Logic ---
            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str.replace(',', ''))
            
            withdrawal, deposit = 0.0, 0.0
            
            if prev_balance is not None:
                if balance > prev_balance + 0.001:
                    deposit = amount
                elif balance < prev_balance - 0.001:
                    withdrawal = amount
            else:
                # First transaction, assume it's a deposit if balance > 0
                if balance > 0:
                    deposit = amount
                else:
                    withdrawal = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d-%m-%Y', errors='coerce'),
                'Narration': narration,
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None, prev_balance

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    # Header: Look for the unique column order
    header_pattern = re.compile(r"WITHDRAWAL\s+\(DR\)\s+DEPOSIT\s+\(CR\)\s+BALANCE")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        # We are after the header
        if not line or "--- PAGE BREAK ---" in line or "Opening Balance" in line:
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance # Update balance
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df

def parse_canara_bank_format2(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting Canara Bank (v3) Parser ---")

    # --- THIS IS THE FIX (Added \s*) ---
    # G1: Date, G2: Time
    date_start_pattern = re.compile(r"^(\d{2}-\d{2}-\d{4})\s*(\d{2}:\d{2}:\d{2})")
    
    # This pattern finds the *end* of a transaction line
    # G1: Branch Code, G2: Amount, G3: Balance
    end_pattern = re.compile(r"(\d{2})\s+([\d,.]+)\s+([\d,.]+)$")

    # Find Opening Balance
    last_balance = None
    ob_match = re.search(r"Opening\s+Balance\s+Rs\.\s+([\d,.]+)", text, re.IGNORECASE)
    if ob_match:
        try:
            bal_str = ob_match.group(1).replace(',', '')
            last_balance = float(bal_str)
            print(f"Found Opening Balance: {last_balance}")
        except Exception:
            pass 

    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        date_match = date_start_pattern.search(full_block)
        money_match = end_pattern.search(full_block)
        
        if not date_match or not money_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None, prev_balance
            
        try:
            date_str = date_match.group(1)
            amount_str = money_match.group(2)
            balance_str = money_match.group(3)
            
            # --- NEW NARRATION LOGIC ---
            # Find the value date (e.g., "02 Apr 2024")
            value_date_match = re.search(r"(\d{2}\s+\w{3}\s+\d{4})", full_block)
            if not value_date_match:
                return None, prev_balance # Can't find value date

            # Narration is everything *after* the value date and *before* the money
            narration_start_index = value_date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                return None, prev_balance

            narration = full_block[narration_start_index:narration_end_index].strip()
            
            # Clean junk from narration (AXL IDs, etc.)
            narration = re.sub(r"//\s+AXL\w+.*$", "", narration)
            narration = re.sub(r"\s+\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}$", "", narration)
            # --- END NEW NARRATION LOGIC ---
            
            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str.replace(',', ''))
            
            withdrawal, deposit = 0.0, 0.0
            
            if prev_balance is not None:
                if balance > prev_balance + 0.001:
                    deposit = amount
                elif balance < prev_balance - 0.001:
                    withdrawal = amount
            else:
                if "UPI/CR" in narration:
                    deposit = amount
                else:
                    withdrawal = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d-%m-%Y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None, prev_balance

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"Debit\s+Credit\s+Balance")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or "Opening Balance" in line or "Txn Date Value Date" in line:
            continue
            
        if date_start_pattern.match(line):
            # This is the start of a new block
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
            
            # Check if this line is the *end* of the block
            if end_pattern.match(line):
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance # Update balance
                
                # Clear the block
                current_block_lines = []
    
    # Process any remaining lines
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df

def parse_central_bank_of_india_format2(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting Central Bank of India (v2) Parser ---")

    # This pattern finds the *start* of a new transaction line
    date_start_pattern = re.compile(r"^(\d{2}/\d{2}/\d{4})")
    
    # This pattern finds the LAST TWO numbers on the line
    # G1: Amount (debit or credit), G2: Balance (with " CR")
    money_pattern_end = re.compile(r"([\d,.]+)\s+([\d,.]+\s+CR)$")

    # This format doesn't have an "Opening Balance" line, so we start at None
    last_balance = None
    
    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        date_match = date_start_pattern.match(full_block)
        money_match = money_pattern_end.search(full_block)
        
        if not date_match or not money_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None, prev_balance
            
        try:
            date_str = date_match.group(1)
            amount_str = money_match.group(1)
            balance_str = money_match.group(2).replace(' CR', '')
            
            # Narration is between the start and the money
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                return None, prev_balance

            # Extract narration, which includes Value Date and Branch Code
            narration_block = full_block[narration_start_index:narration_end_index].strip()
            
            # Clean the narration by removing the Value Date, Branch Code, and Cheque No.
            # e.g., "16/04/2024 1657 NEFT MOTILAL OSWAL..."
            # e.g., "13/06/2024 1657 325740 Paid to SELF"
            parts = narration_block.split()
            narration = ""
            if len(parts) > 2:
                # Check if parts[0] is a date, parts[1] is a code
                if re.match(r"\d{2}/\d{2}/\d{4}", parts[0]) and parts[1].isdigit():
                    # Check if parts[2] is a cheque number
                    if len(parts) > 3 and parts[2].isdigit() and len(parts[2]) > 4:
                        narration = " ".join(parts[3:]) # Has cheque number
                    else:
                        narration = " ".join(parts[2:]) # No cheque number
                else:
                    narration = narration_block # Fallback
            else:
                narration = narration_block
            
            # --- Balance Logic ---
            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str.replace(',', ''))
            
            withdrawal, deposit = 0.0, 0.0
            
            if prev_balance is not None:
                if balance > prev_balance + 0.001:
                    deposit = amount
                elif balance < prev_balance - 0.001:
                    withdrawal = amount
            else:
                # First transaction fallback
                if "NEFT" in narration or "CREDIT" in narration:
                    deposit = amount
                else:
                    withdrawal = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d/%m/%Y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None, prev_balance

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"Debit\s+Credit\s+Balance")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or "Page Total Credit" in line or "Order by GL. Date" in line:
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance # Update balance
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df

def parse_central_bank_of_india_format3(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting Central Bank of India (v3) Parser ---")

    # This pattern finds the *start* of a new transaction line
    date_start_pattern = re.compile(r"^(\d{2}-\d{2}-\d{4})")
    
    # This pattern finds the LAST TWO numbers on the line
    # G1: Amount (debit or credit), G2: Balance
    money_pattern_end = re.compile(r"([\d,.]+)\s+([\d,.]+)$")

    # Find Opening Balance
    last_balance = None
    ob_match = re.search(r"Opening\s+Balance\s+([\d,.]+)", text, re.IGNORECASE)
    if ob_match:
        try:
            bal_str = ob_match.group(1).replace(',', '')
            last_balance = float(bal_str)
            print(f"Found Opening Balance: {last_balance}")
        except Exception:
            pass 
    
    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        date_match = date_start_pattern.match(full_block)
        money_match = money_pattern_end.search(full_block)
        
        if not date_match or not money_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None, prev_balance
            
        try:
            date_str = date_match.group(1)
            amount_str = money_match.group(1)
            balance_str = money_match.group(2)
            
            # Narration is between the date and the money
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                return None, prev_balance

            narration = full_block[narration_start_index:narration_end_index].strip()
            
            # --- Balance Logic ---
            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str.replace(',', ''))
            
            withdrawal, deposit = 0.0, 0.0
            
            if prev_balance is not None:
                if balance > prev_balance + 0.001:
                    deposit = amount
                elif balance < prev_balance - 0.001:
                    withdrawal = amount
            else:
                # Fallback guess
                if "NEFT" in narration or "CREDIT" in narration:
                    deposit = amount
                else:
                    withdrawal = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d-%m-%Y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None, prev_balance

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"Date\s+Particulars\s+Withdrawals\s+Deposits\s+Balance")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or "Opening Balance" in line:
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance # Update balance
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df

def parse_hdfc_bank_format2(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting HDFC Bank (Format 2) Parser [FIXED] ---")

    # This pattern finds the *start* of a new transaction line
    date_start_pattern = re.compile(r"^(\d{2}/\d{2}/\d{2})")
    
    # --- THIS IS THE FIX ---
    # We remove the '$' (end of line) anchor so the regex can
    # find the money block even if text comes after it.
    money_pattern_search = re.compile(
        r"(\d{2}/\d{2}/\d{2})\s+([\d,.]+)\s+(?:([\d,.]+)\s+)?([\d,.]+)"
    )
    # --- END OF FIX ---

    last_balance = None
    
    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        date_match = date_start_pattern.match(full_block)
        # We must use .search() to find the money block *anywhere*
        money_match = money_pattern_search.search(full_block)
        
        if not date_match or not money_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None, prev_balance
            
        try:
            date_str = date_match.group(1)
            
            groups = money_match.groups()
            
            # --- UPDATED NARRATION/MONEY LOGIC ---
            if groups[2] is None:
                # Format: ValDt Amount Balance
                amount_str = groups[1]
                balance_str = groups[3]
            else:
                # Format: ValDt Withdrawal Deposit Balance
                w_amt = float(groups[1].replace(',', '')) if groups[1] else 0.0
                d_amt = float(groups[2].replace(',', '')) if groups[2] else 0.0
                amount_str = str(w_amt or d_amt)
                balance_str = groups[3]
            
            # 1. Narration is between the start date and the value date
            narration_start_index = date_match.end()
            narration_end_index = money_match.start(1) # Start of Value Dt
            narration_part1 = full_block[narration_start_index:narration_end_index].strip()
            
            # 2. Extra narration (spill-over) is *after* the money block
            extra_narration_start = money_match.end(4) # End of the balance
            narration_part2 = full_block[extra_narration_start:].strip()

            narration = f"{narration_part1} {narration_part2}".strip()
            narration = re.sub(r'\s+', ' ', narration) # Clean up spaces
            # --- END UPDATED LOGIC ---
            
            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str.replace(',', ''))
            
            withdrawal, deposit = 0.0, 0.0
            
            if prev_balance is not None:
                if balance > prev_balance + 0.001:
                    deposit = amount
                elif balance < prev_balance - 0.001:
                    withdrawal = amount
            else:
                if "CR" in narration.upper() or "PAYOUT" in narration.upper() or "NEFT CR" in narration.upper():
                    deposit = amount
                else:
                    withdrawal = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d/%m/%y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None, prev_balance

    # --- State Machine (Unchanged) ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"Date\s+Narration\s+Chq\./Ref\.No\.")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or line.startswith("********"):
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance
            
            current_block_lines = [line]
        
        elif current_block_lines:
            current_block_lines.append(line)
    
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df

def parse_icici_bank_format3(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting ICICI Bank (Format 3) Parser ---")

    # This pattern finds the *start* of a new transaction line
    date_start_pattern = re.compile(r"^(\d{2}-\d{2}-\d{4})")
    
    # This pattern finds the *end* of a transaction line
    # G1: Amount, G2: Type (CR/DR)
    end_pattern = re.compile(r"([\d,.]+)\s+(CR|DR)$")

    # --- Helper function to process a finished block ---
    def process_block(block_lines):
        if not block_lines:
            return None
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        date_match = date_start_pattern.match(full_block)
        money_match = end_pattern.search(full_block)
        
        if not date_match or not money_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None
            
        try:
            date_str = date_match.group(1)
            amount_str = money_match.group(1)
            type_str = money_match.group(2)
            
            # Narration is between the date and the money
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                return None

            narration = full_block[narration_start_index:narration_end_index].strip()
            
            amount = float(amount_str.replace(',', ''))
            
            withdrawal, deposit = 0.0, 0.0
            
            if type_str == 'DR':
                withdrawal = amount
            else:
                deposit = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d-%m-%Y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': 0.0 # No balance column in this format
            }
            
            return txn_data
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"Date\s+Description\s+Amount\s+Type")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or line.startswith("This is a system-generated"):
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn = process_block(current_block_lines)
                if parsed_txn:
                    transactions.append(parsed_txn)
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn = process_block(current_block_lines)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    
    # File is in reverse-chronological order, so we reverse it
    df = df.iloc[::-1].reset_index(drop=True)
    
    return df

def parse_idbi_bank_format3(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting IDBI Bank (Format 5) Parser ---")

    # This pattern finds the *start* of a new transaction line
    # G1: S.No, G2: Txn Date
    start_pattern = re.compile(r"^(\d+)\s+(\d{2}/\d{2}/\d{4})")
    
    # This pattern finds the LAST TWO numbers on the line
    # G1: Amount (debit or credit), G2: Balance
    money_pattern_end = re.compile(r"([\d,.]+)\s+([\d,.]+)$")

    last_balance = None
    
    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        start_match = start_pattern.match(full_block)
        money_match = money_pattern_end.search(full_block)
        
        if not start_match or not money_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None, prev_balance
            
        try:
            date_str = start_match.group(2)
            amount_str = money_match.group(1)
            balance_str = money_match.group(2)
            
            narration_start_index = start_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                return None, prev_balance

            narration_block = full_block[narration_start_index:narration_end_index].strip()
            narration = re.sub(r"^\d{2}:\d{2}:\d{2}\s+\d{2}/\d{2}/\d{4}\s+", "", narration_block).strip()

            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str.replace(',', ''))
            
            withdrawal, deposit = 0.0, 0.0
            
            if prev_balance is not None:
                if balance > prev_balance + 0.001:
                    deposit = amount
                elif balance < prev_balance - 0.001:
                    withdrawal = amount
            else:
                if "NEFT-" in narration or "DEPOSIT" in narration:
                    deposit = amount
                else:
                    withdrawal = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d/%m/%Y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None, prev_balance

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    # --- THIS IS THE FIX ---
    header_pattern = re.compile(r"S\.No\s+Txn Date\s+Value Date\s+Description")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line:
            continue
            
        if start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance # Update balance
            
            current_block_lines = [line]
        
        elif current_block_lines:
            current_block_lines.append(line)
    
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    
    df = df.iloc[::-1].reset_index(drop=True)
    
    return df

def parse_indian_bank_v7(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting Indian Bank (v7) Parser ---")

    # This pattern finds the *start* of a new transaction line
    date_start_pattern = re.compile(r"^(\d{2}\s\w{3}\s\d{4})")
    
    # This pattern finds the LAST THREE money-like items
    # G1: Debit, G2: Credit, G3: Balance
    money_pattern_end = re.compile(r"(INR\s+[\d,.]+|-) (INR\s+[\d,.]+|-) (INR\s+[\d,.]+)$")

    # Helper to clean the money strings
    def clean_money(s):
        return s.replace('INR', '').replace(',', '').replace('-', '').strip()

    # --- Helper function to process a finished block ---
    def process_block(block_lines):
        if not block_lines:
            return None
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        date_match = date_start_pattern.match(full_block)
        money_match = money_pattern_end.search(full_block)
        
        if not date_match or not money_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None
            
        try:
            date_str = date_match.group(1)
            debit_str, credit_str, balance_str = money_match.groups()
            
            # Narration is between the date and the money
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                return None

            narration = full_block[narration_start_index:narration_end_index].strip()
            
            debit = float(clean_money(debit_str) or '0')
            credit = float(clean_money(credit_str) or '0')
            balance = float(clean_money(balance_str) or '0')
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d %b %Y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': debit,
                'Deposit Amt.': credit,
                'Closing Balance': balance
            }
            
            return txn_data
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"Date\s+Transaction Details\s+Debits\s+Credits\s+Balance")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or line.startswith("Date Transaction Details"):
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn = process_block(current_block_lines)
                if parsed_txn:
                    transactions.append(parsed_txn)
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn = process_block(current_block_lines)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df


def parse_indusind_bank_format4(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting IndusInd Bank (Format 6) Parser ---")

    # This pattern finds the *start* of a new transaction line
    date_start_pattern = re.compile(r"^(\d{2}-\w{3}-\d{4})")
    
    # This pattern finds the LAST TWO numbers on the line
    # G1: Amount (debit or credit), G2: Balance
    money_pattern_end = re.compile(r"([\d,.]+)\s+([\d,.]+)$")

    # Find Opening Balance
    last_balance = None
    ob_match = re.search(r"Brought Forward\s+([\d,.]+)", text, re.IGNORECASE)
    if ob_match:
        try:
            bal_str = ob_match.group(1).replace(',', '')
            last_balance = float(bal_str)
            print(f"Found Opening Balance: {last_balance}")
        except Exception:
            pass 
    
    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        date_match = date_start_pattern.match(full_block)
        money_match = money_pattern_end.search(full_block)
        
        if not date_match or not money_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None, prev_balance
            
        try:
            date_str = date_match.group(1)
            amount_str = money_match.group(1)
            balance_str = money_match.group(2)
            
            # Narration is between the date and the money
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                return None, prev_balance

            narration = full_block[narration_start_index:narration_end_index].strip()
            
            # --- Balance Logic ---
            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str.replace(',', ''))
            
            withdrawal, deposit = 0.0, 0.0
            
            if prev_balance is not None:
                if balance > prev_balance + 0.001:
                    deposit = amount
                elif balance < prev_balance - 0.001:
                    withdrawal = amount
            else:
                # Fallback guess for first transaction
                if "DEBIT" in narration or "DR" in narration:
                    withdrawal = amount
                else:
                    deposit = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d-%b-%Y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None, prev_balance

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    # --- THIS IS THE FIX ---
    # This part of the header is on a single line and is unique
    header_pattern = re.compile(r"Chq\./Ref\. No")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or "Brought Forward" in line or "WithDrawal Deposit Balance" in line:
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance # Update balance
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df


# --- (NEW Parser: IndusInd Bank - v6) ---
def parse_indusind_bank_format5(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting IndusInd Bank (Format 6) Parser ---")

    # This pattern finds the *start* of a new transaction line
    # It matches 'N1832... or S623...
    start_pattern = re.compile(r"^'\w{10,}|^\w{7,}$")
    
    # This pattern finds the LAST TWO numbers on the line
    # G1: Amount (debit or credit), G2: Balance
    money_pattern_end = re.compile(r"([\d,.]+)\s+([\d,.]+)$")

    last_balance = None
    
    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        # We need to find the date, type, amount, and balance
        date_match = re.search(r"(\d{2}-\w{3}-\d{4})", full_block) # Find first date
        type_match = re.search(r"(Credit|Debit)", full_block)
        money_match = money_pattern_end.search(full_block)
        
        if not date_match or not money_match or not type_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None, prev_balance
            
        try:
            date_str = date_match.group(1)
            type_str = type_match.group(1)
            amount_str, balance_str = money_match.groups()
            
            # Narration is between the Type and the Amount
            narration_start_index = type_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                narration = "N/A" # Handle cases where narration is missing
            else:
                narration = full_block[narration_start_index:narration_end_index].strip()
            
            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str.replace(',', ''))
            
            withdrawal, deposit = 0.0, 0.0
            
            if type_str == 'Debit':
                withdrawal = amount
            else:
                deposit = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d-%b-%Y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None, prev_balance

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    # --- THIS IS THE FIX ---
    # Look for the new header
    header_pattern = re.compile(r"Bank Reference\s+Value Date\s+Transaction")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or "Brought Forward" in line:
            continue
            
        if start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance # Update balance
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df


def parse_kotak_bank_format2(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting Kotak Bank (v2) Parser ---")

    # This pattern finds the *start* of a new transaction line
    date_start_pattern = re.compile(r"^(\d{2}-\w{3}-\d{2})")
    
    # This pattern finds the LAST TWO numbers on the line
    # G1: Amount (debit or credit), G2: Balance (with "(Cr)" or "(Dr)")
    money_pattern_end = re.compile(r"([\d,.]+)\s+([\d,.]+\((?:Cr|Dr)\))$")

    # Find Opening Balance
    last_balance = None
    ob_match = re.search(r"OPENINGBALANCE\.\.\.\s+([\d,.]+)\s+([\d,.]+\(Cr\))", text, re.IGNORECASE)
    if ob_match:
        try:
            bal_str = ob_match.group(2).replace(',', '').replace('(Cr)', '')
            last_balance = float(bal_str)
            print(f"Found Opening Balance: {last_balance}")
        except Exception:
            pass 
    
    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        date_match = date_start_pattern.match(full_block)
        money_match = money_pattern_end.search(full_block)
        
        if not date_match or not money_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None, prev_balance
            
        try:
            date_str = date_match.group(1)
            amount_str = money_match.group(1)
            balance_str = money_match.group(2).replace(',', '').replace('(Cr)', '').replace('(Dr)', '')
            
            # Narration is between the date and the money
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                return None, prev_balance

            narration = full_block[narration_start_index:narration_end_index].strip()
            
            # Clean "OPENINGBALANCE..." from narration
            if "OPENINGBALANCE" in narration:
                narration = "Opening Balance"
            
            # --- Balance Logic ---
            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str)
            
            withdrawal, deposit = 0.0, 0.0
            
            if prev_balance is not None:
                if balance > prev_balance + 0.001:
                    deposit = amount
                elif balance < prev_balance - 0.001:
                    withdrawal = amount
            else:
                # Fallback guess for first transaction
                if "NEFT" in narration or "CREDIT" in narration:
                    deposit = amount
                else:
                    withdrawal = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d-%b-%y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None, prev_balance

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"Withdrawal\s+\(Dr\)\s+Deposit\s+\(Cr\)\s+Balance")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line:
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance # Update balance
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df

def parse_kotak_bank_v3(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting Kotak Bank (v3) Parser ---")

    # --- THIS IS THE FIX ---
    # G1: Date (dd-mm-yyyy)
    date_start_pattern = re.compile(r"^(\d{2}-\d{2}-\d{4})")
    
    # G1: Amount (with Cr/Dr), G2: Balance (with Cr/Dr)
    money_pattern_end = re.compile(r"([\d,.]+\((?:Cr|Dr)\))\s+([\d,.]+\((?:Cr|Dr)\))$")
    # --- END OF FIX ---

    # Helper to clean the money strings
    def clean_money(s):
        return s.replace(',', '').replace('(Cr)', '').replace('(Dr)', '').strip()

    # Find Opening Balance
    last_balance = None
    ob_match = re.search(r"OPENINGBALANCE\.\.\.\s+([\d,.]+)\s+([\d,.]+\(Cr\))", text, re.IGNORECASE)
    if ob_match:
        try:
            bal_str = ob_match.group(2) # Get the balance string, e.g., "385,057.29(Cr)"
            last_balance = float(clean_money(bal_str)) # Use the helper
            print(f"Found Opening Balance: {last_balance}")
        except Exception:
            pass 
    
    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        date_match = date_start_pattern.match(full_block)
        money_match = money_pattern_end.search(full_block)
        
        if not date_match or not money_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None, prev_balance
            
        try:
            date_str = date_match.group(1)
            amount_str = money_match.group(1) # e.g., "2,000.00(Cr)"
            balance_str_raw = money_match.group(2) # e.g., "2,000.00(Cr)"
            
            balance_str = clean_money(balance_str_raw) # This call will now work
            
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                narration = "N/A"
            else:
                narration = full_block[narration_start_index:narration_end_index].strip()
            
            if "OPENINGBALANCE" in narration:
                narration = "Opening Balance"
            
            # --- Balance Logic ---
            amount_raw = clean_money(amount_str)
            amount = float(amount_raw if amount_raw else '0')
            balance = float(balance_str)
            
            withdrawal, deposit = 0.0, 0.0
            
            if "(Dr)" in amount_str:
                withdrawal = amount
            elif "(Cr)" in amount_str:
                deposit = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d-%m-%Y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None, prev_balance

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    # This header check is correct
    header_pattern = re.compile(r"Date\s+Narration\s+Chq/Ref No")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        # Skip junk lines *after* header is found
        if not line or "--- PAGE BREAK ---" in line or header_pattern.search(line) or "Deposit(Cr) Balance" in line:
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance # Update balance
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    
    # Remove the Opening Balance row if it was parsed as a transaction
    df = df[df['Narration'] != 'Opening Balance'].reset_index(drop=True)
    
    return df

def parse_punjab_national_bank_v2(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting PNB (v2) Parser ---")

    # This pattern finds the *start* of a new transaction line
    date_start_pattern = re.compile(r"^(\d{2}/\d{2}/\d{4})")
    
    # This pattern finds all the data in a joined block
    # G1: Date, G2: Amount, G3: Type, G4: Balance, G5: Remarks
    transaction_pattern = re.compile(
        r"^(\d{2}/\d{2}/\d{4})\s+([\d,.]+)\s+(DR|CR)\s+([\d,.]+)\s+(.*)$"
    )

    # --- Helper function to process a finished block ---
    def process_block(block_lines):
        if not block_lines:
            return None
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        match = transaction_pattern.search(full_block)
        
        if not match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None
            
        try:
            date_str = match.group(1)
            amount_str = match.group(2)
            type_str = match.group(3)
            balance_str = match.group(4)
            narration = match.group(5).strip()
            
            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str.replace(',', ''))
            
            withdrawal, deposit = 0.0, 0.0
            
            if type_str == 'DR':
                withdrawal = amount
            else:
                deposit = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d/%m/%Y', errors='coerce'),
                'Narration': narration,
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"Date\s+Instrument ID\s+Amount\s+Type\s+Balance\s+Remarks")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line:
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn = process_block(current_block_lines)
                if parsed_txn:
                    transactions.append(parsed_txn)
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn = process_block(current_block_lines)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    
    # File is in reverse-chronological order, so we reverse it
    df = df.iloc[::-1].reset_index(drop=True)
    
    return df

def parse_sbi_bank_v2(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting SBI Bank (v2) Parser ---")

    # This pattern finds the *start* of a new transaction line
    date_start_pattern = re.compile(r"^(\d{2}-\d{2}-\d{4})")
    
    # This pattern finds the LAST TWO numbers on the line
    # G1: Amount (debit or credit), G2: Balance (with "CR")
    money_pattern_end = re.compile(r"([\d,.]+)\s+([\d,.]+)CR$")

    # Find Opening Balance
    last_balance = None
    ob_match = re.search(r"BROUGHT FORWARD\s+([\d,.]+)CR", text, re.IGNORECASE)
    if ob_match:
        try:
            bal_str = ob_match.group(1).replace(',', '')
            last_balance = float(bal_str)
            print(f"Found Opening Balance: {last_balance}")
        except Exception:
            pass 
    
    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        date_match = date_start_pattern.match(full_block)
        money_match = money_pattern_end.search(full_block)
        
        if not date_match or not money_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None, prev_balance
            
        try:
            date_str = date_match.group(1)
            amount_str = money_match.group(1)
            balance_str = money_match.group(2)
            
            # Narration is between the date and the money
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                return None, prev_balance

            # Clean the Value Date from the start of the narration block
            narration = re.sub(r"^\d{2}-\d{2}-\d{4}\s+", "", full_block[narration_start_index:narration_end_index]).strip()
            
            # --- Balance Logic ---
            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str.replace(',', ''))
            
            withdrawal, deposit = 0.0, 0.0
            
            if prev_balance is not None:
                if balance > prev_balance + 0.001:
                    deposit = amount
                elif balance < prev_balance - 0.001:
                    withdrawal = amount
            else:
                # Fallback guess
                if "WDL" in narration:
                    withdrawal = amount
                else:
                    deposit = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d-%m-%Y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None, prev_balance

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"Post Date\s+Value Date\s+Description\s+Cheque")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or "BROUGHT FORWARD" in line or header_pattern.search(line):
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance # Update balance
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df

def parse_sbi_bank_v3(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting SBI Bank (v3) Parser ---")

    # This pattern finds the *start* of a new transaction line
    date_start_pattern = re.compile(r"^(\d{2}-\d{2}-\d{2})")
    
    # This pattern finds all parts of a joined transaction block
    # G1: Date, G2: Narration, G3: Credit, G4: Debit, G5: Balance
    transaction_pattern = re.compile(
        r"^(\d{2}-\d{2}-\d{2})\s+(.*?)\s+(-|[\d,.]+)\s+(-|[\d,.]+)\s+([\d,.]+)$"
    )

    # Helper to clean the money strings
    def clean_money(s):
        return s.replace(',', '').replace('-', '').strip()

    # --- Helper function to process a finished block ---
    def process_block(block_lines):
        if not block_lines:
            return None
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        match = transaction_pattern.search(full_block)
        
        if not match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None
            
        try:
            date_str, narration, credit_str, debit_str, balance_str = match.groups()
            
            debit = float(clean_money(debit_str) or '0')
            credit = float(clean_money(credit_str) or '0')
            balance = float(balance_str.replace(',', ''))
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d-%m-%y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': debit,
                'Deposit Amt.': credit,
                'Closing Balance': balance
            }
            
            return txn_data
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"Date\s+Transaction Reference\s+Ref\.No\./Chq\.No\.")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or "BROUGHT FORWARD" in line or header_pattern.search(line):
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn = process_block(current_block_lines)
                if parsed_txn:
                    transactions.append(parsed_txn)
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn = process_block(current_block_lines)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df

def parse_uco_bank_v2(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting UCO Bank (v2) Parser ---")

    # This pattern finds the *start* of a new transaction line
    date_start_pattern = re.compile(r"^(\d{2}-\d{2}-\d{4})")
    
    # This pattern finds the LAST TWO numbers on the line
    # G1: Amount (debit or credit), G2: Balance (with " CR")
    money_pattern_end = re.compile(r"([\d,.]+)\s+([\d,.]+\s+CR)$")

    # Find Opening Balance
    last_balance = None
    ob_match = re.search(r"Opening Balance as of \d{2}/\d{2}/\d{4}\s+([\d,.]+)\s+CR", text, re.IGNORECASE)
    if ob_match:
        try:
            bal_str = ob_match.group(1).replace(',', '')
            last_balance = float(bal_str)
            print(f"Found Opening Balance: {last_balance}")
        except Exception:
            pass 
    
    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        date_match = date_start_pattern.match(full_block)
        money_match = money_pattern_end.search(full_block)
        
        if not date_match or not money_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None, prev_balance
            
        try:
            date_str = date_match.group(1)
            amount_str = money_match.group(1)
            balance_str = money_match.group(2).replace(' CR', '').replace(',', '')
            
            # Narration is between the date and the money
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                return None, prev_balance

            narration = full_block[narration_start_index:narration_end_index].strip()
            
            # --- Balance Logic ---
            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str)
            
            withdrawal, deposit = 0.0, 0.0
            
            if prev_balance is not None:
                if balance > prev_balance + 0.001:
                    deposit = amount
                elif balance < prev_balance - 0.001:
                    withdrawal = amount
            else:
                # Fallback guess
                if "MPAY/UPI/TRTR" in narration:
                    withdrawal = amount
                else:
                    deposit = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d-%m-%Y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None, prev_balance

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"DATE\s+PARTICULARS\s+CHQ\.NO\.")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or "Opening Balance as of" in line:
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance # Update balance
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df

def parse_union_bank_v2(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting Union Bank (v2) Parser ---")

    # This pattern finds the *start* of a new transaction line
    # G1: Date
    date_start_pattern = re.compile(r"^\d+\s+(\d{2}/\d{2}/\d{4})")
    
    # This pattern finds the LAST TWO money values on the line
    # G1: Amount (with Cr/Dr), G2: Balance (with Cr/Dr)
    money_pattern_end = re.compile(r"([\d,.]+\s+\((?:Cr|Dr)\))\s+([\d,.]+\s+\((?:Cr|Dr)\))$")

    # Helper to clean the money strings
    def clean_money(s):
        return s.replace(',', '').replace('(Cr)', '').replace('(Dr)', '').strip()

    # --- Helper function to process a finished block ---
    def process_block(block_lines):
        if not block_lines:
            return None
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        date_match = date_start_pattern.match(full_block)
        money_match = money_pattern_end.search(full_block)
        
        if not date_match or not money_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None
            
        try:
            date_str = date_match.group(1)
            amount_str = money_match.group(1) # e.g., "472.00 (Dr)"
            balance_str_raw = money_match.group(2) # e.g., "235.10 (Cr)"
            
            balance_str = clean_money(balance_str_raw)
            
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                narration = "N/A"
            else:
                narration = full_block[narration_start_index:narration_end_index].strip()
            
            # Clean the Transaction ID from the start of the narration
            narration = re.sub(r"^\w+\s+", "", narration).strip()
            
            amount_raw = clean_money(amount_str)
            amount = float(amount_raw if amount_raw else '0')
            balance = float(balance_str)
            
            withdrawal, deposit = 0.0, 0.0
            
            if "(Dr)" in amount_str:
                withdrawal = amount
            elif "(Cr)" in amount_str:
                deposit = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d/%m/%Y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"S\.No\s+Date\s+Transaction Id\s+Remarks")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or header_pattern.search(line):
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn = process_block(current_block_lines)
                if parsed_txn:
                    transactions.append(parsed_txn)
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn = process_block(current_block_lines)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End of State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    
    # File is in reverse-chronological order, so we reverse it
    df = df.iloc[::-1].reset_index(drop=True)
    
    return df

def parse_union_bank_v3(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting Union Bank (v4) Parser ---")

    # --- THIS IS THE FIX ---
    # This regex is designed to capture all parts of a *single* transaction line
    # It allows for an optional cheque number
    # G1: Date, G2: Narration, G3: Cheque (optional), G4: Withdrawal, G5: Deposit, G6: Balance
    transaction_pattern = re.compile(
        r"^(\d{2}-\d{2}-\d{4})\s+(.+?)\s+(\d{8,})?\s+([\d,.]*)\s+([\d,.]*)\s+([\d,.]+)Cr$"
    )
    # --- END OF FIX ---
    
    # Helper to clean the money strings
    def clean_money(s):
        s = s.replace(',', '').strip()
        return float(s) if s else 0.0

    data_started = False
    header_pattern = re.compile(r"DATE\s+PARTICULARS\s+CHQ\.NO\.")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or "Cumulative Totals:" in line or header_pattern.search(line):
            continue
            
        match = transaction_pattern.search(line)
        
        if match:
            try:
                date_str = match.group(1)
                narration = match.group(2)
                # group 3 is cheque no, we can ignore it
                debit_str = match.group(4)
                credit_str = match.group(5)
                balance_str = match.group(6)
                
                withdrawal = clean_money(debit_str)
                deposit = clean_money(credit_str)
                balance = float(balance_str.replace(',', ''))
                
                txn_data = {
                    'Date': pd.to_datetime(date_str, format='%d-%m-%Y', errors='coerce'),
                    'Narration': narration.strip(),
                    'Withdrawal Amt.': withdrawal,
                    'Deposit Amt.': deposit,
                    'Closing Balance': balance
                }
                transactions.append(txn_data)
                
            except Exception as e:
                # print(f"Error processing line: {e} | Line: {line}") # Debug
                continue
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    
    return df

def parse_union_bank_format4(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting Union Bank (Format 4) Parser ---")

    # This pattern finds the *start* of a new transaction line
    # G1: Date
    date_start_pattern = re.compile(r"^\d+\s+(\d{2}-\d{2}-\d{4})")
    
    # This pattern finds the LAST TWO numbers on the line
    # G1: Amount (debit or credit), G2: Balance (with " Cr")
    money_pattern_end = re.compile(r"([\d,.]+)\s+([\d,.]+\s+Cr)$")

    # Find Opening Balance
    last_balance = None
    ob_match = re.search(r"Opening\s+Balance\s+([\d,.]+)\s+Cr", text, re.IGNORECASE)
    if ob_match:
        try:
            bal_str = ob_match.group(1).replace(',', '')
            last_balance = float(bal_str)
            print(f"Found Opening Balance: {last_balance}")
        except Exception:
            pass 
    
    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        date_match = date_start_pattern.match(full_block)
        money_match = money_pattern_end.search(full_block)
        
        if not date_match or not money_match:
            return None, prev_balance
            
        try:
            date_str = date_match.group(1)
            amount_str = money_match.group(1)
            balance_str = money_match.group(2).replace(' Cr', '').replace(',', '')
            
            # Narration is between the date and the money
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                return None, prev_balance

            narration = full_block[narration_start_index:narration_end_index].strip()
            
            # --- Balance Logic ---
            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str)
            
            withdrawal, deposit = 0.0, 0.0
            
            if prev_balance is not None:
                if balance > prev_balance + 0.001:
                    deposit = amount
                elif balance < prev_balance - 0.001:
                    withdrawal = amount
            else:
                # Fallback guess
                if "CR/" in narration or "RTGS:" in narration:
                    deposit = amount
                else:
                    withdrawal = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d-%m-%Y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            return None, prev_balance

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"SI\s+Date\s+Particulars\s+Chq\s+Num")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        if not line or "--- PAGE BREAK ---" in line or "Opening Balance" in line or header_pattern.search(line):
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance # Update balance
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df
def parse_yes_bank_format2(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting YES Bank (Format 2) Parser ---")

    # This pattern finds the *start* of a new transaction line
    # G1: Transaction Date, G2: Value Date
    date_start_pattern = re.compile(r"^(\d{2}-\w{3}-\d{4})\s+(\d{2}-\w{3}-\d{4})")
    
    # This pattern finds the LAST TWO numbers on the line
    # G1: Amount (debit or credit), G2: Balance
    # We look for two numbers at the end, ignoring the optional third.
    money_pattern_end = re.compile(r"([\d,.]+)\s+([\d,.]+)$")

    # Find Opening Balance (if any)
    last_balance = None
    
    # --- Helper function to process a finished block ---
    def process_block(block_lines, prev_balance):
        if not block_lines:
            return None, prev_balance
            
        full_block = " ".join(block_lines).replace('\n', ' ').strip()
        full_block = re.sub(r'\s+', ' ', full_block) # Consolidate spaces
        
        date_match = date_start_pattern.match(full_block)
        money_match = money_pattern_end.search(full_block)
        
        if not date_match or not money_match:
            # print(f"Block skipped (no match): {full_block[:70]}...") # Debug
            return None, prev_balance
            
        try:
            date_str = date_match.group(1) # Transaction Date
            amount_str = money_match.group(1) # This is either W/D or Deposit
            balance_str = money_match.group(2) # This is the balance
            
            # Narration is everything between the date block and the money block
            narration_start_index = date_match.end()
            narration_end_index = money_match.start()
            
            if narration_start_index >= narration_end_index:
                return None, prev_balance

            narration_block = full_block[narration_start_index:narration_end_index].strip()
            
            # Clean the Cheq/Ref No. from the start of the narration block
            narration = re.sub(r"^\S+\s+", "", narration_block).strip()

            # --- Balance Logic ---
            amount = float(amount_str.replace(',', ''))
            balance = float(balance_str.replace(',', ''))
            
            withdrawal, deposit = 0.0, 0.0
            
            if prev_balance is not None:
                if balance > prev_balance + 0.001:
                    deposit = amount
                elif balance < prev_balance - 0.001:
                    withdrawal = amount
            else:
                # Fallback guess for first transaction
                if "CR" in narration.upper() or "NEFT CR" in narration.upper():
                    deposit = amount
                else:
                    withdrawal = amount
            
            txn_data = {
                'Date': pd.to_datetime(date_str, format='%d-%b-%Y', errors='coerce'),
                'Narration': narration.strip(),
                'Withdrawal Amt.': withdrawal,
                'Deposit Amt.': deposit,
                'Closing Balance': balance
            }
            
            return txn_data, balance # Return new balance
            
        except Exception as e:
            # print(f"Error processing block: {e} | Block: {full_block[:70]}...") # Debug
            return None, prev_balance

    # --- State Machine ---
    current_block_lines = []
    data_started = False
    
    header_pattern = re.compile(r"Transaction Date\s+Value Date\s+Cheque No/ Reference No")
    
    for line in text.split('\n'):
        line = line.strip()

        if not data_started:
            if header_pattern.search(line):
                data_started = True
                print("Header found, starting parser.")
            continue
        
        # Skip junk lines
        if not line or \
           "--- PAGE BREAK ---" in line or \
           header_pattern.search(line) or \
           line.startswith("Page ") or \
           line.startswith("Primary Holder:") or \
           line.startswith("POOJA BIND"):
            continue
            
        if date_start_pattern.match(line):
            if current_block_lines:
                parsed_txn, new_balance = process_block(current_block_lines, last_balance)
                if parsed_txn:
                    transactions.append(parsed_txn)
                    last_balance = new_balance # Update balance
            
            current_block_lines = [line]
        
        elif current_block_lines:
            # This is a continuation line
            current_block_lines.append(line)
    
    # Process the last block
    if current_block_lines:
        parsed_txn, new_balance = process_block(current_block_lines, last_balance)
        if parsed_txn:
            transactions.append(parsed_txn)
    # --- End State Machine ---
        
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"--- Parser finished: Extracted {len(transactions)} transactions. ---")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df

def parse_au_bank_format4(text: str) -> pd.DataFrame:
    transactions = []
    print("--- Starting AU Bank (Format 4) Parser ---")

    # Pattern to find transaction date lines
    date_line_pattern = re.compile(r"^(\d{2}\s\w{3}\s\d{4})\s+(\d{2}\s\w{3}\s\d{4})$")
    
    # Pattern to find money lines (with or without dashes for zero values)
    # Handles: "N093242966171056 -  1,63,666.00  1,73,666.80"
    # Handles: "409321190797  69,333.00 -  1,04,333.80"  
    # Handles: "-  30,648.00  40,719.78"
    # Handles: "69,333.00 -  10,000.80"
    money_line_pattern = re.compile(r"^\s*(?:[\w\d]+\s+)?(-|[\d,]+\.\d{2})\s+(-|[\d,]+\.\d{2})\s+([\d,]+\.\d{2})\s*$")

    # Find Opening Balance
    last_balance = None
    ob_match = re.search(r"Opening Balance\(₹\)\s+([\d,]+\.\d{2})", text, re.IGNORECASE)
    if ob_match:
        try:
            bal_str = ob_match.group(1).replace(',', '')
            last_balance = float(bal_str)
            print(f"Found Opening Balance: {last_balance}")
        except Exception:
            pass
    
    # --- State Machine Variables ---
    current_date = None
    current_narration_lines = []
    data_started = False
    
    lines = text.split('\n')
    print(f"\nDEBUG: Total lines in PDF: {len(lines)}")
    
    for i, line in enumerate(lines):
        line_stripped = line.strip()
        
        # Find header
        if not data_started:
            if "Description/Narration" in line or (i > 0 and "Transaction" in lines[i-1] and "Date Value Date" in line):
                data_started = True
                print(f"✓ Header found at line {i}")
                continue
            continue
        
        # Skip junk lines
        if not line_stripped or \
           "--- PAGE BREAK ---" in line_stripped or \
           "ACCOUNT STATEMENT" in line_stripped or \
           "This is an auto-generated" in line_stripped or \
           "Page " in line_stripped or \
           "Customer ID" in line_stripped or \
           line_stripped.startswith("Call us at") or \
           line_stripped.startswith("Website"):
            continue
        
        # Check if this is a date line (start of new transaction)
        date_match = date_line_pattern.match(line_stripped)
        
        if date_match:
            # Process previous transaction if exists
            if current_date and current_narration_lines:
                # Try to find money line in accumulated narration
                money_line_found = False
                for narr_line in current_narration_lines:
                    money_match = money_line_pattern.match(narr_line)
                    if money_match:
                        try:
                            debit_str = money_match.group(1)
                            credit_str = money_match.group(2)
                            balance_str = money_match.group(3)
                            
                            # Clean amounts
                            debit = 0.0 if debit_str == '-' else float(debit_str.replace(',', ''))
                            credit = 0.0 if credit_str == '-' else float(credit_str.replace(',', ''))
                            balance = float(balance_str.replace(',', ''))
                            
                            # Build narration (exclude the money line)
                            narration_parts = [l for l in current_narration_lines if l != narr_line]
                            narration = " ".join(narration_parts).strip()
                            
                            # Determine withdrawal/deposit based on balance change
                            withdrawal, deposit = 0.0, 0.0
                            amount = debit if debit > 0 else credit
                            
                            if last_balance is not None:
                                if balance > last_balance + 0.001:
                                    deposit = amount
                                elif balance < last_balance - 0.001:
                                    withdrawal = amount
                            else:
                                # Fallback: credit column has value = deposit
                                if credit > 0:
                                    deposit = credit
                                else:
                                    withdrawal = debit
                            
                            txn_data = {
                                'Date': pd.to_datetime(current_date, format='%d %b %Y', errors='coerce'),
                                'Narration': narration,
                                'Withdrawal Amt.': withdrawal,
                                'Deposit Amt.': deposit,
                                'Closing Balance': balance
                            }
                            
                            transactions.append(txn_data)
                            last_balance = balance
                            money_line_found = True
                            break
                            
                        except Exception as e:
                            print(f"Error processing transaction: {e}")
                            continue
                
                if not money_line_found:
                    print(f"Warning: No money line found for transaction on {current_date}")
            
            # Start new transaction
            current_date = date_match.group(1)  # Transaction date
            current_narration_lines = []
            
        elif current_date:
            # Accumulate narration lines
            current_narration_lines.append(line_stripped)
    
    # Process last transaction
    if current_date and current_narration_lines:
        for narr_line in current_narration_lines:
            money_match = money_line_pattern.match(narr_line)
            if money_match:
                try:
                    debit_str = money_match.group(1)
                    credit_str = money_match.group(2)
                    balance_str = money_match.group(3)
                    
                    debit = 0.0 if debit_str == '-' else float(debit_str.replace(',', ''))
                    credit = 0.0 if credit_str == '-' else float(credit_str.replace(',', ''))
                    balance = float(balance_str.replace(',', ''))
                    
                    narration_parts = [l for l in current_narration_lines if l != narr_line]
                    narration = " ".join(narration_parts).strip()
                    
                    withdrawal, deposit = 0.0, 0.0
                    amount = debit if debit > 0 else credit
                    
                    if last_balance is not None:
                        if balance > last_balance + 0.001:
                            deposit = amount
                        elif balance < last_balance - 0.001:
                            withdrawal = amount
                    else:
                        if credit > 0:
                            deposit = credit
                        else:
                            withdrawal = debit
                    
                    txn_data = {
                        'Date': pd.to_datetime(current_date, format='%d %b %Y', errors='coerce'),
                        'Narration': narration,
                        'Withdrawal Amt.': withdrawal,
                        'Deposit Amt.': deposit,
                        'Closing Balance': balance
                    }
                    
                    transactions.append(txn_data)
                    break
                    
                except Exception as e:
                    print(f"Error processing last transaction: {e}")
                    continue
    
    if not transactions:
        print("--- Parser finished: No transactions were extracted. ---")
        return pd.DataFrame()

    print(f"\n✓✓✓ Parser finished: Extracted {len(transactions)} transactions.")
    df = pd.DataFrame(transactions)
    df = df.dropna(subset=['Date'])
    return df


def parse_bank_statement(filename: str, file_content: bytes) -> pd.DataFrame:
    """
    Main router function. Extracts text and routes to the correct parser
    based *only* on the filename.
    """
    
    # 1. Extract text
    text = extract_text_from_pdf(filename, file_content)
    if not text:
        return pd.DataFrame()
    
    print(f"--- Processing: {filename} ---")
    upper_filename = filename.upper()
    
    # We still need clean_upper_text for *internal* format checks (like for Axis/ICICI)
    upper_text = text[:1500].upper()
    clean_upper_text = re.sub(r'\s+', '', upper_text)

    # ---
    # NEW FILENAME-ONLY ROUTER
    # This order is now much less important, but good to keep.
    # ---

    if "PUNJAB NATIONAL" in upper_filename or "PNB" in upper_filename:
        print("Bank identified by filename as: Punjab National Bank.")
        
        # Internal check for PNB's two formats
        # We check for the new format's unique header
        if "DATEINSTRUMENTIDAMOUNTTYPEBALANCEREMARKS" in clean_upper_text:
            print(" -> Using PNB Format 2 (v2 parser).")
            return parse_punjab_national_bank_v2(text)
        else:
            # Fallback to original parser
            print(" -> Using PNB Format 1 (original parser).")
            return parse_punjab_national_bank_v1(text)

    elif "YES BANK" in upper_filename:
        print("Bank identified by filename as: YES Bank.")
        
        # --- NEW ROUTER LOGIC (FIXED) ---
        # We check for Format 2's unique header "TRANSACTION DATE" first.
        # We must use 'upper_text' (with spaces) because 'clean_upper_text'
        # would merge "TRANSACTIONDATE" and "DATE".
        
        if "TRANSACTION DATE" in upper_text:
            print(" -> Found 'TRANSACTION DATE'. Using YES Bank Format 2 (parse_yes_bank_format2).")
            return parse_yes_bank_format2(text)
        
        # If it's not Format 2, it must be Format 1
        else:
            print(" -> 'TRANSACTION DATE' not found. Using YES Bank Format 1 (original parser).")
            return parse_yes_bank(text)
        
    elif "UNION" in upper_filename:
        print("Bank identified by filename as: Union Bank.")
        
        # Internal check for Union Bank's three formats
        # We check for the most unique headers first
        if "SIDATEPARTICULARSCHQNUM" in clean_upper_text:
            print(" -> Using Union Bank Format 4 (v4 parser).")
            return parse_union_bank_format4(text) # <-- The new parser
        
        if "DATEPARTICULARSCHQ.NO." in clean_upper_text:
            print(" -> Using Union Bank Format 3 (v3 parser).")
            return parse_union_bank_v3(text) # <-- The new parser
            
        elif "S.NODATETRANSACTIONIDREMARKS" in clean_upper_text:
            print(" -> Using Union Bank Format 2 (v2 parser).")
            return parse_union_bank_v2(text)
            
        else:
            # Fallback to original parser
            print(" -> Using Union Bank Format 1 (original parser).")
            return parse_union_bank(text)

    elif "INDIAN BANK" in upper_filename:
        print("Bank identified by filename as: Indian Bank.")
        
        # Internal check for Indian Bank's two formats
        # We check for the new format's unique header
        if "DATETRANSACTIONDETAILSDEBITS" in clean_upper_text:
            print(" -> Using Indian Bank Format 2 (v7 parser).")
            return parse_indian_bank_v7(text)
        else:
            # Fallback to original parser
            print(" -> Using Indian Bank Format 1 (v6 parser).")
            return parse_indian_bank_v6(text)
        
    elif "SBI" in upper_filename:
        print("Bank identified by filename as: SBI.")
        
        # Internal check for SBI's three formats
        # We check for the most unique headers first
        
        # --- THIS IS THE FIX ---
        # Check for the unique cover page text of Format 3 (v3 parser)
        if "TRANSACTION ACCOUNTS" in upper_text and "LOAN ACCOUNTS" in upper_text:
            print(" -> Using SBI Format 3 (v3 parser).")
            return parse_sbi_bank_v3(text) # The new parser
            
        # Check for v2 (SBI (FORMAT 3).pdf)
        elif "POSTDATEVALUEDATEDESCRIPTIONCHEQUENO/REFERENCE" in clean_upper_text:
            print(" -> Using SBI Format 2 (v2 parser).")
            return parse_sbi_bank_v2(text)
            
        else:
            # Fallback to original parser (v1)
            print(" -> Using SBI Format 1 (original parser).")
            return parse_sbi_bank(text)

    elif "DHANLAXMI" in upper_filename:
        print("Bank identified by filename as: Dhanlaxmi Bank.")
        return parse_dhanlaxmi_bank_v2(text)
        
    elif "SARASWAT" in upper_filename:
        print("Bank identified by filename as: Saraswat Bank.")
        return parse_saraswat_bank_v6(text)
        
    elif "IDBI" in upper_filename:
        print("Bank identified by filename as: IDBI Bank.")
        
        # Internal check for IDBI's three formats
        # We check for the most unique headers first
        
        # --- THIS IS THE FIX ---
        if "S.NOTXNDATEVALUEDATEDESCRIPTION" in clean_upper_text: # Check without spaces
            print(" -> Using IDBI Format 3 (v5 parser).")
            return parse_idbi_bank_format3(text) # Use the new parser
            
        elif "BALANCE(INR)AMOUNT(INR)" in clean_upper_text:
            print(" -> Using IDBI Format 2 (v4 parser).")
            return parse_idbi_bank_v4(text)
            
        else:
            # Fallback to original parser
            print(" -> Using IDBI Format 1 (original parser).")
            return parse_idbi_bank_format2(text)
        
    elif "IDFCFIRST" in upper_filename:
        print("Bank identified by filename as: IDFC First Bank.")
        return parse_idfc_first_bank(text)

    elif "INDIAN OVERSEAS" in upper_filename:
        print("Bank identified by filename as: Indian Overseas Bank.")
        return parse_indian_overseas_bank(text)
    
    elif "HDFC" in upper_filename:
        print("Bank identified by filename as: HDFC.")
        print(f"DEBUG: First 1000 chars of upper_text: {upper_text[:1000]}")
        print(f"DEBUG: First 500 chars of clean_upper_text: {clean_upper_text[:500]}")
        
        # Check for Format 1 indicators
        has_value_dt = "VALUE" in upper_text and "DT" in upper_text
        has_chq_ref = "CHQ" in upper_text or "REF" in upper_text
        
        print(f"DEBUG: has_value_dt={has_value_dt}, has_chq_ref={has_chq_ref}")
        
        if has_value_dt and has_chq_ref:
            print(" -> Using HDFC Format 1 (parse_hdfc_bank).")
            return parse_hdfc_bank(text)
        
        elif "CHQ./REF.NO.VALUEDT" in clean_upper_text:
            print(" -> Using HDFC Format 2 (parse_hdfc_bank_format2).")
            return parse_hdfc_bank_format2(text)
            
        else:
            print(" -> No specific HDFC header found, trying Format 1 as default.")
            return parse_hdfc_bank(text)
        

    elif "AXIS" in upper_filename:
        print("Bank identified by filename as: Axis.")
        # Internal check for Axis's two formats
        if "S.NOTRANSACTION" in clean_upper_text:
            print(" -> Using Axis Format 1.")
            return parse_axis_bank_format1(text)
        elif "TRANDATECHQNO" in clean_upper_text:
            print(" -> Using Axis Format 2.")
            return parse_axis_bank_format2(text)
        else:
            df = parse_axis_bank_format1(text)
            if not df.empty: return df
            return parse_axis_bank_format2(text)

    elif "INDUSIND" in upper_filename or "INDB" in clean_upper_text: # Corrected from INDUSLAND
        print("Bank identified by filename as: IndusInd Bank.")
        
        # --- CHECK FOR FORMAT 2 FIRST (IT IS MORE SPECIFIC) ---
        if "FINSENSESECURITIES" in clean_upper_text:
            print(" -> Using IndusInd Format 2.")
            return parse_indusind_bank_format2(text)

        # --- NEW CHECK FOR FORMAT 5 ---
        elif "BANKREFERENCEVALUEDATETRANSACTION" in clean_upper_text:
             print(" -> Using IndusInd Format 5.")
             return parse_indusind_bank_format5(text) # <-- The new parser
        
        # --- Your other existing checks (unchanged) ---
        elif "CHQ./REF.NOWITHDRAWALDEPOSITBALANCE" in clean_upper_text:
             print(" -> Using IndusInd Format 4.")
             return parse_indusind_bank_format4(text) 
        
        elif "DATE TYPE DESCRIPTION DEBIT CREDIT BALANCE" in re.sub(r'\s+', ' ', upper_text):
            print(" -> Using IndusInd Format 3.")
            return parse_indusind_bank_format3(text)
            
        else:
            print(" -> Using IndusInd Format 1.")
            return parse_indusind_bank(text)
        

    elif "AU" in upper_filename:
        print("Bank identified by filename as: AU Small Finance Bank.")
        
        # Debug: print first 1500 chars
        print(f"DEBUG Router: First 1500 chars of text:\n{text[:1500]}\n")
        print(f"DEBUG Router: First 500 chars of upper_text:\n{upper_text[:500]}\n")
        
        # Internal check for AU's formats
        # Check for Format 4 (new format) - multiple ways to detect
        format4_indicators = [
            "TRANSACTION" in upper_text and "VALUE DATE" in upper_text,
            "DESCRIPTION/NARRATION" in upper_text,
            "ACCOUNT STATEMENT" in upper_text and "AU SMALL FINANCE BANK" in upper_text
        ]
        
        if any(format4_indicators):
            print(" -> Using AU Format 4 (v4 parser) - New format detected")
            return parse_au_bank_format4(text)
        
        # Check for Format 3 (Format 9 in your code)
        elif "TXNDATEVALUE" in clean_upper_text:
            print(" -> Using AU Format 3 (v9 parser).")
            return parse_au_bank_format3(text)
        
        else:
            print(" -> Using AU Format 1 (original parser).")
            result = parse_au_bank(text)
            
            # If Format 1 fails, try Format 4 as fallback
            if result.empty:
                print(" -> Format 1 failed, trying Format 4 as fallback...")
                return parse_au_bank_format4(text)
            
            return result
        

    elif "ICICI" in upper_filename:
        print("Bank identified by filename as: ICICI Bank.")
        
        # Internal check for ICICI's three formats
        # We check for the most unique headers first
        
        if "DATE DESCRIPTION AMOUNT TYPE" in upper_text:
            print(" -> Using ICICI Format 3 (v3 parser).")
            return parse_icici_bank_format3(text)
            
        elif "SNO.VALUEDATETRANSACTIONDATE" in clean_upper_text:
            print(" -> Using ICICI Format 2 (v2 parser).")
            return parse_icici_bank_format2(text)
            
        else:
            # Fallback to original parser
            print(" -> Using ICICI Format 1 (original parser).")
            return parse_icici_bank(text)

    elif "KOTAK" in upper_filename or "KKBK" in clean_upper_text:
        print("Bank identified by filename as: Kotak Bank.")
        
        # Internal check for Kotak's three formats
        # We check for the most unique headers first
        
        # Format 3 (dd-mm-yyyy dates)
        if "DATENARRATIONCHQ/REFNOWITHDRAWAL(DR)/DEPOSIT(CR)BALANCE" in clean_upper_text:
            print(" -> Using Kotak Format 4 (v3 parser).")
            return parse_kotak_bank_v3(text) # The new parser
            
        # Format 2 (dd-Mmm-yy dates)
        elif "DATENARRATIONCHQ/REFNO." in clean_upper_text:
            print(" -> Using Kotak Format 2 (v2 parser).")
            return parse_kotak_bank_format2(text)
            
        else:
            # Fallback to original block parser
            print(" -> Using Kotak Format 1 (original parser).")
            return parse_kotak_bank(text)

    elif "UCO" in upper_filename:
        print("Bank identified by filename as: UCO Bank.")
        
        # Internal check for UCO's two formats
        # We check for the new format's unique header
        if "DATEPARTICULARSCHQ.NO." in clean_upper_text:
            print(" -> Using UCO Format 2 (v2 parser).")
            return parse_uco_bank_v2(text)
        else:
            # Fallback to original parser
            print(" -> Using UCO Format 1 (original parser).")
            return parse_uco_bank(text)
        
    elif "CENTRAL BANK" in upper_filename:
        print("Bank identified by filename as: Central Bank of India.")
        
        # Internal check for CBI's three formats
        # We must check in the correct, most-specific-first order.
        
        if "DATEPARTICULARSWITHDRAWALS" in clean_upper_text:
            print(" -> Using CBI Format 3 (v3 parser).")
            return parse_central_bank_of_india_format3(text)
            
        elif "POSTDATEVALUEDATE" in clean_upper_text: # <-- v2 check (now uses spaceless text)
            print(" -> Using CBI Format 2 (v2 parser).")
            return parse_central_bank_of_india_format2(text)
            
        elif "POSTDATETXNDATE" in clean_upper_text: # <-- v1 check (now uses spaceless text)
            print(" -> Using CBI Format 1 (original parser).")
            return parse_central_bank_of_india(text)
            
        else:
            # A final fallback if no header matches
            print(" -> Could not detect specific format, trying v1.")
            return parse_central_bank_of_india(text)

    elif "PUNJAB & SIND" in upper_filename:
        print("Bank identified by filename as: Punjab & Sind Bank.")
        return parse_punjab_sind_bank(text)

    elif "CANARA" in upper_filename:
        print("Bank identified by filename as: Canara Bank.")

        # Internal check for Canara's two formats
        # We check for the new format's unique header
        if "Txn Date Value Date" in upper_text:
            print(" -> Using Canara Format 2 (v3 parser).")
            return parse_canara_bank_format2(text)
        else:
            # Fallback to original parser
            print(" -> Using original Canara parser.")
            return parse_canara_bank(text)

    elif "EQUITAS" in upper_filename:
        print("Bank identified by filename as: Equitas.")
        return parse_equitas_bank(text)

    elif "FEDERAL BANK" in upper_filename:
        print("Bank identified by filename as: Federal Bank.")
        return parse_federal_bank(text)
        
    

    elif "BANDHAN" in upper_filename:
        print("Bank identified by filename as: Bandhan Bank.")
        return parse_bandhan_bank(text)
        
    elif "BARODA" in upper_filename:
        print("Bank identified by filename as: Bank of Baroda.")
        
        # --- NEW: Check for Format 3 first ---
        # It has a very unique header.
        if "WITHDRAWAL (DR) DEPOSIT (CR) BALANCE" in upper_text:
            print(" -> Using BoB Format 4 (v3 parser).")
            return parse_bank_of_baroda_format4(text)
        
        # --- EXISTING: Fallback to original logic for Format 1 and 2 ---
        else:
            print(" -> Format 4 header not found. Trying Format 1...")
            df = parse_bank_of_baroda(text) 
            if not df.empty:
                print(" -> BoB Format 1 SUCCEEDED.")
                return df
                
            print(" -> BoB Format 1 failed, trying Format 2...")
            return parse_bank_of_baroda_format2(text)
        
    elif "BANK OF INDIA" in upper_filename:
        print("Bank identified by filename as: Bank of India.")
        return parse_bank_of_india(text)

    

    
            
    
                   
    # ---
    # 4. FINAL FALLBACK
    # ---
    else:
        print(f"⚠️ No parser found for filename: {filename}. Skipping.")
        return pd.DataFrame()