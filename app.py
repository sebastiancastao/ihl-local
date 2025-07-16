import os
import csv
import math
from pathlib import Path
import platform
import pandas as pd
import re
import shutil
from difflib import get_close_matches
from io import StringIO
from flask import Flask, render_template, request, send_file, jsonify, session
from werkzeug.utils import secure_filename
from data_processor import DataProcessor
from csv_exporter import CSVExporter
from config import OUTPUT_CSV_NAME  # e.g. "combined_data.csv"
#test commit2
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = os.path.dirname(os.path.abspath(__file__))
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['SESSION_COOKIE_SAMESITE'] = 'None'  # Allow cookie in iframe
app.config['SESSION_COOKIE_SECURE'] = True  # Required for SameSite=None
app.config['SESSION_COOKIE_HTTPONLY'] = False  # Allow JavaScript access to cookie
app.secret_key = 'your-secret-key-here'  # Required for session management

# Allowed extensions for CSV/XLSX upload
ALLOWED_CSV_EXTENSIONS = {'csv', 'xlsx', 'xls'}

# Path for the master UOM file
MASTER_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "master_data")
MASTER_UOM_FILE = os.path.join(MASTER_DATA_DIR, "master_uom.csv")

# Ensure master data directory exists
if not os.path.exists(MASTER_DATA_DIR):
    os.makedirs(MASTER_DATA_DIR)

def allowed_file(filename, allowed_set):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_set

# Helper function to normalize column headers for matching
def normalize_header(header):
    if not isinstance(header, str):
        return ""
    # Convert to lowercase
    header = str(header).lower()
    # Remove special characters and extra spaces
    header = re.sub(r'[^a-z0-9]', '', header)
    return header

# Helper function to find the best matching column in a dataframe
def find_matching_column(df, target_column, required=True):
    """
    Find the best matching column in the dataframe for a target column name.
    
    Args:
        df: DataFrame to search in
        target_column: The target column name to find
        required: If True, raises ValueError if no match found
        
    Returns:
        The actual column name in the dataframe that best matches the target
    """
    # If the column exists exactly as specified, use it
    if target_column in df.columns:
        return target_column
    
    # Normalize all column headers for matching
    normalized_target = normalize_header(target_column)
    normalized_headers = {normalize_header(col): col for col in df.columns}
    
    # Look for exact match with normalized headers
    if normalized_target in normalized_headers:
        return normalized_headers[normalized_target]
    
    # Try to find the closest match
    matches = get_close_matches(normalized_target, normalized_headers.keys(), n=1, cutoff=0.6)
    
    if matches:
        matched_header = normalized_headers[matches[0]]
        print(f"Matched '{target_column}' to '{matched_header}' in the uploaded file")
        return matched_header
    
    if required:
        raise ValueError(f"Could not find a suitable match for required column: {target_column}")
    
    return None

def process_first_csv(file_path, session_dir):
    """Process the first CSV file ('Sensual UOM Excel').
    Extract the following columns:
    - Item #
    - Weight
    - Cube
    - Length
    - Width
    - Height
    
    Create a new CSV with the following column headers:
    - Column S (18) = "Size"
    - Column U (20) = "UOM" 
    - Column V (21) = "Cartons"
    - Column W (22) = "CARTONS"
    - Column X (23) = "weight w/out add"
    - Column Y (24) = "individual carton weight (add 2 lbs)"
    - Column Z (25) = "cube in cm"
    - Column AA (26) = "Length"
    - Column AB (27) = "Width"
    - Column AC (28) = "Height"
    - Column AD (29) = "dimension"
    - Column AE (30) = "cube in cft"
    - Column AF (31) = "total cubes"
    - Column AG (32) = "PALLET"
    - Column AH (33) = "FINAL CUBE"
    - Column AI (34) = "TOTAL WEIGHT"
    """
    try:
        # Read the CSV or Excel file
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".csv":
            input_df = pd.read_csv(file_path, dtype=str)
        elif ext in [".xlsx", ".xls"]:
            input_df = pd.read_excel(file_path, dtype=str)
        else:
            return False, "Unsupported file extension"
        
        # Find column mappings - make best guess at matching columns based on names
        required_columns = ["Item #", "Weight", "Cube", "Length", "Width", "Height", "Sequence 10: QTY"]
        column_mappings = {}
        missing_columns = []
        
        print(f"Input columns: {input_df.columns.tolist()}")
        
        # Find the best matches for each required column
        for col in required_columns:
            try:
                matched_col = find_matching_column(input_df, col)
                column_mappings[col] = matched_col
                print(f"Matched '{col}' to '{matched_col}'")
            except ValueError as e:
                missing_columns.append(col)
                print(f"Could not match column: {col}")
        
        if missing_columns:
            return False, f"Missing columns in input file: {', '.join(missing_columns)}"
        
        # Create a new DataFrame with the required structure
        # Add +1 row for the header row in the output
        num_rows = len(input_df) + 1
        column_count = 35  # AI is the 34th column (0-indexed)
        output_df = pd.DataFrame("", index=range(num_rows), columns=range(column_count))
        
        # Map column names to indices (0-based) - shifted one to the left for all columns starting from U
        column_mapping = {
            18: "Size",                               # Column S
            20: "UOM",                                # Column U
            21: "Cartons",                            # Column V
            22: "CARTONS",                            # Column W
            23: "weight w/out add",                   # Column X
            24: "individual carton weight (add 2 lbs)", # Column Y
            25: "cube in cm",                         # Column Z
            26: "Length",                             # Column AA
            27: "Width",                              # Column AB
            28: "Height",                             # Column AC
            29: "dimension",                          # Column AD
            30: "cube in cft",                        # Column AE
            31: "total cubes",                        # Column AF
            32: "PALLET",                             # Column AG
            33: "FINAL CUBE",                         # Column AH
            34: "TOTAL WEIGHT"                        # Column AI
        }
        
        # Add column headers to the first row of the output
        for col_idx, col_name in column_mapping.items():
            output_df.iloc[0, col_idx] = col_name
        
        # Basic data mapping from input to output (without calculations)
        for i in range(len(input_df)):
            # Map input row i to output row i+1 (to account for header row)
            output_row = i + 1
            item_id = input_df.iloc[i][column_mappings["Item #"]]
            
            # Set the UOM value to "" for all rows - shifted to correct column
            if "Sequence 10: QTY" in column_mappings:
                output_df.iloc[output_row, 20] = input_df.iloc[i][column_mappings["Sequence 10: QTY"]]  # Use the QTY column
            else:
                output_df.iloc[output_row, 20] = ""  # Default to empty string if column not found
            
            # Copy basic values from input to output - shifted to correct columns
            output_df.iloc[output_row, 23] = input_df.iloc[i][column_mappings["Weight"]]     # weight w/out add
            output_df.iloc[output_row, 25] = input_df.iloc[i][column_mappings["Cube"]]       # cube in cm
            output_df.iloc[output_row, 26] = input_df.iloc[i][column_mappings["Length"]]     # Length
            output_df.iloc[output_row, 27] = input_df.iloc[i][column_mappings["Width"]]      # Width
            output_df.iloc[output_row, 28] = input_df.iloc[i][column_mappings["Height"]]     # Height
        
        # Save the DataFrame to the session directory with the output name
        output_path = os.path.join(session_dir, OUTPUT_CSV_NAME)
        output_df.to_csv(output_path, index=False, header=False)
        
        return True, "First CSV processed successfully"
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return False, str(e)

def process_second_csv(file_path, session_dir):
    """Process the second CSV file ('incoming 940').
    Extract the following columns:
    - Num
    - Ship Date
    - P. O. #
    - CANCEL DATE
    - Item
    - Qty
    - Ship To Address 1
    
    Create/update the CSV with the following column headers:
    - Column K (10) = "Order Date"
    - Column L (11) = "Customer" 
    - Column M (12) = "Ship to Name"
    - Column N (13) = "Start Date"
    - Column O (14) = "Cancel Date"
    - Column P (15) = "PO#"
    - Column Q (16) = "Item/Style"
    - Column R (17) = "INVOICE #"
    - Column T (19) = "TOTAL PIECES"
    
    Only rows from the second CSV are included in the final output.
    Item data from the first CSV is matched using Item # = Item/Style.
    """
    try:
        # Read second input file as DataFrame with all columns as strings
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".csv":
            input_df = pd.read_csv(file_path, dtype=str)
        elif ext in [".xlsx", ".xls"]:
            input_df = pd.read_excel(file_path, dtype=str)
        else:
            return False, "Unsupported file extension"
        
        # Try to find matching columns for required fields
        required_columns = ["Num", "Ship Date", "P. O. #", "CANCEL DATE", 
                           "Item", "Qty", "Ship To Address 1"]
        column_mappings = {}
        missing_columns = []
        
        print(f"Second CSV columns: {input_df.columns.tolist()}")
        
        # Find the best matches for each required column
        for col in required_columns:
            try:
                matched_col = find_matching_column(input_df, col)
                column_mappings[col] = matched_col
                print(f"Matched '{col}' to '{matched_col}'")
            except ValueError as e:
                missing_columns.append(col)
                print(f"Could not match column: {col}")
        
        if missing_columns:
            return False, f"Missing columns in second CSV: {', '.join(missing_columns)}"
        
        # Check if we have a master UOM file to use
        if os.path.exists(MASTER_UOM_FILE):
            first_csv_original_path = MASTER_UOM_FILE
            print(f"Using master UOM file: {MASTER_UOM_FILE}")
        else:
            # If no master file exists, look for an uploaded one
            first_csv_files = [f for f in os.listdir(session_dir) if f.endswith(('.csv', '.xlsx', '.xls')) 
                              and f != OUTPUT_CSV_NAME]
            
            if not first_csv_files:
                return False, "No UOM file found. Please upload a UOM file first or contact an administrator to set up the master UOM file."
            
            # Use the first file found as the first CSV
            first_csv_original_path = os.path.join(session_dir, first_csv_files[0])
            print(f"Using uploaded UOM file: {first_csv_original_path}")
        
        # Read the UOM file (either master or uploaded)
        ext = os.path.splitext(first_csv_original_path)[1].lower()
        if ext == ".csv":
            first_df_original = pd.read_csv(first_csv_original_path, dtype=str)
        elif ext in [".xlsx", ".xls"]:
            first_df_original = pd.read_excel(first_csv_original_path, dtype=str)
        else:
            return False, "Unsupported UOM file extension"
        
        # Try to find matching columns for required fields in first CSV
        required_columns_first = ["Item #", "Weight", "Cube", "Length", "Width", "Height", "Sequence 10: QTY"]
        first_csv_column_mappings = {}
        missing_columns_first = []
        
        # Find the best matches for each required column in first CSV
        for col in required_columns_first:
            try:
                matched_col = find_matching_column(first_df_original, col)
                first_csv_column_mappings[col] = matched_col
                print(f"First CSV matched '{col}' to '{matched_col}'")
            except ValueError as e:
                missing_columns_first.append(col)
                print(f"Could not match column in first CSV: {col}")
        
        if missing_columns_first:
            return False, f"Missing columns in UOM file: {', '.join(missing_columns_first)}"
        
        # Debug: Print first few rows of both dataframes to compare formats
        print("First CSV 'Item #' column (first 5 rows):")
        for i in range(min(5, len(first_df_original))):
            print(f"  Row {i}: '{first_df_original.iloc[i][first_csv_column_mappings['Item #']]}'")
        
        print("\nSecond CSV 'Item' column (first 5 rows):")
        for i in range(min(5, len(input_df))):
            print(f"  Row {i}: '{input_df.iloc[i][column_mappings['Item']]}'")
        
        # Function to normalize item values for matching
        def normalize_item(item_value):
            if not pd.isna(item_value):
                # Convert to string and strip whitespace
                item_str = str(item_value).strip()
                # Remove any special characters or formatting that might differ
                # Convert to uppercase for case-insensitive matching
                item_str = item_str.upper()
                # Replace common separators with empty string
                for char in ['-', '.', ' ', '_']:
                    item_str = item_str.replace(char, '')
                return item_str
            return ""
        
        # Create normalized versions of the item columns for both DataFrames
        first_df_original['normalized_item'] = first_df_original[first_csv_column_mappings['Item #']].apply(normalize_item)
        input_df['normalized_item'] = input_df[column_mappings['Item']].apply(normalize_item)
        
        # Create sets of normalized items from both DataFrames to check for common items
        first_items = set(first_df_original['normalized_item'].unique())
        second_items = set(input_df['normalized_item'].unique())
        common_items = first_items.intersection(second_items)
        
        print(f"\nNumber of unique normalized items in first CSV: {len(first_items)}")
        print(f"Number of unique normalized items in second CSV: {len(second_items)}")
        print(f"Number of common normalized items: {len(common_items)}")
        
        if len(common_items) > 0:
            print(f"Sample common items (up to 5): {list(common_items)[:5]}")
        
        # Create a dictionary to map item data from the first CSV using normalized Item # as key
        item_data = {}
        item_data_raw_keys = {}  # For debugging
        
        for _, row in first_df_original.iterrows():
            item_id = str(row[first_csv_column_mappings["Item #"]]).strip() if pd.notna(row[first_csv_column_mappings["Item #"]]) else ""
            normalized_id = normalize_item(item_id)
            
            if normalized_id:
                item_data[normalized_id] = {
                    "weight": row[first_csv_column_mappings["Weight"]],     # Using original Weight column 
                    "cube": row[first_csv_column_mappings["Cube"]],         # Using original Cube column
                    "length": row[first_csv_column_mappings["Length"]],     # Using original Length column
                    "width": row[first_csv_column_mappings["Width"]],       # Using original Width column
                    "height": row[first_csv_column_mappings["Height"]],     # Using original Height column
                    "qty": row[first_csv_column_mappings["Sequence 10: QTY"]] if "Sequence 10: QTY" in first_csv_column_mappings else "10"  # Use QTY from master or default to 10
                }
                item_data_raw_keys[item_id] = normalized_id  # Store mapping for debugging
        
        print(f"\nCreated item_data dictionary with {len(item_data)} entries")
        print(f"Sample normalized item keys: {list(item_data.keys())[:5]}")
        print(f"Sample raw to normalized key mappings: {dict(list(item_data_raw_keys.items())[:5])}")
        
        # Create a new DataFrame for the output with all required columns
        num_rows = len(input_df) + 1  # +1 for header row
        column_count = 35  # AI is the 34th column (0-indexed)
        output_df = pd.DataFrame("", index=range(num_rows), columns=range(column_count))
        
        # Define column mappings for all headers
        all_column_mapping = {
            10: "Order Date",                          # Column K
            11: "Customer",                            # Column L
            12: "Ship to Name",                        # Column M
            13: "Start Date",                          # Column N
            14: "Cancel Date",                         # Column O
            15: "PO#",                                 # Column P
            16: "Item/Style",                          # Column Q
            17: "INVOICE #",                           # Column R
            19: "TOTAL PIECES",                        # Column T
            18: "Size",                                # Column S
            20: "UOM",                                 # Column U
            21: "Cartons",                             # Column V
            22: "CARTONS",                             # Column W
            23: "weight w/out add",                    # Column X
            24: "individual carton weight (add 2 lbs)",# Column Y
            25: "cube in cm",                          # Column Z
            26: "Length",                              # Column AA
            27: "Width",                               # Column AB
            28: "Height",                              # Column AC
            29: "dimension",                           # Column AD
            30: "cube in cft",                         # Column AE
            31: "total cubes",                         # Column AF
            32: "PALLET",                              # Column AG
            33: "FINAL CUBE",                          # Column AH
            34: "TOTAL WEIGHT"                         # Column AI
        }
        
        # Set all column headers in the first row
        for col_idx, col_name in all_column_mapping.items():
            output_df.iloc[0, col_idx] = col_name
        
        # Define function to extract Ship to Name from Ship To Address 1 based on rules
        def extract_ship_to_name(address):
            if not isinstance(address, str):
                return ""
                
            address = address.strip().upper()
            
            # Apply special rules
            if "BURLINGTON" in address:
                return "BURLINGTON"
            elif "SAN BERNARDINO" in address:
                return "SAN BERNARDINO"
            elif "MARSHALLS" in address:
                return "MARSHALLS"
            elif "TJMAXX" in address or "TJ MAXX" in address or "T.J. MAXX" in address:
                return "T.J. MAXX"
            elif "DDS" in address or " DD " in address or address.startswith("DD ") or address.endswith(" DD") or address == "DD":
                return "DDs"
            elif "BEALLS" in address:
                return "BEALLS"
            elif "ROSS" in address:
                return "ROSS"
            elif "FASHION NOVA" in address or "FASHIONNOVA" in address:
                return "FASHION NOVA"
            else:
                # Extract first word (split by whitespace and take first element)
                first_word = address.split()[0] if address.split() else ""
                return first_word
        
        # Helper function to find closest matching item
        def find_closest_match(item_normalized, item_data_dict, item_df):
            # Try exact match first
            if item_normalized in item_data_dict:
                return item_data_dict[item_normalized], "exact"
            
            # Try partial match
            for first_item in item_df['normalized_item']:
                if not first_item:
                    continue
                    
                # If either is contained in the other
                if item_normalized in first_item or first_item in item_normalized:
                    return item_data_dict[first_item], "partial"
            
            # No match found
            return None, "none"
        
        # Helper function to safely convert string to float
        def safe_float(value, default=0.0):
            try:
                # Clean the value by removing commas and other non-numeric characters
                if isinstance(value, str):
                    # Remove commas and other thousands separators
                    value = value.replace(',', '')
                    # Replace other potential decimal separators
                    value = value.replace(' ', '')
                return float(value) if pd.notna(value) and value != "" else default
            except (ValueError, TypeError):
                print(f"Failed to convert '{value}' to float, using default {default}")
                return default
        
        # Debug counter for matched/unmatched items
        exact_matches = 0
        partial_matches = 0
        unmatched_items = 0
        
        # Process each row of input data and map to output
        for i, row in input_df.iterrows():
            # Calculate the corresponding row in output DataFrame (add 1 for header row)
            output_row = i + 1
            
            # Fill in data for each mapped column from step 2
            output_df.iloc[output_row, 11] = "SENSUAL"  # Customer is always "SENSUAL"
            output_df.iloc[output_row, 12] = extract_ship_to_name(row[column_mappings["Ship To Address 1"]])
            output_df.iloc[output_row, 13] = row[column_mappings["Ship Date"]]
            output_df.iloc[output_row, 14] = row[column_mappings["CANCEL DATE"]]
            output_df.iloc[output_row, 15] = row[column_mappings["P. O. #"]]
            output_df.iloc[output_row, 16] = row[column_mappings["Item"]]  # Item/Style
            output_df.iloc[output_row, 17] = row[column_mappings["Num"]]   # INVOICE #
            output_df.iloc[output_row, 19] = row[column_mappings["Qty"]]   # TOTAL PIECES
            
            # Set UOM to "" for all rows (empty string instead of "10")
            output_df.iloc[output_row, 20] = ""  # Default value if no match found
            
            # Debug print for Qty values
            if i < 5:  # Print first 5 rows for debugging
                print(f"Row {i}, Qty: '{row[column_mappings['Qty']]}', type: {type(row[column_mappings['Qty']])}")
            
            # Look up and add item data from first CSV if available
            # Use normalized keys for matching
            item_key_raw = str(row[column_mappings["Item"]]).strip() if pd.notna(row[column_mappings["Item"]]) else ""
            item_key_normalized = normalize_item(item_key_raw)
            
            # Find best match - either exact or partial
            item_data_match, match_type = find_closest_match(item_key_normalized, item_data, first_df_original)
            
            if item_data_match:
                if match_type == "exact":
                    exact_matches += 1
                else:
                    partial_matches += 1
                    
                # Use the matched item data
                output_df.iloc[output_row, 20] = item_data_match["qty"]     # UOM from Sequence 10: QTY column
                output_df.iloc[output_row, 23] = item_data_match["weight"]     # weight w/out add (from Weight)
                output_df.iloc[output_row, 25] = item_data_match["cube"]       # cube in cm (from Cube)
                output_df.iloc[output_row, 26] = item_data_match["length"]     # Length
                output_df.iloc[output_row, 27] = item_data_match["width"]      # Width
                output_df.iloc[output_row, 28] = item_data_match["height"]     # Height
                
                # Calculate derived columns
                total_pieces = safe_float(row[column_mappings["Qty"]])
                uom = safe_float(output_df.iloc[output_row, 20])
                weight_wo_add = safe_float(item_data_match["weight"])
                length = safe_float(item_data_match["length"])
                width = safe_float(item_data_match["width"])
                height = safe_float(item_data_match["height"])
                
                # Debug print for calculation values
                if i < 5:  # Print first 5 rows for debugging
                    print(f"  Calculation - Row {i}: total_pieces={total_pieces}, uom={uom}")
                
                # Calculate Cartons = total pieces / UOM
                cartons = total_pieces / uom if uom > 0 else 0
                output_df.iloc[output_row, 21] = f"{cartons:.2f}"
                
                # Calculate CARTONS = rounded up Cartons
                cartons_rounded = math.ceil(cartons)
                output_df.iloc[output_row, 22] = str(cartons_rounded)
                
                # Calculate individual carton weight = weight w/out add + 2
                individual_weight = weight_wo_add + 2
                output_df.iloc[output_row, 24] = f"{individual_weight:.2f}"
                
                # Calculate dimension = (L * W * H) / 1728
                dimension = (length * width * height) / 1728 if (length > 0 and width > 0 and height > 0) else 0
                output_df.iloc[output_row, 29] = f"{dimension:.2f}"
                
                # Calculate cube in cft = cube in cm / 1728 + 0.3
                cube_in_cm = safe_float(item_data_match["cube"])
                cube_in_cft = (cube_in_cm / 1728) + 0.3
                output_df.iloc[output_row, 30] = f"{cube_in_cft:.2f}"
                
                # Calculate total cubes = cube in cft * CARTONS (using rounded up value)
                total_cubes = cube_in_cft * cartons_rounded
                output_df.iloc[output_row, 31] = f"{total_cubes:.2f}"
                
                # Calculate PALLET = ceil(total_cubes / 65)
                # 0-64 = 1 pallet, 65-129 = 2 pallets, etc.
                pallets = math.ceil(total_cubes / 65) if total_cubes > 0 else 1
                output_df.iloc[output_row, 32] = str(pallets)
                
                # Calculate FINAL CUBE = PALLET * 130
                final_cube = pallets * 130
                output_df.iloc[output_row, 33] = str(final_cube)
                
                # Calculate TOTAL WEIGHT = (CARTONS * individual carton weight) + (PALLET * 40)
                total_weight = (cartons_rounded * individual_weight) + (pallets * 40)
                output_df.iloc[output_row, 34] = f"{total_weight:.2f}"
                
            else:
                unmatched_items += 1
                if i < 10:  # Only print first 10 unmatched items to avoid flooding logs
                    print(f"Unmatched item: '{item_key_raw}' (normalized: '{item_key_normalized}')")
        
        print(f"Item matching: {exact_matches} exact matches, {partial_matches} partial matches, {unmatched_items} unmatched")
        
        # Save the DataFrame to the session directory with the output name
        output_path = os.path.join(session_dir, OUTPUT_CSV_NAME)
        output_df.to_csv(output_path, index=False, header=False)
        
        return True, f"Second CSV processed and merged successfully. Exact matches: {exact_matches}, Partial matches: {partial_matches}, Unmatched: {unmatched_items}"
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return False, str(e)

def compute_pallet(bol_cube):
    try:
        return str(math.ceil(float(bol_cube) / 40)) if bol_cube else ""
    except (ValueError, TypeError):
        return ""

def compute_burlington(ship_to_name, pallet):
    if ship_to_name and "BURLINGTON" in ship_to_name.upper():
        return pallet
    return ""

def compute_final_cube(ship_to_name, pallet):
    if ship_to_name and "BURLINGTON" not in ship_to_name.upper():
        return pallet
    return ""
        
def cleanup_old_files():
    """Clean up old processing sessions."""
    import shutil
    from datetime import datetime, timedelta
    
    session_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "processing_sessions")
    if not os.path.exists(session_dir):
        return
        
    now = datetime.now()
    for item in os.listdir(session_dir):
        item_path = os.path.join(session_dir, item)
        if os.path.isdir(item_path):
            try:
                # Check if dir is older than 24 hours
                created_time = datetime.fromtimestamp(os.path.getctime(item_path))
                if now - created_time > timedelta(hours=24):
                    shutil.rmtree(item_path)
                    print(f"Cleaned up old session: {item}")
            except Exception as e:
                print(f"Error cleaning up {item}: {e}")

def get_or_create_session():
    """Get existing session ID or create a new one."""
    cleanup_old_files()  # First clean up old files
    import uuid
    
    # Create the session directory if it doesn't exist
    session_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "processing_sessions")
    if not os.path.exists(session_root):
        os.makedirs(session_root)
    
    # If we don't have a session ID yet, create one
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())
        
    # Create session directory if it doesn't exist
    session_dir = os.path.join(session_root, session['session_id'])
    if not os.path.exists(session_dir):
        os.makedirs(session_dir)
        
    return session_dir

@app.route('/', methods=['GET'])
def index():
    # Get or create session without cleaning up existing valid sessions
    get_or_create_session()
    
    # Check if master UOM file exists to determine if we should show Step 1
    master_file_exists = os.path.exists(MASTER_UOM_FILE)
    
    return render_template('index.html', master_file_exists=master_file_exists)

@app.route('/process', methods=['POST'])
def process():
    # Use existing session instead of creating new one
    session_dir = get_or_create_session()
    
    # Process through pipeline
    success, message = process_first_csv(session_dir)
    
    # Return result
    if success:
        return jsonify({"status": "success", "message": message})
    else:
        return jsonify({"status": "error", "error": message}), 500

@app.route('/health')
def health():
    # Simple health check endpoint
    try:
        # Check if we can create a session directory
        session_dir = get_or_create_session()
        return jsonify({"status": "ok", "message": "Service is healthy", "session_dir": session_dir})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/upload-first-csv', methods=['POST'])
def upload_first_csv():
    # Get existing session directory
    session_dir = get_or_create_session()
    
    # Check if the post request has the file part
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
        
    file = request.files['file']
    
    # If user does not select file, browser submits an empty file without filename
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
        
    if file and allowed_file(file.filename, ALLOWED_CSV_EXTENSIONS):
        filename = secure_filename(file.filename)
        file_path = os.path.join(session_dir, filename)
        file.save(file_path)
        
        # Process the first CSV file
        success, message = process_first_csv(file_path, session_dir)
        
        if success:
            return jsonify({"status": "success", "message": "First CSV processed successfully"})
        else:
            return jsonify({"error": message}), 500
    else:
        return jsonify({"error": "Invalid file type. Please upload a CSV or Excel file."}), 400

@app.route('/upload-second-csv', methods=['POST'])
def upload_second_csv():
    # Get existing session directory
    session_dir = get_or_create_session()
    
    # Check if the post request has the file part
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
        
    file = request.files['file']
    
    # If user does not select file, browser submits an empty file without filename
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
        
    if file and allowed_file(file.filename, ALLOWED_CSV_EXTENSIONS):
        filename = secure_filename(file.filename)
        file_path = os.path.join(session_dir, filename)
        file.save(file_path)
        
        # Process the second CSV file - this will now use the master UOM file if available
        success, message = process_second_csv(file_path, session_dir)
        
        if success:
            return jsonify({"status": "success", "message": message})
        else:
            return jsonify({"error": message}), 500
    else:
        return jsonify({"error": "Invalid file type. Please upload a CSV or Excel file."}), 400

@app.route('/download')
def download_file():
    # First try with the current session directory
    session_dir = get_or_create_session()
    
    file_path = os.path.join(session_dir, OUTPUT_CSV_NAME)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True, download_name=OUTPUT_CSV_NAME)
    
    # If not found, look in all recent session directories (within the last hour)
    # This is a fallback for iframe issues where session might be lost
    session_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "processing_sessions")
    if os.path.exists(session_root):
        # Check for processed files in recent sessions
        from datetime import datetime, timedelta
        now = datetime.now()
        one_hour_ago = now - timedelta(hours=1)
        
        # Sort directories by creation time, newest first
        session_dirs = []
        for item in os.listdir(session_root):
            item_path = os.path.join(session_root, item)
            if os.path.isdir(item_path):
                created_time = datetime.fromtimestamp(os.path.getctime(item_path))
                if now - created_time < timedelta(hours=1):  # Only check recent sessions
                    session_dirs.append((item_path, created_time))
        
        # Sort by creation time, newest first
        session_dirs.sort(key=lambda x: x[1], reverse=True)
        
        # Check each session directory for the output file
        for session_dir_path, _ in session_dirs:
            check_file = os.path.join(session_dir_path, OUTPUT_CSV_NAME)
            if os.path.exists(check_file):
                print(f"Found file in alternate session directory: {session_dir_path}")
                return send_file(check_file, as_attachment=True, download_name=OUTPUT_CSV_NAME)
    
    # If still not found, return error
    return jsonify({"error": "File not found. Please process your data first."}), 404

@app.after_request
def after_request(response):
    # Get the request origin
    origin = request.headers.get('Origin', '*')
    
    # Allow the specific origin that made the request instead of wildcard
    response.headers.add('Access-Control-Allow-Origin', origin)
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    
    # Vary the response based on the Origin header
    response.headers.add('Vary', 'Origin')
    return response

# Add new admin routes for managing the master UOM file
@app.route('/admin/upload-master-uom', methods=['POST'])
def upload_master_uom():
    """Admin endpoint to upload and update the master UOM file"""
    
    # Check if the post request has the file part
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
        
    file = request.files['file']
    
    # If user does not select file, browser submits an empty file without filename
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
        
    if file and allowed_file(file.filename, ALLOWED_CSV_EXTENSIONS):
        # Backup the existing master file if it exists
        if os.path.exists(MASTER_UOM_FILE):
            backup_file = f"{MASTER_UOM_FILE}.bak"
            shutil.copy2(MASTER_UOM_FILE, backup_file)
            
        # Save the new master file
        file.save(MASTER_UOM_FILE)
        
        # Verify the file can be read and has the required columns
        try:
            ext = os.path.splitext(MASTER_UOM_FILE)[1].lower()
            if ext == ".csv":
                df = pd.read_csv(MASTER_UOM_FILE, dtype=str)
            elif ext in [".xlsx", ".xls"]:
                df = pd.read_excel(MASTER_UOM_FILE, dtype=str)
                
            required_columns = ["Item #", "Weight", "Cube", "Length", "Width", "Height"]
            missing_columns = []
            
            for col in required_columns:
                try:
                    matched_col = find_matching_column(df, col, required=True)
                except ValueError:
                    missing_columns.append(col)
            
            if missing_columns:
                # Restore from backup if validation fails
                if os.path.exists(f"{MASTER_UOM_FILE}.bak"):
                    shutil.copy2(f"{MASTER_UOM_FILE}.bak", MASTER_UOM_FILE)
                return jsonify({"error": f"Uploaded file is missing required columns: {', '.join(missing_columns)}"}), 400
                
            return jsonify({"status": "success", "message": "Master UOM file updated successfully"})
            
        except Exception as e:
            # Restore from backup if validation fails
            if os.path.exists(f"{MASTER_UOM_FILE}.bak"):
                shutil.copy2(f"{MASTER_UOM_FILE}.bak", MASTER_UOM_FILE)
            return jsonify({"error": f"Error validating uploaded file: {str(e)}"}), 500
    else:
        return jsonify({"error": "Invalid file type. Please upload a CSV or Excel file."}), 400

@app.route('/admin/download-master-uom')
def download_master_uom():
    """Admin endpoint to download the current master UOM file"""
    if os.path.exists(MASTER_UOM_FILE):
        return send_file(MASTER_UOM_FILE, as_attachment=True, download_name="master_uom.csv")
    else:
        return jsonify({"error": "Master UOM file not found. Please upload one first."}), 404

@app.route('/admin')
def admin_page():
    """Admin page for managing the master UOM file"""
    # Check if master UOM file exists
    master_file_exists = os.path.exists(MASTER_UOM_FILE)
    return render_template('admin.html', master_file_exists=master_file_exists)

if __name__ == "__main__":
    # Get port from environment variable (Render sets this) or default to 5000
    port = int(os.environ.get('PORT', 5500))
    # Bind to 0.0.0.0 to accept connections from any host (required for Render)
    app.run(host='0.0.0.0', port=port, debug=False)