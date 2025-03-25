import os
import csv
import math
from pathlib import Path
import platform
import pandas as pd
from io import StringIO
from flask import Flask, render_template, request, send_file, jsonify, session
from werkzeug.utils import secure_filename
from pdf_processor import PDFProcessor
from data_processor import DataProcessor
from csv_exporter import CSVExporter
from config import OUTPUT_CSV_NAME  # e.g. "combined_data.csv"

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = os.path.dirname(os.path.abspath(__file__))
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['SESSION_COOKIE_SAMESITE'] = 'None'  # Allow cookie in iframe
app.config['SESSION_COOKIE_SECURE'] = True  # Required for SameSite=None
app.secret_key = 'your-secret-key-here'  # Required for session management

# Allowed extensions for PDF upload
ALLOWED_PDF_EXTENSIONS = {'pdf'}
# Allowed extensions for CSV/XLSX upload
ALLOWED_CSV_EXTENSIONS = {'csv', 'xlsx', 'xls'}

# Check if poppler is installed or install it if on Render
if os.environ.get('RENDER') and platform.system() != 'Windows':
    try:
        # Try to use poppler
        from pdf2image import convert_from_path
        test_pdf = Path(__file__).parent / "test.pdf"
        if not test_pdf.exists():
            # Create a valid test file with actual content
            with open(test_pdf, "wb") as f:
                # This is a minimal but valid PDF with one page
                f.write(b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj 2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj 3 0 obj<</Type/Page/MediaBox[0 0 3 3]/Parent 2 0 R/Resources<<>>>>endobj\nxref\n0 4\n0000000000 65535 f\n0000000010 00000 n\n0000000053 00000 n\n0000000102 00000 n\ntrailer<</Size 4/Root 1 0 R>>\nstartxref\n180\n%%EOF\n")
        
        # Test if poppler works
        pages = convert_from_path(str(test_pdf), dpi=72)
        print(f"Poppler working correctly. Detected {len(pages)} pages.")
    except Exception as e:
        print(f"Error with poppler: {e}")
        print("Poppler not available, functionality will be limited")


def allowed_file(filename, allowed_set):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_set

def process_pdf():
    """Process the PDF file through our pipeline."""
    try:
        # Initialize processors
        pdf_processor = PDFProcessor()
        data_processor = DataProcessor()
        csv_exporter = CSVExporter()
        
        # Process through pipeline
        if not pdf_processor.process_first_pdf():
            return False, "Failed to process PDF"
            
        if not data_processor.process_all_files():
            return False, "Failed to process text files"
            
        if not csv_exporter.combine_to_csv():
            return False, "Failed to create CSV file"
            
        return True, "Processing completed successfully"
        
    except Exception as e:
        return False, str(e)

def process_csv_file(file_path, session_dir):
    """Process and merge incoming CSV/Excel data with the PDF CSV by matching on:
       - Invoice No.
       - Style
       - Cartons* (renamed to 'Cartons')
       - Pieces* (renamed to 'Individual Pieces')
       
       Then update the following fields using the incoming headers:
       - "Invoice Date" -> "Order Date"
       - "Ship-to Name" -> "Ship To Name"
       - "Order No." -> "Purchase Order No."
       - "Delivery Date" -> "Start Date"
       - "Cancel Date" -> "Cancel Date"
    """
    try:
        # Read input file as DataFrame with all columns as strings
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".csv":
            incoming_df = pd.read_csv(file_path, dtype=str)
        elif ext in [".xlsx", ".xls"]:
            incoming_df = pd.read_excel(file_path, dtype=str)
        else:
            return False, "Unsupported file extension"
        
        # Rename incoming columns used for matching.
        incoming_df.rename(columns={"Cartons*": "Cartons", "Pieces*": "Individual Pieces"}, inplace=True)
        
        # Define mapping for additional fields using header names:
        additional_mapping = {
            "Invoice Date": "Order Date",
            "Ship-to Name": "Ship To Name",
            "Order No.": "Purchase Order No.",
            "Delivery Date": "Start Date",
            "Cancel Date": "Cancel Date"
        }
        
        # Read existing combined CSV (from PDF processing) from session directory
        combined_csv_path = os.path.join(session_dir, OUTPUT_CSV_NAME)
        if not os.path.exists(combined_csv_path):
            return False, "No PDF data processed yet. Please process PDF first."
        existing_df = pd.read_csv(combined_csv_path, dtype=str)
        
        # Ensure matching columns exist in both DataFrames.
        matching_columns = ["Invoice No.", "Style", "Cartons", "Individual Pieces"]
        for col in matching_columns:
            if col not in existing_df.columns:
                return False, f"Column '{col}' not found in PDF CSV data."
            if col not in incoming_df.columns:
                return False, f"Column '{col}' not found in incoming file."
        
        # Create a composite match key in both DataFrames.
        def create_match_key(df, cols):
            return df[cols].fillna('').apply(
                lambda row: "_".join([str(x).strip().replace(",", "").lower() for x in row]),
                axis=1
            )
        
        key_cols = matching_columns
        existing_df["match_key"] = create_match_key(existing_df, key_cols)
        incoming_df["match_key"] = create_match_key(incoming_df, key_cols)
        
        print("Existing DataFrame match keys:")
        print(existing_df[["Invoice No.", "Style", "Cartons", "Individual Pieces", "match_key"]].head(20))
        print("Incoming DataFrame match keys:")
        print(incoming_df[["Invoice No.", "Style", "Cartons", "Individual Pieces", "match_key"]].head(20))
        
        # Merge: update existing_df rows using incoming additional mapping.
        for idx, inc_row in incoming_df.iterrows():
            key = inc_row["match_key"]
            matches = existing_df[existing_df["match_key"] == key]
            if not matches.empty:
                existing_index = matches.index[0]
                for inc_col, pdf_col in additional_mapping.items():
                    if inc_col in incoming_df.columns and pdf_col in existing_df.columns:
                        value = inc_row.get(inc_col, "")
                        existing_df.at[existing_index, pdf_col] = value
        
        # Drop the match_key columns.
        existing_df.drop(columns=["match_key"], inplace=True)
        incoming_df.drop(columns=["match_key"], inplace=True)
        
        # Compute values for all rows first
        if "BOL Cube" in existing_df.columns:
            pallet_values = existing_df["BOL Cube"].apply(lambda x: compute_pallet(x))
            existing_df["Pallet"] = ""  # Initialize empty column
        else:
            print("Warning: 'BOL Cube' column not found in existing CSV data.")
            pallet_values = pd.Series([""] * len(existing_df))
        
        if "Ship To Name" in existing_df.columns:
            burlington_values = existing_df.apply(
                lambda row: compute_burlington(row["Ship To Name"], pallet_values.iloc[row.name]), 
                axis=1
            )
            final_cube_values = existing_df.apply(
                lambda row: compute_final_cube(row["Ship To Name"], pallet_values.iloc[row.name]), 
                axis=1
            )
            
            existing_df["Burlington Cube"] = ""  # Initialize empty column
            existing_df["Final Cube"] = ""      # Initialize empty column
        else:
            print("Warning: 'Ship To Name' column not found in existing CSV data.")
            burlington_values = pd.Series([""] * len(existing_df))
            final_cube_values = pd.Series([""] * len(existing_df))
        
        # Group by Invoice No. and only set values for first row of each group
        current_invoice = None
        is_first_row = True
        
        for idx in range(len(existing_df)):
            invoice_no = existing_df.iloc[idx]["Invoice No."]
            
            # Check if this is the first row of a new invoice group
            if invoice_no != current_invoice:
                current_invoice = invoice_no
                is_first_row = True
            
            # Only set the values for the first row of each invoice group
            if is_first_row:
                existing_df.iloc[idx, existing_df.columns.get_loc("Pallet")] = pallet_values.iloc[idx]
                existing_df.iloc[idx, existing_df.columns.get_loc("Burlington Cube")] = burlington_values.iloc[idx]
                existing_df.iloc[idx, existing_df.columns.get_loc("Final Cube")] = final_cube_values.iloc[idx]
                is_first_row = False
            
        def parse_cancel_date(date_str):
            """
            Convert a string like '3152025' -> 03/15/2025 or
            '2202025' -> 02/20/2025 into a datetime object.

            Handles:
            - 7-digit format:  MDDYYYY  (e.g. '3152025')
            - 8-digit format: MMDDYYYY  (e.g. '03152025')
            """
            date_str = str(date_str).strip()

            # 7-digit: MDDYYYY
            if len(date_str) == 7:
                month = date_str[0]             # e.g. '3'
                day   = date_str[1:3]          # e.g. '15'
                year  = date_str[3:]           # e.g. '2025'
                try:
                    return pd.to_datetime(f"{month.zfill(2)}/{day}/{year}", format="%m/%d/%Y")
                except:
                    return pd.NaT

            # 8-digit: MMDDYYYY
            elif len(date_str) == 8:
                month = date_str[0:2]          # e.g. '03'
                day   = date_str[2:4]          # e.g. '15'
                year  = date_str[4:]           # e.g. '2025'
                try:
                    return pd.to_datetime(f"{month}/{day}/{year}", format="%m/%d/%Y")
                except:
                    return pd.NaT

            return pd.NaT

        # --- Sorting the output ---
        if "Cancel Date" in existing_df.columns and "Ship To Name" in existing_df.columns:
            # Convert the raw strings in "Cancel Date" to datetime using the custom function:
            existing_df["Cancel Date_dt"] = existing_df["Cancel Date"].apply(parse_cancel_date)

            # Compute the earliest date per "Ship To Name":
            existing_df["min_cancel_date"] = existing_df.groupby("Ship To Name")["Cancel Date_dt"].transform("min")

            # Sort by earliest group date, then Ship To Name, then the individual date:
            existing_df.sort_values(by=["min_cancel_date", "Ship To Name", "Cancel Date_dt"], inplace=True)

            # Drop the helper columns:
            existing_df.drop(columns=["min_cancel_date", "Cancel Date_dt"], inplace=True)

        else:
            print("Warning: 'Cancel Date' or 'Ship To Name' column not found; skipping sort.")
        
        # Save updated DataFrame back to the combined CSV in session directory
        existing_df.to_csv(combined_csv_path, index=False)
        
        return True, f"CSV data merged successfully (processed {len(incoming_df)} rows)"
        
    except pd.errors.EmptyDataError:
        return False, "The uploaded file is empty"
    except pd.errors.ParserError:
        return False, "Error parsing the file. Please ensure it's a valid CSV/Excel file"
    except Exception as e:
        print(f"Error processing CSV: {str(e)}")
        return False, f"Error processing file: {str(e)}"

def compute_pallet(bol_cube):
    """Compute pallet value from BOL Cube."""
    try:
        value = float(str(bol_cube).replace(",", "").strip())
        return math.ceil(value / 80)
    except Exception:
        return ""

def compute_burlington(ship_to_name, pallet):
    """Compute Burlington Cube value."""
    try:
        if isinstance(ship_to_name, str) and "burlington" in ship_to_name.lower():
            if pd.isna(pallet) or pallet == "":
                return ""
            return int(pallet) * 93
    except Exception:
        return ""
    return ""

def compute_final_cube(ship_to_name, pallet):
    """Compute Final Cube value."""
    try:
        if isinstance(ship_to_name, str) and "burlington" not in ship_to_name.lower():
            if pd.isna(pallet) or pallet == "":
                return ""
            return int(pallet) * 130
    except Exception:
        return ""
    return ""

def cleanup_old_files():
    """Clean up old PDFs and combined CSV file when page is loaded/refreshed."""
    try:
        script_dir = app.config['UPLOAD_FOLDER']
        
        # Delete old PDFs
        for file in os.listdir(script_dir):
            if file.lower().endswith('.pdf'):
                try:
                    os.remove(os.path.join(script_dir, file))
                    print(f"Cleaned up old PDF: {file}")
                except Exception as e:
                    print(f"Error deleting PDF {file}: {str(e)}")
        
        # Delete old combined CSV
        combined_csv = os.path.join(script_dir, OUTPUT_CSV_NAME)
        if os.path.exists(combined_csv):
            try:
                os.remove(combined_csv)
                print(f"Cleaned up old combined CSV: {OUTPUT_CSV_NAME}")
            except Exception as e:
                print(f"Error deleting combined CSV: {str(e)}")
                
    except Exception as e:
        print(f"Error during cleanup: {str(e)}")

def get_or_create_session():
    """Get existing processor or create new one with session management."""
    if 'session_id' not in session:
        # Only clean up old sessions when creating a new one AND no session exists
        base_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'processing_sessions')
        if os.path.exists(base_dir):
            # Check if there are any existing sessions with combined_data.csv
            existing_sessions = []
            for session_dir in os.listdir(base_dir):
                full_path = os.path.join(base_dir, session_dir)
                if os.path.isdir(full_path):
                    csv_path = os.path.join(full_path, OUTPUT_CSV_NAME)
                    if os.path.exists(csv_path):
                        existing_sessions.append(session_dir)
            
            if existing_sessions:
                # Use the most recent session that has a combined_data.csv
                most_recent = sorted(existing_sessions)[-1]
                session['session_id'] = most_recent
                processor = DataProcessor(session_id=most_recent)
                print(f"Reusing existing session with data: {most_recent}")
                return processor
            else:
                # No valid sessions found, clean up and create new
                DataProcessor.cleanup_sessions()
        
        # Create new session if none exists or no valid sessions found
        processor = DataProcessor()
        session['session_id'] = processor.session_id
        print(f"Created new session: {processor.session_id}")
    else:
        processor = DataProcessor(session_id=session['session_id'])
        print(f"Using existing session: {session['session_id']}")
        print(f"Using existing session: {session['session_id']}")
    return processor

@app.route('/', methods=['GET'])
def index():
    # Get or create session without cleaning up existing valid sessions
    processor = get_or_create_session()
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    # Use existing session instead of creating new one
    processor = get_or_create_session()
    
    # Process the files
    processor.process_all_files()
    
    # Create exporter with the same session directory
    exporter = CSVExporter(session_dir=processor.session_dir)
    exporter.combine_to_csv()
    
    return jsonify({"status": "success"})

# Add near your other routes
@app.route('/health')
def health():
    # Check if poppler is working
    poppler_status = "working" if os.environ.get('POPPLER_WORKING') else "not working"
    
    return jsonify({
        "status": "healthy",
        "poppler_status": poppler_status,
        "environment": os.environ.get('RENDER', 'local')
    }), 200

@app.route('/upload', methods=['POST'])
def upload_file():
    # Get existing processor with session directory
    processor = get_or_create_session()
    
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
        
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
        
    if not allowed_file(file.filename, ALLOWED_PDF_EXTENSIONS):
        return jsonify({'error': 'Invalid file type (PDF required)'}), 400
        
    try:
        # Save the uploaded PDF directly to session directory
        filename = secure_filename(file.filename)
        file_path = os.path.join(processor.session_dir, filename)
        file.save(file_path)
        
        # Process the PDF through our pipeline
        pdf_processor = PDFProcessor(session_dir=processor.session_dir)
        if not pdf_processor.process_first_pdf():
            return jsonify({'error': 'Failed to process PDF'}), 500
            
        if not processor.process_all_files():
            return jsonify({'error': 'Failed to process text files'}), 500
            
        # Create exporter with the same session directory
        exporter = CSVExporter(session_dir=processor.session_dir)
        if not exporter.combine_to_csv():
            return jsonify({'error': 'Failed to create CSV file'}), 500
            
        return jsonify({'message': 'File processed successfully'}), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    try:
        # Get existing processor with session directory
        processor = get_or_create_session()
        
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
            
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'}), 400
            
        if not allowed_file(file.filename, ALLOWED_CSV_EXTENSIONS):
            return jsonify({'error': 'Invalid file type. Please upload a CSV or Excel file'}), 400
            
        # Save uploaded file to session directory
        filename = secure_filename(f"temp_{file.filename}")
        file_path = os.path.join(processor.session_dir, filename)
        
        try:
            file.save(file_path)
            success, message = process_csv_file(file_path, processor.session_dir)
            
            if not success:
                return jsonify({'error': message}), 400
                
            return jsonify({
                'message': 'CSV data mapped successfully',
                'status': 'success'
            }), 200
            
        finally:
            # Clean up temporary file
            if os.path.exists(file_path):
                os.remove(file_path)
                
    except Exception as e:
        print(f"Upload error: {str(e)}")  # For debugging
        return jsonify({'error': 'An unexpected error occurred'}), 500

@app.route('/download')
def download_file():
    try:
        # Get existing processor with session directory
        processor = get_or_create_session()
        csv_path = os.path.join(processor.session_dir, OUTPUT_CSV_NAME)
        return send_file(csv_path, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.after_request
def after_request(response):
    """Add headers to allow iframe embedding and CORS."""
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
    response.headers.add('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
    return response

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)