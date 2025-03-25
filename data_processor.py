import os
import re
import csv
import io
import uuid
import shutil
from datetime import datetime
from utils import FileUtils  # Removed OpenAI dependency
import gc

class DataProcessor:
    def __init__(self, session_id=None):
        """Initialize the data processor with a session directory."""
        self.base_dir = FileUtils.get_script_dir()
        self.session_id = session_id or self._generate_session_id()
        self.session_dir = os.path.join(self.base_dir, 'processing_sessions', self.session_id)
        self.invoice_data = {}  # Store data for multi-page invoices
        self._setup_session_directory()

    def _generate_session_id(self):
        """Generate a unique session ID using timestamp and UUID."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        return f"session_{timestamp}_{unique_id}"

    def _setup_session_directory(self):
        """Create the session directory if it doesn't exist."""
        os.makedirs(self.session_dir, exist_ok=True)
        print(f"Created session directory: {self.session_dir}")

    @staticmethod
    def cleanup_sessions():
        """Clean up all processing session directories."""
        base_dir = FileUtils.get_script_dir()
        sessions_dir = os.path.join(base_dir, 'processing_sessions')
        if os.path.exists(sessions_dir):
            try:
                shutil.rmtree(sessions_dir)
                print("Cleaned up all processing sessions")
            except Exception as e:
                print(f"Error cleaning up sessions: {str(e)}")

    def process_all_files(self):
        """Process all TXT files in the session directory."""
        # Get all txt files except requirements.txt
        txt_files = [f for f in FileUtils.get_txt_files(self.session_dir) if f != 'requirements.txt']
        if not txt_files:
            print("No TXT files found in the session directory")
            return False

        print(f"Found {len(txt_files)} TXT files to process")
        
        try:
            # Process files in smaller batches to conserve memory
            batch_size = 10
            for i in range(0, len(txt_files), batch_size):
                batch = txt_files[i:i + batch_size]
                print(f"Processing batch {i//batch_size + 1} of {(len(txt_files) + batch_size - 1)//batch_size}")
                
                # First pass: Collect data for this batch
                for txt_file in batch:
                    self._collect_invoice_data(txt_file)
                
                # Second pass: Process collected data for this batch
                for invoice_no, pages_data in self.invoice_data.items():
                    self._process_invoice_data(invoice_no, pages_data)
                
                # Clear processed data from memory
                self.invoice_data.clear()
                gc.collect()
            
            return True
            
        except Exception as e:
            print(f"Error processing files: {str(e)}")
            return False

    def _collect_invoice_data(self, txt_file):
        """Collect data from a single TXT file and group by invoice number."""
        file_path = os.path.join(self.session_dir, txt_file)
        print(f"Collecting data from {txt_file}...")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()

            invoice_no = self._get_invoice_no(content)
            if not invoice_no:
                print(f"Invoice number not found in {txt_file}")
                return

            # Initialize invoice data if not exists
            if invoice_no not in self.invoice_data:
                self.invoice_data[invoice_no] = {
                    'pages': [],
                    'has_totals': False
                }

            # Extract table rows and check for totals
            table_data = self._extract_table_data(content)
            if table_data:
                rows, has_totals, totals = table_data
                page_data = {
                    'rows': rows,  # Don't store full content, just extracted data
                    'has_totals': has_totals,
                    'totals': totals,
                    'bol_cube': self._extract_bol_cube(content)
                }
                self.invoice_data[invoice_no]['pages'].append(page_data)
                if has_totals:
                    self.invoice_data[invoice_no]['has_totals'] = True

            # Delete the processed txt file
            os.remove(file_path)
            
        except Exception as e:
            print(f"Error collecting data from {txt_file}: {str(e)}")
        
        # Force garbage collection
        gc.collect()

    def _extract_table_data(self, content):
        """Extract table rows and totals from content."""
        lines = content.splitlines()
        table_start = None
        rows = []
        has_totals = False
        totals = {'pieces': '', 'weight': ''}

        # Find table start
        for i, line in enumerate(lines):
            if "CARTONS" in line.upper() and "STYLE" in line.upper() and "PIECES" in line.upper():
                table_start = i
                break

        if table_start is None:
            return None

        # Process rows and look for totals
        for line in lines[table_start+1:]:
            if "TOTAL CARTONS" in line.upper():
                has_totals = True
                tokens = line.split()
                if len(tokens) >= 11:
                    totals['pieces'] = tokens[3].replace(',', '')
                    totals['weight'] = tokens[-1].replace(',', '')
                break
            if "SHIPPING INSTRUCTIONS:" in line.upper():
                break
            if not line.strip():
                continue
            if re.match(r'^\d+', line.strip()):
                tokens = line.split()
                if len(tokens) >= 3:
                    cartons = tokens[0].replace(',', '')
                    style = tokens[1]
                    individual_pieces = tokens[2].replace(',', '')
                    individual_weight = tokens[-1].replace(',', '')
                    rows.append([cartons, individual_pieces, individual_weight, style])

        return rows, has_totals, totals

    def _extract_bol_cube(self, content):
        """Extract BOL Cube from content."""
        lines = content.splitlines()
        for i, line in enumerate(lines):
            if "SHIPPING INSTRUCTIONS:" in line.upper():
                j = i - 1
                while j >= 0:
                    candidate = lines[j].strip()
                    match = re.search(r'\b\d{1,3}\.\d{2}\b', candidate)
                    if match:
                        return match.group(0)
                    j -= 1
                break
        return ""

    def _process_invoice_data(self, invoice_no, data):
        """Process collected data for an invoice and create CSV."""
        if not data['has_totals']:
            print(f"Warning: No totals found for invoice {invoice_no}")
            return

        # Get totals from the last page that has non-empty totals
        totals = None
        bol_cube = ""
        print("\nLooking for totals in pages (reverse order):")  # Debug
        for i, page in enumerate(reversed(data['pages'])):
            print(f"  Checking page {len(data['pages'])-i}")  # Debug
            print(f"    Has totals: {page['has_totals']}")
            if page['has_totals'] and page['totals']['pieces'] and page['totals']['weight']:
                totals = page['totals']
                bol_cube = page['bol_cube']
                print(f"    Found valid totals: {totals}")
                print(f"    BOL Cube: {bol_cube}")
                break

        if not totals:
            print(f"ERROR: No valid totals found in any page for invoice {invoice_no}")
            return

        # Collect all rows from all pages
        all_rows = []
        for page in data['pages']:
            for row in page['rows']:
                # row is [cartons, individual_pieces, individual_weight, style]
                all_rows.append([row[0], bol_cube, row[1], row[2], invoice_no, row[3]])

        # Generate CSV
        formatted_data = self._format_csv(all_rows, totals['pieces'], totals['weight'])
        if formatted_data:
            new_filename = f"{invoice_no}.csv"
            new_file_path = os.path.join(self.session_dir, new_filename)
            
            with open(new_file_path, 'w', encoding='utf-8', newline='') as file:
                file.write(formatted_data)
            
            print(f"Processed multi-page invoice {invoice_no}")

    def _format_csv(self, rows, total_pieces, total_weight):
        """Format rows into CSV with proper column mapping."""
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header
        header = [""] * 28
        header[0] = "RTS ID"
        header[1] = "RTS Status"
        header[2] = "Load #"
        header[3] = "Wave #"
        header[4] = "Routed Date"
        header[5] = "Ready Date"
        header[6] = "Date of Pickup"
        header[7] = "Time of Pickup"
        header[8] = "Outbound BOL"
        header[9] = "Order Date"              # Column J
        header[10] = "Customer"
        header[11] = "Ship To Name"           # Column L
        header[12] = "Purchase Order No."    # Column M
        header[13] = "Cartons"                # Column N
        header[14] = "Start Date"             # Column O
        header[15] = "Cancel Date"            # Column P
        header[16] = "BOL Cube"               # Column Q
        header[17] = "Final Cube"
        header[18] = "Burlington Cube"
        header[19] = "Pallet"
        header[20] = "Individual Pieces"      # Column U
        header[21] = "Total Pieces"
        header[22] = "Individual Weight"      # Column W
        header[23] = "Total Weight"
        header[24] = "Invoice No."            # Column Y
        header[25] = "Style"                  # Column Z
        header[26] = "Release"                  # Column AA
        header[27] = "Assigned Trucking Co."                  # Column AB
        writer.writerow(header)

        # Sort rows by Invoice No. to ensure consistent grouping
        sorted_rows = sorted(rows, key=lambda x: x[4])  # Sort by Invoice No. (index 4)
        
        # Group rows by invoice number
        current_invoice = None
        is_first_row = True

        # Write data rows
        for row_data in sorted_rows:
            data_row = [""] * 28
            invoice_no = row_data[4]  # Get current row's invoice number
            
            # Check if this is the first row of a new invoice group
            if invoice_no != current_invoice:
                current_invoice = invoice_no
                is_first_row = True
            
            # Fill in the standard fields
            data_row[13] = row_data[0]  # Cartons
            data_row[16] = row_data[1]  # BOL Cube
            data_row[20] = row_data[2]  # Individual Pieces
            data_row[22] = row_data[3]  # Individual Weight
            data_row[24] = row_data[4]  # Invoice No.
            data_row[25] = row_data[5]  # Style
            
            # Only include totals in the first row of each invoice group
            if is_first_row:
                data_row[21] = total_pieces  # Total Pieces
                data_row[23] = total_weight  # Total Weight
                is_first_row = False
            
            writer.writerow(data_row)

        return output.getvalue()

    def _get_invoice_no(self, content):
        """Extract invoice number from content using regex."""
        lines = content.splitlines()
        invoice_no = ""
        for line in lines[:10]:
            if "BILL OF LADING" in line.upper():
                match = re.search(r'BILL OF LADING\s+([A-Z]\d+)', line, re.IGNORECASE)
                if match:
                    invoice_no = match.group(1)
                    break
        return invoice_no

    def _format_data(self, content):
        """Extract and structure shipping information into CSV format with specific column assignments."""
        lines = content.splitlines()
        
        # --- Extract BOL Cube ---
        bol_cube = ""
        for i, line in enumerate(lines):
            if "SHIPPING INSTRUCTIONS:" in line.upper():
                j = i - 1
                while j >= 0:
                    candidate = lines[j].strip()
                    match = re.search(r'\b\d{1,3}\.\d{2}\b', candidate)
                    if match:
                        bol_cube = match.group(0)
                        break
                    j -= 1
                break
        
        # --- Extract Invoice No. ---
        invoice_no = ""
        for line in lines[:10]:
            if "BILL OF LADING" in line.upper():
                match = re.search(r'BILL OF LADING\s+([A-Z]\d+)', line, re.IGNORECASE)
                if match:
                    invoice_no = match.group(1)
                    break

        # --- Locate the Table Header ---
        table_start = None
        for i, line in enumerate(lines):
            if "CARTONS" in line.upper() and "STYLE" in line.upper() and "PIECES" in line.upper():
                table_start = i
                break
        
        if table_start is None:
            print("Table header not found in the document.")
            return None

        # --- Process Table Rows and Extract Summary Totals ---
        rows = []
        summary_total_pieces = ""
        summary_total_weight = ""
        for line in lines[table_start+1:]:
            if "TOTAL CARTONS" in line.upper():
                # Expect a line like: "30 TOTAL CARTONS 2,160 TOTAL PIECES TOTAL VOL / WGT 595.2"
                tokens = line.split()
                if len(tokens) >= 11:
                    summary_total_pieces = tokens[3].replace(',', '')  # Remove commas
                    summary_total_weight = tokens[-1].replace(',', '')  # Remove commas
                break
            if "SHIPPING INSTRUCTIONS:" in line.upper():
                break
            if not line.strip():
                continue
            if re.match(r'^\d+', line.strip()):
                tokens = line.split()
                if len(tokens) < 3:
                    continue
                cartons = tokens[0].replace(',', '')  # Remove commas
                style = tokens[1]
                individual_pieces = tokens[2].replace(',', '')  # Remove commas
                individual_weight = tokens[-1].replace(',', '')  # Remove commas
                rows.append([cartons, bol_cube, individual_pieces, individual_weight, invoice_no, style])
            else:
                continue

        # --- Generate CSV with Specific Column Mapping ---
        # Create a CSV with 28 columns (A-AB)
        output = io.StringIO()
        writer = csv.writer(output)

        header = [""] * 28
        header[0] = "RTS ID"
        header[1] = "RTS Status"
        header[2] = "Load #"
        header[3] = "Wave #"
        header[4] = "Routed Date"
        header[5] = "Ready Date"
        header[6] = "Date of Pickup"
        header[7] = "Time of Pickup"
        header[8] = "Outbound BOL"
        header[9] = "Order Date"              # Column J
        header[10] = "Customer"
        header[11] = "Ship To Name"           # Column L
        header[12] = "Purchase Order No."    # Column M
        header[13] = "Cartons"                # Column N
        header[14] = "Start Date"             # Column O
        header[15] = "Cancel Date"            # Column P
        header[16] = "BOL Cube"               # Column Q
        header[17] = "Final Cube"
        header[18] = "Burlington Cube"
        header[19] = "Pallet"
        header[20] = "Individual Pieces"      # Column U
        header[21] = "Total Pieces"
        header[22] = "Individual Weight"      # Column W
        header[23] = "Total Weight"
        header[24] = "Invoice No."            # Column Y
        header[25] = "Style"                  # Column Z
        header[26] = "Release"                  # Column AA
        header[27] = "Assigned Trucking Co."                  # Column AB

        # Ensure any header cell that is empty is explicitly an empty string.
        header = [col if col else "" for col in header]
        writer.writerow(header)

        # Write each data row, adding the summary totals to the designated columns.
        for row_data in rows:
            # row_data is [cartons, bol_cube, individual_pieces, individual_weight, invoice_no, style]
            data_row = [""] * 28
            data_row[13] = row_data[0]  # Cartons -> Column N
            data_row[16] = row_data[1]  # BOL Cube -> Column Q
            data_row[20] = row_data[2]  # Individual Pieces -> Column U
            data_row[22] = row_data[3]  # Individual Weight -> Column W
            data_row[24] = row_data[4]  # Invoice No. -> Column Y
            data_row[25] = row_data[5]  # Style -> Column Z
            data_row[21] = summary_total_pieces  # Total Pieces -> Column V (index 21)
            data_row[23] = summary_total_weight  # Total Weight -> Column X (index 23)
            writer.writerow(data_row)
        
        return output.getvalue()

if __name__ == "__main__":
    processor = DataProcessor()
    processor.process_all_files()