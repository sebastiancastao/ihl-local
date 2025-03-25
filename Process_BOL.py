import time
from utils import UIUtils
from pdf_processor import PDFProcessor
from data_processor import DataProcessor
from csv_exporter import CSVExporter

def print_robot():
    robot = """
         ,     ,
        (\\____/)
         (_oo_)
           (O)
         __||__    \\)
      []/______\\[] /
      / \\______/ \\/
     /    /__\\
    (\\   /____\\
    """
    print(robot)

def print_hammer():
    hammer = """
      _____
     /     \\
    /  { }  \\
    \\  { }  /
     \\_____/
    """
    print(hammer)

def main():
    """Main process to handle BOL processing workflow."""
    print_robot()
    UIUtils.print_with_typing_effect("Welcome to the BOL Processing System!")
    print_hammer()
    
    # Step 1: Process PDF
    UIUtils.print_with_typing_effect("\nStep 1: Processing PDF...")
    UIUtils.loading_animation(1, "Initializing PDF processor")
    pdf_processor = PDFProcessor()
    if not pdf_processor.process_first_pdf():
        print("Failed to process PDF. Exiting...")
        return
    
    # Step 2: Process Text Files
    UIUtils.print_with_typing_effect("\nStep 2: Processing text files...")
    UIUtils.loading_animation(1, "Initializing data processor")
    data_processor = DataProcessor()
    if not data_processor.process_all_files():
        print("Failed to process text files. Exiting...")
        return
    
    # Step 3: Create CSV
    UIUtils.print_with_typing_effect("\nStep 3: Creating CSV file...")
    UIUtils.loading_animation(1, "Initializing CSV exporter")
    csv_exporter = CSVExporter()
    if not csv_exporter.combine_to_csv():
        print("Failed to create CSV file. Exiting...")
        return
    
    UIUtils.print_with_typing_effect("\nAll done! Your BOL has been processed successfully!")
    print_robot()

if __name__ == "__main__":
    main() 