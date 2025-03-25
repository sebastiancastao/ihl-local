import os
import gc
import pdfplumber
import pdf2image
from utils import PopplerUtils, FileUtils
from config import POPPLER_PATH

class PDFProcessor:
    def __init__(self, session_dir):
        """Initialize the PDF processor with a session directory."""
        PopplerUtils.check_poppler_installation()
        self.session_dir = session_dir

    def process_first_pdf(self):
        """Process the first PDF found in the directory."""
        pdf_files = [f for f in os.listdir(self.session_dir) if f.lower().endswith('.pdf')]
        if not pdf_files:
            print("No PDF files found in the session directory")
            return False

        pdf_path = os.path.join(self.session_dir, pdf_files[0])
        print(f"Processing {pdf_path}...")
        
        try:
            # Extract text
            success = self.extract_text(pdf_path)
            
            # Clean up the PDF file after processing
            os.remove(pdf_path)
            print(f"Removed processed PDF: {pdf_files[0]}")
            
            # Force garbage collection
            gc.collect()
            
            return success
            
        except Exception as e:
            print(f"Error processing PDF: {str(e)}")
            return False

    def extract_text(self, pdf_path):
        """Extract text from PDF and save as numbered TXT files."""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for i, page in enumerate(pdf.pages):
                    # Process one page at a time
                    text = page.extract_text()
                    text_path = os.path.join(self.session_dir, f"{i+1}.txt")
                    
                    with open(text_path, 'w', encoding='utf-8') as text_file:
                        text_file.write(text)
                    print(f"Saved text to {text_path}")
                    
                    # Clear page from memory
                    page.flush_cache()
                    
                    # Force garbage collection every few pages
                    if i % 5 == 0:
                        gc.collect()
                        
            return True
                    
        except Exception as e:
            print(f"Error extracting text from PDF: {str(e)}")
            return False

    def extract_images(self, pdf_path):
        """Convert PDF pages to images and save as numbered JPGs."""
        try:
            images = pdf2image.convert_from_path(
                pdf_path,
                poppler_path=POPPLER_PATH
            )
            
            for i, image in enumerate(images):
                image_path = os.path.join(self.session_dir, f"page_{i+1}.jpg")
                image.save(image_path, "JPEG")
                print(f"Saved image to {image_path}")
                
        except Exception as e:
            print(f"Error converting PDF to images: {str(e)}")

if __name__ == "__main__":
    processor = PDFProcessor(".")  # Use current directory for CLI usage
    processor.process_first_pdf() 