import os
import platform
import shutil
import sys
import time
from subprocess import Popen, PIPE
import openai
from config import OPENAI_API_KEY, POPPLER_PATH, TYPING_DELAY, LOADING_ANIMATION_CHARS

# OpenAI setup
openai.api_key = OPENAI_API_KEY
client = openai.OpenAI(api_key=openai.api_key)

class PopplerUtils:
    @staticmethod
    def check_poppler_installation():
        """Check if Poppler is properly installed and accessible."""
        if platform.system() == "Windows":
            # On Windows, use the configured POPPLER_PATH and check for pdfinfo.exe
            poppler_exe = os.path.join(POPPLER_PATH, "pdfinfo.exe")
            if not os.path.exists(poppler_exe):
                PopplerUtils.print_installation_instructions()
                sys.exit(1)
            try:
                process = Popen([poppler_exe], stdout=PIPE, stderr=PIPE)
                process.communicate()
                if process.returncode == 99:
                    print("Poppler installation found successfully!")
                    return
                print(f"\nError: Poppler exists but returned unexpected code: {process.returncode}")
                PopplerUtils.print_installation_instructions()
                sys.exit(1)
            except Exception as e:
                print(f"\nError running Poppler: {str(e)}")
                PopplerUtils.print_installation_instructions()
                sys.exit(1)
        else:
            # On Linux/macOS, assume poppler-utils is installed via apt-get or package manager
            poppler_exe = shutil.which("pdfinfo")
            if not poppler_exe:
                PopplerUtils.print_installation_instructions()
                sys.exit(1)
            try:
                # Running with the '-v' flag should output version info and return 0 on success.
                process = Popen([poppler_exe, "-v"], stdout=PIPE, stderr=PIPE)
                stdout, stderr = process.communicate()
                if process.returncode == 0:
                    print("Poppler installation found successfully!")
                    return
                print(f"\nError: Poppler exists but returned unexpected code: {process.returncode}")
                PopplerUtils.print_installation_instructions()
                sys.exit(1)
            except Exception as e:
                print(f"\nError running Poppler: {str(e)}")
                PopplerUtils.print_installation_instructions()
                sys.exit(1)

    @staticmethod
    def print_installation_instructions():
        """Print instructions for installing Poppler."""
        print("\nPlease follow these steps:")
        print("1. Download Poppler from: https://github.com/oschwartz10612/poppler-windows/releases/")
        print("2. Extract it to a location (e.g., C:\\Program Files\\poppler)")
        print("3. Add the bin directory to your system PATH")
        print("\nAfter installation:")
        print("- Restart your terminal/IDE")
        print("- Make sure C:\\Program Files\\poppler\\bin is in your system PATH")
        print("\nTo verify installation, you should see pdfinfo.exe in:")
        print("C:\\Program Files\\poppler\\bin\\pdfinfo.exe")

class UIUtils:
    @staticmethod
    def print_with_typing_effect(text, delay=TYPING_DELAY):
        """Print text with a typing effect."""
        for char in text:
            sys.stdout.write(char)
            sys.stdout.flush()
            time.sleep(delay)
        print()

    @staticmethod
    def loading_animation(duration, message):
        """Show a loading animation with a message."""
        start_time = time.time()
        i = 0
        while time.time() - start_time < duration:
            sys.stdout.write(f'\r{LOADING_ANIMATION_CHARS[i]} {message}')
            sys.stdout.flush()
            time.sleep(0.1)
            i = (i + 1) % len(LOADING_ANIMATION_CHARS)
        sys.stdout.write('\r' + ' ' * (len(message) + 2) + '\r')
        sys.stdout.flush()

class FileUtils:
    @staticmethod
    def get_txt_files(directory):
        """Get all TXT files in the specified directory."""
        return [f for f in os.listdir(directory) if f.endswith('.txt')]

    @staticmethod
    def get_pdf_files(directory):
        """Get all PDF files in the specified directory."""
        return [f for f in os.listdir(directory) if f.lower().endswith('.pdf')]

    @staticmethod
    def get_script_dir():
        """Get the directory where the current script is located."""
        return os.path.dirname(os.path.abspath(__file__))
