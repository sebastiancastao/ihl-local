import os
import platform

# OpenAI Configuration
OPENAI_API_KEY = os.environ.get("OPEN_AI_API_KEY", "")

# Poppler Configuration
if platform.system() == "Windows":
    POPPLER_PATH = "C:\\Program Files\\poppler\\bin"
else:
    # On Linux, poppler-utils is installed via apt-get and is usually in PATH
    POPPLER_PATH = None  # or '/usr/bin' if your code requires an explicit path

# File Processing
OUTPUT_CSV_NAME = "combined_data.csv"

# Models
OPENAI_MODEL = "o3-mini"

# Animation Settings
TYPING_DELAY = 0.03
LOADING_ANIMATION_CHARS = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"] 