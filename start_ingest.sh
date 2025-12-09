#!/bin/bash
 
# Move into src folder
cd "$(dirname "$0")/src"
 
# Activate Windows virtual environment (Git Bash compatible)
source ../venv/Scripts/activate
 
# Start main script
python -m main