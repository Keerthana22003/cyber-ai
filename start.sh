#!/bin/bash

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    print_error "Virtual environment not found!"
    echo "Creating virtual environment..."
    python -m venv venv
    print_status "Virtual environment created"
fi

# Activate virtual environment (cross-platform)
if [ -f "venv/Scripts/activate" ]; then
    # Windows (Git Bash)
    source venv/Scripts/activate
    print_status "Virtual environment activated (Windows)"
elif [ -f "venv/bin/activate" ]; then
    # Linux/Mac
    source venv/bin/activate
    print_status "Virtual environment activated (Linux/Mac)"
else
    print_error "Could not find activation script!"
    exit 1
fi

# Check if requirements need to be installed
if [ ! -f "venv/.installed" ]; then
    print_warning "Installing dependencies..."
    pip install --upgrade pip
    pip install -r requirements.txt
    touch venv/.installed
    print_status "Dependencies installed"
else
    print_status "Dependencies already installed"
fi

# Check for required directories
echo ""
echo "Checking directories..."
mkdir -p data/raw/input
mkdir -p data/processed/input
mkdir -p data/processed_pdfs
mkdir -p data/logs
print_status "Directories ready"

# Parse command line arguments
MODE="both"
WATCH_PATH="data/raw/input"

while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            MODE="$2"
            shift 2
            ;;
        --watch-path)
            WATCH_PATH="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: ./start.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --mode MODE          Service mode: both, watcher, tokenizer (default: both)"
            echo "  --watch-path PATH    Path to watch for files (default: data/raw/input)"
            echo "  -h, --help          Show this help message"
            echo ""
            echo "Examples:"
            echo "  ./start.sh                              # Run both services"
            echo "  ./start.sh --mode watcher               # Run file watcher only"
            echo "  ./start.sh --mode tokenizer             # Run tokenizer only"
            echo "  ./start.sh --watch-path /custom/path    # Custom watch directory"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Display configuration
echo ""
echo "Configuration:"
echo "  Mode: $MODE"
echo "  Watch Path: $WATCH_PATH"
echo ""

# Start the application
print_status "Starting CyberAI Pipeline..."
echo ""

# Move to src folder and run
cd src
python -m main --mode "$MODE" --watch-path "../$WATCH_PATH"

# Deactivate on exit (if script exits normally)
deactivate