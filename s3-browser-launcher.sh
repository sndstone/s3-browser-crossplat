#!/bin/bash

# Enhanced S3 Browser Clone Launcher Script
# This script ensures all dependencies are properly installed and configured

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PYTHON_SCRIPT="$SCRIPT_DIR/s3_browser.py"
VENV_DIR="$SCRIPT_DIR/venv"
REQUIREMENTS_FILE="$SCRIPT_DIR/requirements.txt"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${CYAN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

echo "=============================================="
echo "Enhanced S3 Browser Clone Launcher"
echo "=============================================="

# Create requirements.txt if it doesn't exist
create_requirements_file() {
    if [ ! -f "$REQUIREMENTS_FILE" ]; then
        log "Creating requirements.txt file..."
        cat > "$REQUIREMENTS_FILE" << EOF
boto3>=1.26.0
botocore>=1.29.0
requests>=2.28.0
urllib3>=1.26.0
python-dateutil>=2.8.0
jmespath>=1.0.0
s3transfer>=0.6.0
six>=1.16.0
prettytable>=3.5.0
EOF
        success "Requirements file created"
    fi
}

# Detect OS and package manager
detect_os() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$NAME
        VER=$VERSION_ID
    elif type lsb_release >/dev/null 2>&1; then
        OS=$(lsb_release -si)
        VER=$(lsb_release -sr)
    elif [ -f /etc/lsb-release ]; then
        . /etc/lsb-release
        OS=$DISTRIB_ID
        VER=$DISTRIB_RELEASE
    elif [ -f /etc/debian_version ]; then
        OS=Debian
        VER=$(cat /etc/debian_version)
    elif [ -f /etc/SuSe-release ]; then
        OS=openSUSE
    elif [ -f /etc/redhat-release ]; then
        OS=RedHat
    else
        OS=$(uname -s)
        VER=$(uname -r)
    fi
    
    info "Detected OS: $OS $VER"
}

# Install system dependencies
install_system_deps() {
    log "Installing system dependencies..."
    
    # Detect package manager and install dependencies
    if command -v apt-get &> /dev/null; then
        # Debian/Ubuntu
        info "Using apt-get package manager"
        sudo apt-get update -qq
        # Install python3-venv with specific version if needed
        PYTHON_VERSION_FULL=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        sudo apt-get install -y python3 python3-pip python3-venv python${PYTHON_VERSION_FULL}-venv python3-dev python3-tk \
            build-essential libssl-dev libffi-dev curl wget git
        
    elif command -v dnf &> /dev/null; then
        # Fedora/CentOS 8+
        info "Using dnf package manager"
        sudo dnf install -y python3 python3-pip python3-devel python3-tkinter \
            gcc openssl-devel libffi-devel curl wget git
        
    elif command -v yum &> /dev/null; then
        # CentOS/RHEL 7
        info "Using yum package manager"
        sudo yum install -y python3 python3-pip python3-devel python3-tkinter \
            gcc openssl-devel libffi-devel curl wget git
        
    elif command -v pacman &> /dev/null; then
        # Arch Linux
        info "Using pacman package manager"
        sudo pacman -S --needed --noconfirm python python-pip tk gcc openssl libffi curl wget git
        
    elif command -v zypper &> /dev/null; then
        # openSUSE
        info "Using zypper package manager"
        sudo zypper install -y python3 python3-pip python3-devel python3-tk \
            gcc libopenssl-devel libffi-devel curl wget git
        
    elif command -v emerge &> /dev/null; then
        # Gentoo
        info "Using emerge package manager"
        sudo emerge --ask=n dev-lang/python dev-python/pip dev-lang/tk \
            sys-devel/gcc dev-libs/openssl dev-libs/libffi net-misc/curl net-misc/wget dev-vcs/git
        
    elif command -v apk &> /dev/null; then
        # Alpine Linux
        info "Using apk package manager"
        sudo apk add python3 py3-pip python3-dev tk gcc musl-dev libffi-dev openssl-dev curl wget git
        
    elif command -v pkg &> /dev/null; then
        # FreeBSD
        info "Using pkg package manager"
        sudo pkg install -y python3 py37-pip py37-tkinter gcc openssl libffi curl wget git
        
    elif command -v brew &> /dev/null; then
        # macOS with Homebrew
        info "Using brew package manager"
        brew install python3 python-tk openssl libffi curl wget git
        
    else
        warning "Could not detect package manager. Please install the following manually:"
        echo "  - Python 3.7 or higher"
        echo "  - pip3"
        echo "  - python3-venv"
        echo "  - python3-tk (tkinter)"
        echo "  - Development tools (gcc, make, etc.)"
        echo "  - OpenSSL development headers"
        echo "  - libffi development headers"
        echo ""
        read -p "Press Enter to continue if you have installed these manually..."
    fi
}

# Check if Python 3 is installed and meets minimum version
check_python() {
    log "Checking Python installation..."
    
    if ! command -v python3 &> /dev/null; then
        error "Python 3 is not installed or not in PATH"
        return 1
    fi
    
    PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
    REQUIRED_VERSION="3.7"
    
    if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then
        error "Python $PYTHON_VERSION is installed, but Python $REQUIRED_VERSION or higher is required"
        return 1
    fi
    
    success "Python $PYTHON_VERSION found"
    return 0
}

# Check if pip is installed and upgrade if needed
check_pip() {
    log "Checking pip installation..."
    
    if ! python3 -m pip --version &> /dev/null; then
        warning "pip is not installed. Installing pip..."
        curl -s https://bootstrap.pypa.io/get-pip.py | python3
        if [ $? -ne 0 ]; then
            error "Failed to install pip"
            return 1
        fi
    fi
    
    # Upgrade pip to latest version
    info "Upgrading pip to latest version..."
    if ! python3 -m pip install --upgrade pip --quiet; then
        warning "System pip upgrade failed (externally-managed environment). Continuing with venv pip."
    fi
    
    success "pip is ready"
    return 0
}

# Check and install tkinter
check_tkinter() {
    log "Checking tkinter availability..."
    
    if ! python3 -c "import tkinter" &> /dev/null; then
        warning "tkinter is not available. Attempting to install..."
        
        # Try to install tkinter using detected package manager
        if command -v apt-get &> /dev/null; then
            sudo apt-get install -y python3-tk
        elif command -v dnf &> /dev/null; then
            sudo dnf install -y python3-tkinter
        elif command -v yum &> /dev/null; then
            sudo yum install -y python3-tkinter
        elif command -v pacman &> /dev/null; then
            sudo pacman -S --noconfirm tk
        elif command -v zypper &> /dev/null; then
            sudo zypper install -y python3-tk
        elif command -v apk &> /dev/null; then
            sudo apk add tk
        else
            error "Could not install tkinter automatically"
            echo "Please install tkinter manually for your distribution:"
            echo "  Ubuntu/Debian: sudo apt-get install python3-tk"
            echo "  Fedora/CentOS: sudo dnf install python3-tkinter"
            echo "  Arch Linux: sudo pacman -S tk"
            return 1
        fi
        
        # Verify installation
        if ! python3 -c "import tkinter" &> /dev/null; then
            error "tkinter installation failed"
            return 1
        fi
    fi
    
    success "tkinter is available"
    return 0
}

# Check if venv module is available
check_venv() {
    log "Checking python3-venv availability..."
    
    if ! python3 -m venv --help &> /dev/null; then
        warning "python3-venv is not installed"
        
        # Try to install it automatically
        if command -v apt-get &> /dev/null; then
            info "Attempting to install python3-venv..."
            PYTHON_VERSION_FULL=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
            sudo apt-get update -qq
            # Try both generic and version-specific packages
            sudo apt-get install -y python3-venv python${PYTHON_VERSION_FULL}-venv 2>/dev/null || \
            sudo apt-get install -y python3-venv
        elif command -v dnf &> /dev/null; then
            info "Attempting to install python3-venv..."
            sudo dnf install -y python3-venv
        elif command -v yum &> /dev/null; then
            info "Attempting to install python3-venv..."
            sudo yum install -y python3-venv
        elif command -v pacman &> /dev/null; then
            info "python3-venv should be included with python on Arch Linux"
        else
            error "Cannot install python3-venv automatically"
            echo "Please install python3-venv manually or run this script with --install flag"
            return 1
        fi
        
        # Verify installation
        if ! python3 -m venv --help &> /dev/null; then
            error "python3-venv installation failed"
            echo "Please run manually:"
            echo "  Ubuntu/Debian: sudo apt-get install python3-venv python${PYTHON_VERSION_FULL}-venv"
            echo "  or run this script with: $0 --install"
            return 1
        fi
    fi
    
    success "python3-venv is available"
    return 0
}

# Setup virtual environment
setup_venv() {
    log "Setting up virtual environment..."
    
    if [ ! -d "$VENV_DIR" ]; then
        info "Creating virtual environment..."
        python3 -m venv "$VENV_DIR"
        if [ $? -ne 0 ]; then
            error "Failed to create virtual environment"
            echo "Try running: $0 --install"
            return 1
        fi
    else
        info "Virtual environment already exists"
    fi
    
    # Activate virtual environment
    source "$VENV_DIR/bin/activate"
    if [ $? -ne 0 ]; then
        error "Failed to activate virtual environment"
        return 1
    fi
    
    # Upgrade pip in virtual environment
    "$VENV_DIR/bin/python" -m pip install --upgrade pip --quiet
    
    success "Virtual environment is ready"
    return 0
}

# Install Python dependencies
install_python_deps() {
    log "Installing Python dependencies..."
    
    # Make sure we're in the virtual environment
    if [ -z "$VIRTUAL_ENV" ]; then
        source "$VENV_DIR/bin/activate"
    fi
    
    # Install from requirements file if it exists
    if [ -f "$REQUIREMENTS_FILE" ]; then
        info "Installing from requirements.txt..."
        "$VENV_DIR/bin/python" -m pip install -r "$REQUIREMENTS_FILE" --quiet
    else
        info "Installing individual packages..."
        "$VENV_DIR/bin/python" -m pip install boto3 --quiet
    fi
    
    # Verify critical packages
    python3 -c "import boto3, tkinter, json, threading" &> /dev/null
    if [ $? -ne 0 ]; then
        error "Failed to verify Python dependencies"
        return 1
    fi
    
    success "Python dependencies installed successfully"
    return 0
}

# Verify main script exists
check_main_script() {
    log "Checking main script..."
    
    if [ ! -f "$PYTHON_SCRIPT" ]; then
        error "s3_browser.py not found in $SCRIPT_DIR"
        echo "Please ensure s3_browser.py is in the same directory as this launcher."
        return 1
    fi
    
    success "Main script found"
    return 0
}

# Create desktop entry (optional)
create_desktop_entry() {
    if [ "$XDG_CURRENT_DESKTOP" ] && [ -d "$HOME/.local/share/applications" ]; then
        log "Creating desktop entry..."
        
        cat > "$HOME/.local/share/applications/s3-browser.desktop" << EOF
[Desktop Entry]
Version=1.0
Type=Application
Name=S3 Browser Clone
Comment=Browse and manage S3-compatible storage
Exec=$SCRIPT_DIR/$(basename "$0")
Icon=folder-remote
Terminal=false
Categories=Network;FileManager;
EOF
        
        success "Desktop entry created"
    fi
}

# Main installation process
main() {
    detect_os
    
    # Check if running with --install flag
    if [ "$1" = "--install" ] || [ "$1" = "-i" ]; then
        log "Running full installation..."
        install_system_deps
    fi
    
    # Create requirements file
    create_requirements_file
    
    # Check and setup Python environment
    if ! check_python; then
        error "Python check failed. Run with --install flag to install system dependencies."
        exit 1
    fi
    
    if ! check_pip; then
        error "pip check failed"
        exit 1
    fi
    
    if ! check_tkinter; then
        error "tkinter check failed. Run with --install flag to install system dependencies."
        exit 1
    fi
    
    if ! check_venv; then
        error "python3-venv check failed. Run with --install flag to install system dependencies."
        exit 1
    fi
    
    if ! setup_venv; then
        error "Virtual environment setup failed"
        exit 1
    fi
    
    if ! install_python_deps; then
        error "Python dependencies installation failed"
        exit 1
    fi
    
    if ! check_main_script; then
        error "Main script check failed"
        exit 1
    fi
    
    # Optional desktop entry
    if [ "$1" = "--install" ] || [ "$1" = "-i" ]; then
        create_desktop_entry
    fi
    
    success "All dependencies satisfied"
    echo "=============================================="
    echo "Launching S3 Browser Clone..."
    echo ""
    
    # Launch the application
    source "$VENV_DIR/bin/activate"
    info "Using script: $PYTHON_SCRIPT"
    info "Using Python: $VENV_DIR/bin/python"
    "$VENV_DIR/bin/python" "$PYTHON_SCRIPT"
}

# Handle command line arguments
case "$1" in
    --help|-h)
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --install, -i    Install system dependencies"
        echo "  --help, -h       Show this help message"
        echo ""
        echo "Without options, the script will check dependencies and launch the application."
        echo "If dependencies are missing, run with --install to install them."
        exit 0
        ;;
    --install|-i)
        main --install
        ;;
    *)
        main
        ;;
esac
