#!/bin/bash

# Agentic LLM Platform Setup Script
# This script helps you set up the development environment

set -e

echo "🚀 Setting up Agentic LLM Platform..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3.11+ is required but not installed"
        exit 1
    fi
    
    python_version=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
    if [[ "$(printf '%s\n' "3.11" "$python_version" | sort -V | head -n1)" != "3.11" ]]; then
        print_error "Python 3.11+ is required. Current version: $python_version"
        exit 1
    fi
    
    # Check Node.js
    if ! command -v node &> /dev/null; then
        print_error "Node.js 18+ is required but not installed"
        exit 1
    fi
    
    node_version=$(node --version | cut -d'v' -f2 | cut -d'.' -f1)
    if [[ $node_version -lt 18 ]]; then
        print_error "Node.js 18+ is required. Current version: $node_version"
        exit 1
    fi
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_warning "Docker is not installed. You'll need it for the full development environment."
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        print_warning "Docker Compose is not installed. You'll need it for the full development environment."
    fi
    
    print_success "Prerequisites check completed"
}

# Setup backend
setup_backend() {
    print_status "Setting up backend..."
    
    cd backend
    
    # Create virtual environment
    if [ ! -d "venv" ]; then
        print_status "Creating Python virtual environment..."
        python3 -m venv venv
    fi
    
    # Activate virtual environment
    source venv/bin/activate
    
    # Upgrade pip
    print_status "Upgrading pip..."
    pip install --upgrade pip
    
    # Install dependencies
    print_status "Installing Python dependencies..."
    pip install -r requirements.txt
    
    # Create .env file if it doesn't exist
    if [ ! -f ".env" ]; then
        print_status "Creating .env file..."
        cp .env.example .env
        print_warning "Please edit backend/.env file with your API keys and configuration"
    fi
    
    cd ..
    print_success "Backend setup completed"
}

# Setup frontend
setup_frontend() {
    print_status "Setting up frontend..."
    
    cd frontend
    
    # Install dependencies
    print_status "Installing Node.js dependencies..."
    npm install
    
    # Create next.config.js if needed
    if [ ! -f "tailwind.config.js" ]; then
        print_status "Initializing Tailwind CSS..."
        npx tailwindcss init -p
    fi
    
    cd ..
    print_success "Frontend setup completed"
}

# Setup Docker environment
setup_docker() {
    print_status "Setting up Docker environment..."
    
    # Create Docker network if it doesn't exist
    if ! docker network ls | grep -q agent-platform; then
        print_status "Creating Docker network..."
        docker network create agent-platform
    fi
    
    print_success "Docker environment setup completed"
}

# Download recommended models
setup_models() {
    print_status "Setting up local models..."
    
    # Check if Ollama is available
    if command -v ollama &> /dev/null; then
        print_status "Pulling recommended Ollama models..."
        
        # Pull small model for development
        if ! ollama list | grep -q "llama2:7b"; then
            print_status "Pulling llama2:7b model..."
            ollama pull llama2:7b
        fi
        
        # Pull coding model
        if ! ollama list | grep -q "codellama:7b"; then
            print_status "Pulling codellama:7b model..."
            ollama pull codellama:7b
        fi
        
        print_success "Local models setup completed"
    else
        print_warning "Ollama not installed. Local models will not be available."
        print_status "Install Ollama from: https://ollama.ai"
    fi
}

# Create sample configuration
create_sample_config() {
    print_status "Creating sample configuration..."
    
    # Create examples directory structure
    mkdir -p examples/sample_project
    
    cat > examples/sample_project/requirements.md << EOF
# Sample Project Requirements

## Overview
Build a simple task management web application.

## Core Features
- User authentication and registration
- Create, read, update, delete tasks
- Task categories and priorities
- Due date tracking
- Simple dashboard with statistics

## Technical Requirements
- Web-based application
- Responsive design for mobile and desktop
- RESTful API backend
- Modern frontend framework
- Database for persistence

## Non-Functional Requirements
- Fast response times (< 2 seconds)
- Support for 100 concurrent users
- Clean, intuitive user interface
- Secure user data handling
EOF

    print_success "Sample configuration created"
}

# Display next steps
show_next_steps() {
    echo ""
    print_success "🎉 Setup completed successfully!"
    echo ""
    echo "Next steps:"
    echo ""
    echo "1. Configure your environment:"
    echo "   - Edit backend/.env with your API keys"
    echo "   - Set up OpenAI/Anthropic API keys for commercial models"
    echo "   - Configure GPU provider credentials (Modal, RunPod, etc.)"
    echo ""
    echo "2. Start the development environment:"
    echo "   Using Docker (recommended):"
    echo "   $ docker-compose up -d"
    echo ""
    echo "   Or manually:"
    echo "   $ cd backend && source venv/bin/activate && python main.py"
    echo "   $ cd frontend && npm run dev"
    echo ""
    echo "3. Access the application:"
    echo "   - Frontend: http://localhost:3000"
    echo "   - Backend API: http://localhost:8000"
    echo "   - API Documentation: http://localhost:8000/docs"
    echo ""
    echo "4. Try the sample project:"
    echo "   - Use the requirements from examples/sample_project/requirements.md"
    echo "   - Start a new workflow and paste the sample requirements"
    echo ""
    echo "📚 Documentation:"
    echo "   - Architecture: docs/architecture.md"
    echo "   - API Reference: http://localhost:8000/docs"
    echo ""
    echo "🆘 Need help? Check the README.md file or create an issue on GitHub."
    echo ""
}

# Parse command line arguments
SKIP_MODELS=false
SKIP_DOCKER=false
DOCKER_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-models)
            SKIP_MODELS=true
            shift
            ;;
        --skip-docker)
            SKIP_DOCKER=true
            shift
            ;;
        --docker-only)
            DOCKER_ONLY=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --skip-models    Skip downloading local models"
            echo "  --skip-docker    Skip Docker setup"
            echo "  --docker-only    Only setup Docker environment"
            echo "  -h, --help       Show this help message"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Main setup flow
main() {
    echo "🤖 Agentic LLM Platform Setup"
    echo "============================="
    echo ""
    
    if [[ "$DOCKER_ONLY" == "true" ]]; then
        check_prerequisites
        setup_docker
        print_success "Docker-only setup completed!"
        echo "Run: docker-compose up -d"
        exit 0
    fi
    
    check_prerequisites
    setup_backend
    setup_frontend
    create_sample_config
    
    if [[ "$SKIP_DOCKER" != "true" ]]; then
        setup_docker
    fi
    
    if [[ "$SKIP_MODELS" != "true" ]]; then
        setup_models
    fi
    
    show_next_steps
}

# Run main function
main
