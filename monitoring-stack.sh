#!/bin/bash
# Helper script to manage the DataProject monitoring stack

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Functions
print_header() {
    echo -e "${BLUE}=================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}=================================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check if Docker is installed
check_docker() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
}

# Start the monitoring stack
start_stack() {
    print_header "Starting DataProject Monitoring Stack"
    
    check_docker
    
    print_info "Building and starting containers..."
    docker-compose up -d --build
    
    echo ""
    print_success "Stack started successfully!"
    echo ""
    print_info "Services are available at:"
    echo "  - API:        http://localhost:8000"
    echo "  - API Docs:   http://localhost:8000/docs"
    echo "  - Metrics:    http://localhost:8000/monitoring/metrics"
    echo "  - Prometheus: http://localhost:9090"
    echo "  - Grafana:    http://localhost:3000 (admin/admin)"
    echo ""
    print_info "Grafana dashboard: http://localhost:3000/d/dataproject-api"
    echo ""
    print_info "Run 'docker-compose logs -f' to view logs"
}

# Stop the monitoring stack
stop_stack() {
    print_header "Stopping DataProject Monitoring Stack"
    
    check_docker
    
    docker-compose down
    print_success "Stack stopped successfully!"
}

# Restart the monitoring stack
restart_stack() {
    print_header "Restarting DataProject Monitoring Stack"
    stop_stack
    sleep 2
    start_stack
}

# View logs
view_logs() {
    check_docker
    
    if [ -z "$1" ]; then
        docker-compose logs -f
    else
        docker-compose logs -f "$1"
    fi
}

# Show stack status
show_status() {
    print_header "DataProject Monitoring Stack Status"
    
    check_docker
    
    docker-compose ps
}

# Clean up everything
cleanup() {
    print_header "Cleaning Up DataProject Monitoring Stack"
    
    check_docker
    
    print_info "Stopping and removing containers, networks, and volumes..."
    docker-compose down -v
    
    print_success "Cleanup completed!"
}

# Main script
case "$1" in
    start)
        start_stack
        ;;
    stop)
        stop_stack
        ;;
    restart)
        restart_stack
        ;;
    logs)
        view_logs "$2"
        ;;
    status)
        show_status
        ;;
    cleanup)
        cleanup
        ;;
    *)
        echo "DataProject Monitoring Stack Manager"
        echo ""
        echo "Usage: $0 {start|stop|restart|logs|status|cleanup}"
        echo ""
        echo "Commands:"
        echo "  start    - Start the monitoring stack (API + Prometheus + Grafana)"
        echo "  stop     - Stop the monitoring stack"
        echo "  restart  - Restart the monitoring stack"
        echo "  logs     - View logs (use 'logs api', 'logs prometheus', or 'logs grafana')"
        echo "  status   - Show status of all containers"
        echo "  cleanup  - Stop and remove all containers, networks, and volumes"
        exit 1
        ;;
esac
