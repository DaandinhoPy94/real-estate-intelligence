#!/bin/bash
# beast_mode_recovery.sh - Fix Docker issues and complete Beast Mode

echo "🔧 BEAST MODE RECOVERY - FIXING DOCKER ISSUES"
echo "=============================================="
echo "Current success rate: 50% - Let's get to 100%!"
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_step() {
    echo -e "${BLUE}🔧 $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# =============================================================================
# STEP 1: DIAGNOSE DOCKER ISSUES
# =============================================================================

print_step "Diagnosing Docker issues..."

# Check Docker daemon
if ! docker info > /dev/null 2>&1; then
    print_error "Docker daemon not running"
    echo "Please start Docker Desktop and try again"
    exit 1
else
    print_success "Docker daemon is running"
fi

# Check available ports
print_step "Checking port availability..."

check_port() {
    local port=$1
    local service=$2
    if lsof -i :$port > /dev/null 2>&1; then
        print_warning "Port $port is in use (might be $service from previous run)"
        return 1
    else
        print_success "Port $port is available"
        return 0
    fi
}

# Check critical ports
check_port 9092 "Kafka"
check_port 6379 "Redis" 
check_port 3001 "Grafana"
check_port 5001 "MLflow"

# =============================================================================
# STEP 2: CLEANUP PREVIOUS CONTAINERS
# =============================================================================

print_step "Cleaning up previous containers..."

# Stop any running phase3 containers
docker-compose -f docker-compose-phase3.yml down 2>/dev/null || true

# Remove any conflicting containers
docker rm -f $(docker ps -aq --filter "name=phase3_" 2>/dev/null) 2>/dev/null || true

print_success "Previous containers cleaned up"

# =============================================================================
# STEP 3: CREATE SIMPLIFIED DOCKER COMPOSE
# =============================================================================

print_step "Creating simplified Docker Compose for incremental setup..."

cat > docker-compose-beast-mode.yml << 'EOF'
version: '3.8'

services:
  # =============================================================================
  # CORE INFRASTRUCTURE (Start with these)
  # =============================================================================
  
  # PostgreSQL (Enhanced from Phase 2)
  postgres:
    image: timescale/timescaledb:latest-pg14
    container_name: beast_postgres
    environment:
      POSTGRES_DB: real_estate
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5434:5432"
    volumes:
      - beast_postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5

  # Redis (Enhanced)
  redis:
    image: redis:7-alpine
    container_name: beast_redis
    ports:
      - "6380:6379"
    command: redis-server --appendonly yes
    volumes:
      - beast_redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 3

  # =============================================================================
  # STREAMING LAYER
  # =============================================================================
  
  # Zookeeper for Kafka
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    container_name: beast_zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2182:2181"
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 10s
      timeout: 5s
      retries: 3

  # Kafka
  kafka:
    image: confluentinc/cp-kafka:7.4.0
    container_name: beast_kafka
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9093:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "kafka:29092", "--list"]
      interval: 30s
      timeout: 10s
      retries: 3

  # =============================================================================
  # MONITORING LAYER  
  # =============================================================================
  
  # Prometheus
  prometheus:
    image: prom/prometheus:latest
    container_name: beast_prometheus
    ports:
      - "9091:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
      - beast_prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'
      - '--storage.tsdb.retention.time=200h'
      - '--web.enable-lifecycle'
    healthcheck:
      test: ["CMD", "wget", "--quiet", "--tries=1", "--spider", "http://localhost:9090/-/healthy"]
      interval: 30s
      timeout: 10s
      retries: 3

  # Grafana
  grafana:
    image: grafana/grafana:latest
    container_name: beast_grafana
    ports:
      - "3002:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: beastmode
    volumes:
      - beast_grafana_data:/var/lib/grafana
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:3000/api/health || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 3

  # =============================================================================
  # ML LAYER
  # =============================================================================
  
  # MLflow
  mlflow:
    image: python:3.9-slim
    container_name: beast_mlflow
    ports:
      - "5002:5000"
    environment:
      BACKEND_STORE_URI: postgresql://postgres:postgres@postgres:5432/real_estate
      ARTIFACT_ROOT: /mlflow/artifacts
    volumes:
      - beast_mlflow_data:/mlflow/artifacts
    command: |
      bash -c "
        pip install mlflow psycopg2-binary &&
        mlflow server --backend-store-uri sqlite:///mlflow.db --default-artifact-root /mlflow/artifacts --host 0.0.0.0 --port 5000
      "
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:5000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s

volumes:
  beast_postgres_data:
  beast_redis_data:
  beast_prometheus_data:
  beast_grafana_data:
  beast_mlflow_data:

networks:
  default:
    name: beast_mode_network
    driver: bridge
EOF

print_success "Simplified Docker Compose created"

# =============================================================================
# STEP 4: INCREMENTAL STARTUP
# =============================================================================

print_step "Starting services incrementally..."

# Start core infrastructure first
echo "🏗️ Starting core infrastructure (PostgreSQL + Redis)..."
docker-compose -f docker-compose-beast-mode.yml up -d postgres redis

# Wait for core services
sleep 10

# Check core services
if docker ps | grep -q "beast_postgres.*Up"; then
    print_success "PostgreSQL is running"
else
    print_error "PostgreSQL failed to start"
fi

if docker ps | grep -q "beast_redis.*Up"; then
    print_success "Redis is running"
else
    print_error "Redis failed to start"
fi

# Start streaming layer
echo "⚡ Starting streaming layer (Kafka + Zookeeper)..."
docker-compose -f docker-compose-beast-mode.yml up -d zookeeper
sleep 15
docker-compose -f docker-compose-beast-mode.yml up -d kafka
sleep 20

# Check streaming services
if docker ps | grep -q "beast_kafka.*Up"; then
    print_success "Kafka is running"
    
    # Create Kafka topics
    echo "📝 Creating Kafka topics..."
    docker exec beast_kafka kafka-topics --create --topic property-price-updates --bootstrap-server localhost:29092 --partitions 3 --replication-factor 1 --if-not-exists
    docker exec beast_kafka kafka-topics --create --topic market-alerts --bootstrap-server localhost:29092 --partitions 3 --replication-factor 1 --if-not-exists
    docker exec beast_kafka kafka-topics --create --topic user-notifications --bootstrap-server localhost:29092 --partitions 3 --replication-factor 1 --if-not-exists
    
    print_success "Kafka topics created"
else
    print_warning "Kafka failed to start - continuing without streaming"
fi

# Start monitoring layer
echo "📊 Starting monitoring layer (Prometheus + Grafana)..."
mkdir -p monitoring
echo "global:
  scrape_interval: 15s" > monitoring/prometheus.yml

docker-compose -f docker-compose-beast-mode.yml up -d prometheus grafana

# Start ML layer  
echo "🧠 Starting ML layer (MLflow)..."
docker-compose -f docker-compose-beast-mode.yml up -d mlflow

# =============================================================================
# STEP 5: VERIFICATION
# =============================================================================

print_step "Verifying Beast Mode services..."

sleep 30

echo ""
echo "🔍 Service Status Check:"
echo "========================"

services=("beast_postgres:PostgreSQL" "beast_redis:Redis" "beast_kafka:Kafka" "beast_prometheus:Prometheus" "beast_grafana:Grafana" "beast_mlflow:MLflow")

running_services=0
total_services=${#services[@]}

for service_info in "${services[@]}"; do
    IFS=':' read -r container_name service_name <<< "$service_info"
    
    if docker ps | grep -q "$container_name.*Up"; then
        print_success "$service_name is running"
        ((running_services++))
    else
        print_warning "$service_name is not running"
    fi
done

success_rate=$((running_services * 100 / total_services))

echo ""
echo "🎯 BEAST MODE RECOVERY RESULTS"
echo "=============================="
echo "Services Running: $running_services/$total_services"
echo "Success Rate: $success_rate%"
echo ""

if [ $success_rate -ge 80 ]; then
    print_success "BEAST MODE RECOVERY SUCCESSFUL! 🔥"
    echo ""
    echo "🌐 Access Points (Updated Ports):"
    echo "• PostgreSQL: localhost:5434"
    echo "• Redis: localhost:6380" 
    echo "• Kafka: localhost:9093"
    echo "• Prometheus: http://localhost:9091"
    echo "• Grafana: http://localhost:3002 (admin/beastmode)"
    echo "• MLflow: http://localhost:5002"
    echo ""
    echo "🚀 Ready for Beast Mode development!"
else
    print_warning "PARTIAL SUCCESS - Some services failed to start"
    echo "Check docker logs for specific issues:"
    echo "docker-compose -f docker-compose-beast-mode.yml logs [service-name]"
fi

# =============================================================================
# STEP 6: NEXT STEPS SCRIPT
# =============================================================================

cat > beast_mode_next_steps.py << 'EOF'
#!/usr/bin/env python3
"""
Beast Mode Next Steps - Continue development with working infrastructure
"""
import asyncio
import aiohttp
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_beast_mode_services():
    """Test all Beast Mode services to ensure they're working"""
    
    print("🧪 Testing Beast Mode Services")
    print("=" * 40)
    
    services = [
        {"name": "Prometheus", "url": "http://localhost:9091/-/healthy", "timeout": 5},
        {"name": "Grafana", "url": "http://localhost:3002/api/health", "timeout": 5},
        {"name": "MLflow", "url": "http://localhost:5002/health", "timeout": 10},
    ]
    
    results = {}
    
    async with aiohttp.ClientSession() as session:
        for service in services:
            try:
                async with session.get(service["url"], timeout=service["timeout"]) as response:
                    if response.status == 200:
                        print(f"✅ {service['name']}: Healthy")
                        results[service["name"]] = "healthy"
                    else:
                        print(f"⚠️  {service['name']}: Status {response.status}")
                        results[service["name"]] = f"status_{response.status}"
            except asyncio.TimeoutError:
                print(f"❌ {service['name']}: Timeout")
                results[service["name"]] = "timeout"
            except Exception as e:
                print(f"❌ {service['name']}: Error - {e}")
                results[service["name"]] = f"error_{e}"
    
    # Test Kafka (different approach)
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "exec", "beast_kafka", "kafka-topics", "--bootstrap-server", "localhost:29092", "--list"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            print("✅ Kafka: Healthy")
            results["Kafka"] = "healthy"
        else:
            print("❌ Kafka: Failed")
            results["Kafka"] = "failed"
    except Exception as e:
        print(f"❌ Kafka: Error - {e}")
        results["Kafka"] = f"error_{e}"
    
    print("\n🎯 Beast Mode Health Summary:")
    healthy_count = sum(1 for status in results.values() if status == "healthy")
    total_count = len(results)
    health_percentage = (healthy_count / total_count) * 100
    
    print(f"Healthy Services: {healthy_count}/{total_count} ({health_percentage:.0f}%)")
    
    if health_percentage >= 75:
        print("🔥 BEAST MODE IS READY FOR DEVELOPMENT!")
        return True
    else:
        print("⚠️  Some services need attention before development")
        return False

async def demonstrate_beast_mode_capabilities():
    """Demonstrate key Beast Mode capabilities"""
    
    print("\n🚀 Beast Mode Capability Demonstration")
    print("=" * 50)
    
    # Test real-time data generation
    print("⚡ Generating real-time property data...")
    
    # Simulate property price update
    property_update = {
        "property_id": "DEMO_001",
        "address": "Herengracht 123, Amsterdam", 
        "old_price": 450000,
        "new_price": 465000,
        "change_percent": 3.33,
        "timestamp": datetime.now().isoformat(),
        "source": "beast_mode_demo"
    }
    
    print(f"📊 Property Update: {property_update['address']}")
    print(f"   Price: €{property_update['old_price']:,} → €{property_update['new_price']:,}")
    print(f"   Change: +{property_update['change_percent']:.1f}%")
    
    # Test ML prediction simulation
    print("\n🧠 ML Prediction Simulation...")
    
    import random
    
    # Simulate computer vision analysis
    cv_features = {
        "natural_light_score": random.uniform(0.7, 0.95),
        "room_condition": random.choice(["excellent", "good", "fair"]),
        "architectural_style": random.choice(["modern", "classic", "contemporary"]),
        "outdoor_space": random.choice([True, False])
    }
    
    # Simulate price prediction
    base_prediction = 425000
    cv_boost = cv_features["natural_light_score"] * 0.1
    condition_multiplier = {"excellent": 1.15, "good": 1.05, "fair": 0.95}[cv_features["room_condition"]]
    
    final_prediction = base_prediction * condition_multiplier * (1 + cv_boost)
    
    print(f"🔍 Computer Vision Analysis:")
    print(f"   Natural Light Score: {cv_features['natural_light_score']:.2f}")
    print(f"   Room Condition: {cv_features['room_condition']}")
    print(f"   Architectural Style: {cv_features['architectural_style']}")
    print(f"   Outdoor Space: {cv_features['outdoor_space']}")
    
    print(f"\n💰 ML Price Prediction: €{final_prediction:,.0f}")
    print(f"   Base Model: €{base_prediction:,}")
    print(f"   CV Enhancement: +{(cv_boost * 100):.1f}%")
    print(f"   Condition Adjust: {((condition_multiplier - 1) * 100):+.1f}%")
    
    print("\n✅ Beast Mode capabilities demonstrated!")
    
    return {
        "property_update": property_update,
        "cv_analysis": cv_features,
        "ml_prediction": final_prediction
    }

if __name__ == "__main__":
    async def main():
        # Test services
        services_healthy = await test_beast_mode_services()
        
        if services_healthy:
            # Demonstrate capabilities
            await demonstrate_beast_mode_capabilities()
            
            print("\n🎯 Ready for Phase 3 Development!")
            print("Next steps:")
            print("1. Build real-time property streaming pipeline")
            print("2. Implement computer vision models")
            print("3. Create interactive dashboards")
            print("4. Deploy on Kubernetes")
        else:
            print("\n🔧 Fix service issues first, then try again")
    
    asyncio.run(main())
EOF

chmod +x beast_mode_next_steps.py

print_success "Beast Mode recovery completed!"
echo ""
echo "🔥 NEXT ACTIONS:"
echo "1. Test services: python beast_mode_next_steps.py"
echo "2. Check logs: docker-compose -f docker-compose-beast-mode.yml logs"
echo "3. Access dashboards with updated ports"
echo ""
echo "💪 BEAST MODE IS RESILIENT - WE'VE GOT THIS! 🔥"