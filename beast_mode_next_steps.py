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
