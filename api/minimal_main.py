# api/minimal_main.py - Simplified version for testing
from fastapi import FastAPI
import asyncpg
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Real Estate Intelligence API - Minimal",
    version="1.0.0"
)

@app.get("/")
async def root():
    return {"message": "Real Estate Intelligence API is running!"}

@app.get("/health")
async def health_check():
    checks = {
        "status": "healthy",
        "database": "unknown",
        "api": "running"
    }
    
    # Test database connection
    try:
        database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/real_estate")
        conn = await asyncpg.connect(database_url)
        result = await conn.fetchval("SELECT 'connected' as status")
        await conn.close()
        checks["database"] = "connected"
    except Exception as e:
        checks["database"] = f"error: {str(e)}"
        checks["status"] = "degraded"
    
    return checks

@app.get("/api/v1/test-data")
async def get_test_data():
    """Get sample property data"""
    try:
        database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/real_estate")
        conn = await asyncpg.connect(database_url)
        
        rows = await conn.fetch("""
            SELECT id, address, postal_code, city, price, size_m2
            FROM raw.property_listings
            LIMIT 5
        """)
        
        await conn.close()
        
        properties = []
        for row in rows:
            properties.append({
                "id": str(row['id']),
                "address": row['address'],
                "postal_code": row['postal_code'],
                "city": row['city'],
                "price": float(row['price']) if row['price'] else None,
                "size_m2": row['size_m2']
            })
        
        return {
            "message": "Sample property data",
            "count": len(properties),
            "properties": properties
        }
        
    except Exception as e:
        logger.error(f"Database error: {e}")
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)