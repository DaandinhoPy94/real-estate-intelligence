# api/main.py
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
import asyncpg
import redis.asyncio as redis
from contextlib import asynccontextmanager
import os
from typing import Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global connections
db_pool = None
redis_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown"""
    global db_pool, redis_client
    
    # Startup
    try:
        # Database connection pool
        db_pool = await asyncpg.create_pool(
            os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/real_estate"),
            min_size=5,
            max_size=20
        )
        logger.info("Database pool created")
        
        # Redis connection
        redis_client = redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379"),
            decode_responses=True
        )
        await redis_client.ping()
        logger.info("Redis connection established")
        
    except Exception as e:
        logger.error(f"Startup error: {e}")
        raise
    
    yield
    
    # Shutdown
    if db_pool:
        await db_pool.close()
    if redis_client:
        await redis_client.close()

# Create FastAPI app
app = FastAPI(
    title="Real Estate Intelligence API",
    description="Production-grade API for real estate market intelligence and ML predictions",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

# Dependency injection
async def get_db_connection():
    """Get database connection from pool"""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")
    async with db_pool.acquire() as connection:
        yield connection

async def get_redis_client():
    """Get Redis client"""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")
    return redis_client

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    checks = {
        "status": "healthy",
        "database": "unknown",
        "redis": "unknown"
    }
    
    # Check database
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        checks["database"] = "healthy"
    except Exception as e:
        checks["database"] = f"unhealthy: {str(e)}"
        checks["status"] = "degraded"
    
    # Check Redis
    try:
        await redis_client.ping()
        checks["redis"] = "healthy"
    except Exception as e:
        checks["redis"] = f"unhealthy: {str(e)}"
        checks["status"] = "degraded"
    
    status_code = 200 if checks["status"] == "healthy" else 503
    return JSONResponse(content=checks, status_code=status_code)

# Root endpoint
@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "message": "Real Estate Intelligence API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }

# Market data endpoints
@app.get("/api/v1/market/overview")
async def market_overview(
    city: str = None,
    postal_code: str = None,
    db: asyncpg.Connection = Depends(get_db_connection)
):
    """Get market overview statistics"""
    try:
        query = """
        SELECT 
            COUNT(*) as total_listings,
            AVG(price) as avg_price,
            AVG(price/NULLIF(size_m2, 0)) as avg_price_per_m2,
            MIN(price) as min_price,
            MAX(price) as max_price
        FROM raw.property_listings
        WHERE scraped_at >= NOW() - INTERVAL '7 days'
        """
        
        params = []
        if city:
            query += " AND city = $1"
            params.append(city)
        elif postal_code:
            query += " AND postal_code = $1"
            params.append(postal_code)
        
        row = await db.fetchrow(query, *params)
        
        return {
            "total_listings": row['total_listings'],
            "avg_price": float(row['avg_price']) if row['avg_price'] else None,
            "avg_price_per_m2": float(row['avg_price_per_m2']) if row['avg_price_per_m2'] else None,
            "min_price": float(row['min_price']) if row['min_price'] else None,
            "max_price": float(row['max_price']) if row['max_price'] else None
        }
        
    except Exception as e:
        logger.error(f"Error in market_overview: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/v1/properties/search")
async def search_properties(
    city: str = None,
    min_price: float = None,
    max_price: float = None,
    min_size: int = None,
    max_size: int = None,
    property_type: str = None,
    limit: int = 100,
    offset: int = 0,
    db: asyncpg.Connection = Depends(get_db_connection)
):
    """Search properties with filters"""
    try:
        query = """
        SELECT 
            id, address, postal_code, city, price, size_m2, 
            rooms, property_type, listed_date, scraped_at
        FROM raw.property_listings
        WHERE price > 0 AND size_m2 > 0
        """
        
        params = []
        param_count = 0
        
        if city:
            param_count += 1
            query += f" AND city ILIKE ${param_count}"
            params.append(f"%{city}%")
        
        if min_price:
            param_count += 1
            query += f" AND price >= ${param_count}"
            params.append(min_price)
        
        if max_price:
            param_count += 1
            query += f" AND price <= ${param_count}"
            params.append(max_price)
        
        if min_size:
            param_count += 1
            query += f" AND size_m2 >= ${param_count}"
            params.append(min_size)
        
        if max_size:
            param_count += 1
            query += f" AND size_m2 <= ${param_count}"
            params.append(max_size)
        
        if property_type:
            param_count += 1
            query += f" AND property_type = ${param_count}"
            params.append(property_type)
        
        query += f" ORDER BY scraped_at DESC LIMIT ${param_count + 1} OFFSET ${param_count + 2}"
        params.extend([limit, offset])
        
        rows = await db.fetch(query, *params)
        
        properties = []
        for row in rows:
            properties.append({
                "id": str(row['id']),
                "address": row['address'],
                "postal_code": row['postal_code'],
                "city": row['city'],
                "price": float(row['price']) if row['price'] else None,
                "size_m2": row['size_m2'],
                "rooms": row['rooms'],
                "property_type": row['property_type'],
                "listed_date": row['listed_date'].isoformat() if row['listed_date'] else None,
                "scraped_at": row['scraped_at'].isoformat()
            })
        
        return {
            "properties": properties,
            "count": len(properties),
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error(f"Error in search_properties: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Include additional routers
from api.routers import predictions, analytics

app.include_router(predictions.router, prefix="/api/v1/predictions", tags=["predictions"])
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["analytics"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )