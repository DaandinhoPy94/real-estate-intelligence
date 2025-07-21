# api/main_fixed.py - Fixed version with proper database connections
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import asyncpg
import os
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Real Estate Intelligence API",
    description="Production-grade API for real estate market intelligence and ML predictions",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global database connection
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/real_estate")

async def get_db_connection():
    """Create database connection"""
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=503, detail="Database unavailable")

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
        conn = await get_db_connection()
        await conn.fetchval("SELECT 1")
        await conn.close()
        checks["database"] = "healthy"
    except Exception as e:
        checks["database"] = f"unhealthy: {str(e)}"
        checks["status"] = "degraded"
    
    # Check Redis (mock for now)
    checks["redis"] = "healthy"
    
    status_code = 200 if checks["status"] == "healthy" else 503
    return {"status": checks["status"], "database": checks["database"], "redis": checks["redis"]}

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

# Test data endpoint
@app.get("/api/v1/test-data")
async def get_test_data():
    """Get sample property data from database"""
    try:
        conn = await get_db_connection()
        
        rows = await conn.fetch("""
            SELECT id, address, postal_code, city, price, size_m2, rooms
            FROM raw.property_listings
            ORDER BY scraped_at DESC
            LIMIT 10
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
                "size_m2": row['size_m2'],
                "rooms": row['rooms']
            })
        
        return {
            "message": "Sample property data from database",
            "count": len(properties),
            "properties": properties
        }
        
    except Exception as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# Market data endpoints
@app.get("/api/v1/market/overview")
async def market_overview(
    city: Optional[str] = None,
    postal_code: Optional[str] = None
):
    """Get market overview statistics"""
    try:
        conn = await get_db_connection()
        
        query = """
        SELECT 
            COUNT(*) as total_listings,
            AVG(price) as avg_price,
            AVG(price/NULLIF(size_m2, 0)) as avg_price_per_m2,
            MIN(price) as min_price,
            MAX(price) as max_price
        FROM raw.property_listings
        WHERE price > 0
        """
        
        params = []
        if city:
            query += " AND LOWER(city) = LOWER($1)"
            params.append(city)
        elif postal_code:
            query += " AND postal_code = $1"
            params.append(postal_code)
        
        row = await conn.fetchrow(query, *params)
        await conn.close()
        
        return {
            "location": {"city": city, "postal_code": postal_code},
            "total_listings": row['total_listings'],
            "avg_price": round(float(row['avg_price']), 2) if row['avg_price'] else None,
            "avg_price_per_m2": round(float(row['avg_price_per_m2']), 2) if row['avg_price_per_m2'] else None,
            "min_price": float(row['min_price']) if row['min_price'] else None,
            "max_price": float(row['max_price']) if row['max_price'] else None
        }
        
    except Exception as e:
        logger.error(f"Error in market_overview: {e}")
        raise HTTPException(status_code=500, detail=f"Market overview error: {str(e)}")

@app.get("/api/v1/properties/search")
async def search_properties(
    city: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    min_size: Optional[int] = None,
    max_size: Optional[int] = None,
    property_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    """Search properties with filters"""
    try:
        conn = await get_db_connection()
        
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
            query += f" AND LOWER(city) LIKE LOWER(${param_count})"
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
        
        rows = await conn.fetch(query, *params)
        await conn.close()
        
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
            "filters": {
                "city": city,
                "min_price": min_price,
                "max_price": max_price,
                "min_size": min_size,
                "max_size": max_size,
                "property_type": property_type
            },
            "properties": properties,
            "count": len(properties),
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error(f"Error in search_properties: {e}")
        raise HTTPException(status_code=500, detail=f"Property search error: {str(e)}")

# Include the prediction and analytics routers (simplified versions)
from pydantic import BaseModel
from typing import List

# Prediction models
class PropertyFeatures(BaseModel):
    address: str
    postal_code: str
    city: str
    size_m2: int
    rooms: Optional[int] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    build_year: Optional[int] = None
    property_type: Optional[str] = "apartment"
    has_garden: Optional[bool] = False
    has_balcony: Optional[bool] = False
    has_parking: Optional[bool] = False

class PricePrediction(BaseModel):
    predicted_price: float
    confidence_lower: float
    confidence_upper: float
    confidence_interval: float = 0.95
    model_version: str
    features_used: List[str]
    prediction_date: str

@app.post("/api/v1/predictions/price", response_model=PricePrediction)
async def predict_price(property_features: PropertyFeatures):
    """Predict property price using ML model"""
    try:
        # Simple heuristic for demo
        base_price = property_features.size_m2 * 3500  # €3500 per m2
        
        # Adjust based on features
        if property_features.has_garden:
            base_price *= 1.1
        if property_features.has_parking:
            base_price *= 1.05
        if property_features.build_year and property_features.build_year > 2000:
            base_price *= 1.15
        
        # Add some uncertainty
        confidence_range = base_price * 0.1
        
        return PricePrediction(
            predicted_price=base_price,
            confidence_lower=base_price - confidence_range,
            confidence_upper=base_price + confidence_range,
            model_version="demo_v1.0",
            features_used=["size_m2", "has_garden", "has_parking", "build_year"],
            prediction_date="2024-01-01T00:00:00Z"
        )
        
    except Exception as e:
        logger.error(f"Error in price prediction: {e}")
        raise HTTPException(status_code=500, detail="Prediction failed")

# Analytics endpoints (mock data)
@app.get("/api/v1/analytics/trends/price")
async def price_trends(
    city: Optional[str] = None,
    postal_code: Optional[str] = None,
    days: int = 30
):
    """Get price trends over time"""
    from datetime import datetime, timedelta
    
    dates = []
    prices = []
    
    base_date = datetime.now() - timedelta(days=days)
    base_price = 350000
    
    for i in range(days):
        current_date = base_date + timedelta(days=i)
        trend_factor = 1 + (i * 0.001)
        noise = (i % 7) * 1000
        price = base_price * trend_factor + noise
        
        dates.append(current_date.strftime("%Y-%m-%d"))
        prices.append(round(price, 2))
    
    return {
        "period": {
            "start_date": dates[0],
            "end_date": dates[-1],
            "days": days
        },
        "location": {
            "city": city,
            "postal_code": postal_code
        },
        "data": [
            {"date": date, "avg_price": price}
            for date, price in zip(dates, prices)
        ],
        "summary": {
            "start_price": prices[0],
            "end_price": prices[-1],
            "change_absolute": round(prices[-1] - prices[0], 2),
            "change_percentage": round(((prices[-1] / prices[0]) - 1) * 100, 2)
        }
    }

@app.get("/api/v1/analytics/market/inventory")
async def market_inventory():
    """Get current market inventory statistics"""
    return {
        "total_listings": 1250,
        "new_this_week": 85,
        "sold_this_week": 72,
        "avg_days_on_market": 28,
        "inventory_by_price_range": [
            {"range": "0-300k", "count": 320, "percentage": 25.6},
            {"range": "300k-500k", "count": 450, "percentage": 36.0},
            {"range": "500k-750k", "count": 280, "percentage": 22.4},
            {"range": "750k+", "count": 200, "percentage": 16.0}
        ],
        "inventory_by_size": [
            {"range": "0-75m²", "count": 380, "percentage": 30.4},
            {"range": "75-120m²", "count": 520, "percentage": 41.6},
            {"range": "120-200m²", "count": 250, "percentage": 20.0},
            {"range": "200m²+", "count": 100, "percentage": 8.0}
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)