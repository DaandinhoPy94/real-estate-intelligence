# api/test_main_complete.py - Complete working API
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import asyncpg
import os
from datetime import datetime, timedelta

app = FastAPI(title="Real Estate Intelligence API - Complete", version="1.0.0")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/real_estate")

async def get_db_connection():
    return await asyncpg.connect(DATABASE_URL)

@app.get("/")
async def root():
    return {"message": "Real Estate Intelligence API", "version": "1.0.0", "status": "running"}

@app.get("/health")
async def health():
    checks = {"status": "healthy", "database": "unknown", "api": "running"}
    try:
        conn = await get_db_connection()
        await conn.fetchval("SELECT 1")
        await conn.close()
        checks["database"] = "connected"
    except Exception as e:
        checks["database"] = f"error: {str(e)}"
        checks["status"] = "degraded"
    return checks

@app.get("/api/v1/test-data")
async def test_data():
    try:
        conn = await get_db_connection()
        rows = await conn.fetch("SELECT * FROM raw.property_listings ORDER BY scraped_at DESC LIMIT 10")
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
                "property_type": row['property_type']
            })
        
        return {
            "message": "Sample property data",
            "count": len(properties),
            "properties": properties
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/v1/market/overview")
async def market_overview(city: Optional[str] = None, postal_code: Optional[str] = None):
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
        raise HTTPException(status_code=500, detail=f"Market overview error: {str(e)}")

@app.get("/api/v1/properties/search")
async def search_properties(
    city: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    limit: int = 100
):
    try:
        conn = await get_db_connection()
        
        query = "SELECT * FROM raw.property_listings WHERE price > 0"
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
        
        query += f" ORDER BY scraped_at DESC LIMIT ${param_count + 1}"
        params.append(limit)
        
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
                "property_type": row['property_type']
            })
        
        return {
            "filters": {"city": city, "min_price": min_price, "max_price": max_price},
            "properties": properties,
            "count": len(properties)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")

# Prediction endpoints
class PropertyFeatures(BaseModel):
    address: str
    postal_code: str
    city: str
    size_m2: int
    rooms: Optional[int] = None
    property_type: Optional[str] = "apartment"
    has_garden: Optional[bool] = False
    has_parking: Optional[bool] = False
    build_year: Optional[int] = None

class PricePrediction(BaseModel):
    predicted_price: float
    confidence_lower: float
    confidence_upper: float
    model_version: str
    features_used: List[str]

@app.post("/api/v1/predictions/price", response_model=PricePrediction)
async def predict_price(property_features: PropertyFeatures):
    # Simple pricing model
    base_price = property_features.size_m2 * 3500  # €3500 per m2
    
    # City adjustments
    if property_features.city.lower() == 'amsterdam':
        base_price *= 1.3
    elif property_features.city.lower() == 'rotterdam':
        base_price *= 1.1
    elif property_features.city.lower() == 'utrecht':
        base_price *= 1.2
    
    # Feature adjustments
    if property_features.has_garden:
        base_price *= 1.1
    if property_features.has_parking:
        base_price *= 1.05
    if property_features.build_year and property_features.build_year > 2000:
        base_price *= 1.15
    
    confidence_range = base_price * 0.1
    
    return PricePrediction(
        predicted_price=round(base_price, 2),
        confidence_lower=round(base_price - confidence_range, 2),
        confidence_upper=round(base_price + confidence_range, 2),
        model_version="v1.0",
        features_used=["size_m2", "city", "has_garden", "has_parking", "build_year"]
    )

# Analytics endpoints
@app.get("/api/v1/analytics/trends/price")
async def price_trends(city: Optional[str] = None, days: int = 30):
    dates = []
    prices = []
    
    base_date = datetime.now() - timedelta(days=days)
    base_price = 350000 if not city else (450000 if city.lower() == 'amsterdam' else 320000)
    
    for i in range(days):
        current_date = base_date + timedelta(days=i)
        trend_factor = 1 + (i * 0.001)
        price = base_price * trend_factor
        
        dates.append(current_date.strftime("%Y-%m-%d"))
        prices.append(round(price, 2))
    
    return {
        "period": {"start_date": dates[0], "end_date": dates[-1], "days": days},
        "location": {"city": city},
        "data": [{"date": date, "avg_price": price} for date, price in zip(dates, prices)],
        "summary": {
            "start_price": prices[0],
            "end_price": prices[-1],
            "change_percentage": round(((prices[-1] / prices[0]) - 1) * 100, 2)
        }
    }

@app.get("/api/v1/analytics/market/inventory")
async def market_inventory():
    return {
        "total_listings": 1250,
        "new_this_week": 85,
        "avg_days_on_market": 28,
        "inventory_by_price_range": [
            {"range": "0-300k", "count": 320, "percentage": 25.6},
            {"range": "300k-500k", "count": 450, "percentage": 36.0},
            {"range": "500k-750k", "count": 280, "percentage": 22.4},
            {"range": "750k+", "count": 200, "percentage": 16.0}
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)