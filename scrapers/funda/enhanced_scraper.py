# scrapers/funda/enhanced_scraper.py
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import logging
from datetime import datetime
import re
import json
from tenacity import retry, stop_after_attempt, wait_exponential
import asyncpg

logger = logging.getLogger(__name__)

class EnhancedFundaScraper:
    """
    Production-ready Funda scraper met:
    - Rate limiting en respectvolle scraping
    - Error handling en retry logic
    - Structured data extraction
    - Database integration
    """
    
    def __init__(self, max_concurrent: int = 3):  # Conservatief beginnen
        self.base_url = "https://www.funda.nl"
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Respectvolle headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'nl-NL,nl;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # Respectvolle delay tussen requests
        self.request_delay = 2.0  # 2 seconden
        
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self.session = aiohttp.ClientSession(
            headers=self.headers,
            timeout=timeout
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def fetch_page(self, url: str) -> str:
        """Fetch page with respectful rate limiting"""
        async with self.semaphore:
            try:
                await asyncio.sleep(self.request_delay)  # Rate limiting
                
                async with self.session.get(url) as response:
                    if response.status == 429:  # Too Many Requests
                        logger.warning("Rate limited, backing off...")
                        await asyncio.sleep(10)
                        raise aiohttp.ClientError("Rate limited")
                    
                    response.raise_for_status()
                    html = await response.text()
                    logger.info(f"Successfully fetched: {url}")
                    return html
                    
            except aiohttp.ClientError as e:
                logger.error(f"Error fetching {url}: {e}")
                raise
    
    async def scrape_city_listings(
        self, 
        city: str, 
        max_pages: int = 5,
        min_price: int = 100000,
        max_price: int = 1000000
    ) -> List[Dict]:
        """
        Scrape listings for a specific city with price filtering
        
        Waarom price filtering?
        - Focust op realistische data
        - Vermindert outliers in je ML model
        - Snellere scraping (minder pagina's)
        """
        all_listings = []
        
        for page in range(1, max_pages + 1):
            # Funda URL constructie met filters
            url = f"{self.base_url}/koop/{city}/p{page}/?price=%22{min_price}-{max_price}%22"
            
            try:
                html = await self.fetch_page(url)
                listings = self.parse_search_page(html, city)
                
                if not listings:
                    logger.info(f"No more listings found on page {page}")
                    break
                
                all_listings.extend(listings)
                logger.info(f"Scraped {len(listings)} listings from page {page}")
                
            except Exception as e:
                logger.error(f"Error on page {page}: {e}")
                continue
        
        logger.info(f"Total scraped: {len(all_listings)} listings for {city}")
        return all_listings
    
    def parse_search_page(self, html: str, city: str) -> List[Dict]:
        """Parse search results with robust extraction"""
        soup = BeautifulSoup(html, 'html.parser')
        listings = []
        
        # Funda gebruikt verschillende CSS selectors - we proberen meerdere
        selectors = [
            'div[data-test-id="search-result-item"]',
            'div.search-result',
            '[data-object-id]'
        ]
        
        items = []
        for selector in selectors:
            items = soup.select(selector)
            if items:
                break
        
        if not items:
            logger.warning("No listings found with any selector")
            return []
        
        for item in items:
            try:
                listing = self.extract_listing_data(item, city)
                if listing and self.validate_listing(listing):
                    listings.append(listing)
            except Exception as e:
                logger.warning(f"Error parsing listing: {e}")
                continue
        
        return listings
    
    def extract_listing_data(self, item, city: str) -> Optional[Dict]:
        """Extract structured data from listing element"""
        try:
            # Source identifiers
            source_id = item.get('data-object-id')
            if not source_id:
                # Backup: extract from URL
                link = item.find('a')
                if link and link.get('href'):
                    source_id = re.search(r'/(\d+)/', link['href'])
                    source_id = source_id.group(1) if source_id else None
            
            if not source_id:
                return None
            
            # Address extraction
            address_elem = item.find('h2') or item.select_one('[data-test-id="street-name-house-number"]')
            address = address_elem.get_text(strip=True) if address_elem else "Unknown"
            
            # Price extraction - multiple strategies
            price = self.extract_price(item)
            
            # Size extraction
            size_m2 = self.extract_size(item)
            
            # Rooms extraction
            rooms = self.extract_rooms(item)
            
            # Property type
            property_type = self.extract_property_type(item, address)
            
            # URL construction
            link_elem = item.find('a')
            detail_url = link_elem['href'] if link_elem and link_elem.get('href') else None
            
            # Postal code extraction from address
            postal_code = self.extract_postal_code(address)
            
            listing = {
                'source': 'funda',
                'source_id': source_id,
                'url': detail_url,
                'address': address,
                'postal_code': postal_code,
                'city': city,
                'price': price,
                'size_m2': size_m2,
                'rooms': rooms,
                'property_type': property_type,
                'listing_type': 'sale',
                'scraped_at': datetime.now(),
                'raw_html': str(item)[:1000]  # Store sample for debugging
            }
            
            return listing
            
        except Exception as e:
            logger.error(f"Error extracting listing data: {e}")
            return None
    
    def extract_price(self, item) -> Optional[float]:
        """Extract price with multiple fallback strategies"""
        price_selectors = [
            'span.search-result-price',
            '[data-test-id="price-label"]',
            '.object-price',
            'span[title*="Vraagprijs"]'
        ]
        
        for selector in price_selectors:
            elem = item.select_one(selector)
            if elem:
                price_text = elem.get_text(strip=True)
                price = self.parse_price_text(price_text)
                if price:
                    return price
        
        return None
    
    def parse_price_text(self, price_text: str) -> Optional[float]:
        """Parse Dutch price format: € 425.000 k.k."""
        try:
            # Remove common Dutch price suffixes
            price_clean = re.sub(r'[^\d,.]', '', price_text)
            price_clean = price_clean.replace('.', '').replace(',', '.')
            
            # Handle various formats
            if '.' in price_clean:
                parts = price_clean.split('.')
                if len(parts) == 2 and len(parts[1]) <= 2:
                    # Decimal format: 425.50
                    return float(price_clean)
                else:
                    # Thousands separator: 425.000
                    return float(price_clean.replace('.', ''))
            
            return float(price_clean)
            
        except (ValueError, AttributeError):
            return None
    
    def extract_size(self, item) -> Optional[int]:
        """Extract living area in m²"""
        size_patterns = [
            r'(\d+)\s*m²',
            r'(\d+)\s*m2',
            r'(\d+)\s*vierkante meter'
        ]
        
        text = item.get_text()
        for pattern in size_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return int(match.group(1))
        
        return None
    
    def extract_rooms(self, item) -> Optional[int]:
        """Extract number of rooms"""
        room_patterns = [
            r'(\d+)\s*kamer',
            r'(\d+)\s*room',
            r'(\d+)\s*slaapkamer'
        ]
        
        text = item.get_text()
        for pattern in room_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return int(match.group(1))
        
        return None
    
    def extract_property_type(self, item, address: str) -> str:
        """Determine property type from context"""
        text = item.get_text().lower()
        address_lower = address.lower()
        
        if any(word in text or word in address_lower for word in ['appartement', 'flat', 'studio']):
            return 'apartment'
        elif any(word in text or word in address_lower for word in ['woning', 'huis', 'villa', 'bungalow']):
            return 'house'
        elif any(word in text or word in address_lower for word in ['penthouse']):
            return 'penthouse'
        else:
            return 'unknown'
    
    def extract_postal_code(self, address: str) -> Optional[str]:
        """Extract Dutch postal code (1234 AB format)"""
        pattern = r'\b(\d{4}\s*[A-Za-z]{2})\b'
        match = re.search(pattern, address)
        return match.group(1).replace(' ', '') if match else None
    
    def validate_listing(self, listing: Dict) -> bool:
        """Validate listing has minimum required data"""
        required_fields = ['source_id', 'address', 'city']
        
        for field in required_fields:
            if not listing.get(field):
                return False
        
        # Price validation
        price = listing.get('price')
        if price and (price < 50000 or price > 5000000):
            logger.warning(f"Price outlier detected: {price}")
            return False
        
        return True

# Database integration class
class FundaDataProcessor:
    """
    Processes scraped data and stores in database
    
    Waarom een aparte class?
    - Separation of concerns (scraping vs storage)
    - Easier testing
    - Reusable for other scrapers
    """
    
    def __init__(self, database_url: str):
        self.database_url = database_url
    
    async def process_and_store(self, listings: List[Dict]) -> Dict[str, int]:
        """Process listings and store in database"""
        if not listings:
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        conn = await asyncpg.connect(self.database_url)
        stats = {'inserted': 0, 'updated': 0, 'errors': 0}
        
        try:
            for listing in listings:
                try:
                    # Upsert query (insert or update if exists)
                    result = await conn.execute("""
                        INSERT INTO raw.property_listings 
                        (source, source_id, url, address, postal_code, city, 
                         price, size_m2, rooms, property_type, listing_type, scraped_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                        ON CONFLICT (source, source_id) 
                        DO UPDATE SET 
                            price = EXCLUDED.price,
                            scraped_at = EXCLUDED.scraped_at,
                            updated_at = NOW()
                        RETURNING (xmax = 0) AS inserted
                    """, 
                    listing['source'], listing['source_id'], listing['url'],
                    listing['address'], listing['postal_code'], listing['city'],
                    listing['price'], listing['size_m2'], listing['rooms'],
                    listing['property_type'], listing['listing_type'], listing['scraped_at']
                    )
                    
                    # Track if it was insert or update
                    if result == "INSERT 0 1":
                        stats['inserted'] += 1
                    else:
                        stats['updated'] += 1
                        
                except Exception as e:
                    logger.error(f"Error storing listing {listing.get('source_id')}: {e}")
                    stats['errors'] += 1
        
        finally:
            await conn.close()
        
        return stats

# Main scraper function for Airflow
async def scrape_funda_cities(cities: List[str] = None, max_pages: int = 3):
    """
    Main function to scrape multiple cities
    
    Deze functie wordt later aangeroepen door Airflow DAG
    """
    if not cities:
        cities = ['amsterdam', 'rotterdam', 'utrecht', 'den-haag']
    
    database_url = "postgresql://postgres:postgres@postgres:5432/real_estate"
    
    async with EnhancedFundaScraper() as scraper:
        processor = FundaDataProcessor(database_url)
        
        total_stats = {'inserted': 0, 'updated': 0, 'errors': 0}
        
        for city in cities:
            logger.info(f"Scraping {city}...")
            
            try:
                listings = await scraper.scrape_city_listings(
                    city=city, 
                    max_pages=max_pages
                )
                
                if listings:
                    stats = await processor.process_and_store(listings)
                    
                    # Accumulate stats
                    for key in total_stats:
                        total_stats[key] += stats[key]
                    
                    logger.info(f"{city}: {stats}")
                
            except Exception as e:
                logger.error(f"Failed to scrape {city}: {e}")
        
        logger.info(f"Total stats: {total_stats}")
        return total_stats

if __name__ == "__main__":
    # Test run
    asyncio.run(scrape_funda_cities(['amsterdam'], max_pages=1))