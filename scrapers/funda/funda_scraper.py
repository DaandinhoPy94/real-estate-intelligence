# scrapers/funda/funda_scraper.py
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import logging
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
import json

logger = logging.getLogger(__name__)

class FundaScraper:
    """
    Async scraper voor Funda met rate limiting en error handling
    """
    
    def __init__(self, max_concurrent_requests: int = 5):
        self.base_url = "https://www.funda.nl"
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'nl-NL,nl;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def fetch_page(self, url: str) -> str:
        """
        Fetch page with retry logic
        """
        async with self.semaphore:
            try:
                async with self.session.get(url) as response:
                    response.raise_for_status()
                    return await response.text()
            except aiohttp.ClientError as e:
                logger.error(f"Error fetching {url}: {e}")
                raise
    
    async def scrape_search_results(
        self, 
        city: str, 
        property_type: str = "koop",
        max_pages: int = 10
    ) -> List[Dict]:
        """
        Scrape search results for a city
        """
        results = []
        
        for page in range(1, max_pages + 1):
            url = f"{self.base_url}/koop/{city}/p{page}/"
            
            try:
                html = await self.fetch_page(url)
                listings = self.parse_search_page(html)
                
                if not listings:
                    break
                
                results.extend(listings)
                
                # Rate limiting
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error scraping page {page}: {e}")
                continue
        
        return results
    
    def parse_search_page(self, html: str) -> List[Dict]:
        """
        Parse search results page
        """
        soup = BeautifulSoup(html, 'html.parser')
        listings = []
        
        for item in soup.find_all('div', class_='search-result'):
            try:
                listing = {
                    'source': 'funda',
                    'source_id': item.get('data-object-id'),
                    'url': item.find('a')['href'],
                    'address': item.find('h2').text.strip(),
                    'postal_code': self.extract_postal_code(item),
                    'city': item.find('div', class_='search-result__location').text.strip(),
                    'price': self.parse_price(item.find('span', class_='search-result__price').text),
                    'size_m2': self.parse_size(item),
                    'rooms': self.parse_rooms(item),
                    'property_type': self.extract_property_type(item),
                    'listed_date': datetime.now(),
                    'scraped_at': datetime.now()
                }
                listings.append(listing)
            except Exception as e:
                logger.warning(f"Error parsing listing: {e}")
                continue
        
        return listings
    
    async def scrape_listing_details(self, listing_url: str) -> Dict:
        """
        Scrape detailed information from listing page
        """
        try:
            html = await self.fetch_page(f"{self.base_url}{listing_url}")
            soup = BeautifulSoup(html, 'html.parser')
            
            details = {
                'description': self.extract_description(soup),
                'energy_label': self.extract_energy_label(soup),
                'build_year': self.extract_build_year(soup),
                'features': self.extract_features(soup),
                'photos': self.extract_photo_urls(soup),
                'coordinates': self.extract_coordinates(soup),
            }
            
            return details
            
        except Exception as e:
            logger.error(f"Error scraping details for {listing_url}: {e}")
            return {}
    
    def parse_price(self, price_text: str) -> Optional[float]:
        """
        Parse price from text like '€ 425.000 k.k.'
        """
        try:
            price = price_text.replace('€', '').replace('.', '').replace('k.k.', '')
            price = price.strip().replace(',', '.')
            return float(price)
        except:
            return None
    
    def parse_size(self, item) -> Optional[int]:
        """
        Extract size in m2
        """
        try:
            size_elem = item.find('span', title=lambda x: x and 'm²' in x)
            if size_elem:
                return int(size_elem.text.replace('m²', '').strip())
        except:
            pass
        return None
    
    def parse_rooms(self, item) -> Optional[int]:
        """
        Extract number of rooms
        """
        try:
            rooms_elem = item.find('li', title=lambda x: x and 'kamer' in x)
            if rooms_elem:
                return int(rooms_elem.text.split()[0])
        except:
            pass
        return None
    
    def extract_postal_code(self, item) -> Optional[str]:
        """
        Extract postal code from address
        """
        # Implementation depends on HTML structure
        pass
    
    def extract_property_type(self, item) -> str:
        """
        Determine property type
        """
        # Implementation
        pass
    
    def extract_coordinates(self, soup) -> Optional[Dict[str, float]]:
        """
        Extract lat/lon from page
        """
        try:
            map_data = soup.find('div', {'data-map-config': True})
            if map_data:
                config = json.loads(map_data['data-map-config'])
                return {
                    'latitude': config.get('lat'),
                    'longitude': config.get('lng')
                }
        except:
            pass
        return None

# Async batch processor
class BatchProcessor:
    """
    Process scraped data in batches
    """
    
    def __init__(self, database_url: str, batch_size: int = 100):
        self.database_url = database_url
        self.batch_size = batch_size
        self.buffer = []
    
    async def process_listing(self, listing: Dict):
        """
        Add listing to buffer and flush if needed
        """
        self.buffer.append(listing)
        
        if len(self.buffer) >= self.batch_size:
            await self.flush()
    
    async def flush(self):
        """
        Write buffer to database
        """
        if not self.buffer:
            return
        
        # Use asyncpg for fast inserts
        import asyncpg
        conn = await asyncpg.connect(self.database_url)
        
        try:
            await conn.executemany(
                """
                INSERT INTO property_listings 
                (source, source_id, url, address, postal_code, city, price, size_m2, rooms, scraped_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (source, source_id) DO UPDATE
                SET price = EXCLUDED.price,
                    scraped_at = EXCLUDED.scraped_at
                """,
                [(l['source'], l['source_id'], l['url'], l['address'], 
                  l['postal_code'], l['city'], l['price'], l['size_m2'], 
                  l['rooms'], l['scraped_at']) for l in self.buffer]
            )
            logger.info(f"Inserted {len(self.buffer)} listings")
            self.buffer = []
            
        finally:
            await conn.close()