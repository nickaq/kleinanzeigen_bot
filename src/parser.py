"""HTML parser for Kleinanzeigen search and listing pages."""

import logging
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .config import KNOWN_BRANDS, Config

logger = logging.getLogger(__name__)

BASE_URL = "https://www.kleinanzeigen.de"


@dataclass
class ListingPreview:
    """Preview data from search results page."""
    listing_id: str
    url: str
    title: str


@dataclass
class ListingDetails:
    """Full listing details from listing page."""
    listing_id: str
    url: str
    title: str
    brand: str
    year: str
    price: str
    location: str
    plz: str
    
    def format_message(self) -> str:
        """Format listing as Telegram message."""
        # Use full title instead of just brand
        title_display = self.title if self.title and self.title != "—" else self.brand
        year_display = self.year if self.year != "—" else "—"
        
        return (
            f"🚗 {title_display}\n"
            f"📅 Год: {year_display}\n"
            f"📍 {self.location}\n"
            f"🏷 PLZ: {self.plz}\n"
            f"💶 Цена: {self.price}\n"
            f"\n"
            f"🔗 {self.url}"
        )


class Parser:
    """HTML parser for Kleinanzeigen pages."""
    
    # Regex patterns
    YEAR_PATTERN = re.compile(r'\b(19[7-9]\d|20[0-2]\d)\b')
    PLZ_PATTERN = re.compile(r'\b(\d{5})\b')
    LISTING_ID_PATTERN = re.compile(r'/s-anzeige/[^/]+/(\d+)-')
    
    def parse_search_page(self, html: str, max_listings: Optional[int] = None) -> list[ListingPreview]:
        """
        Parse search results page and extract listing previews.
        
        Args:
            html: Raw HTML content
            max_listings: Maximum number of listings to extract
            
        Returns:
            List of ListingPreview objects
        """
        max_listings = max_listings or Config.MAX_LISTINGS_PER_QUERY
        soup = BeautifulSoup(html, 'lxml')
        listings = []
        
        # Find all listing articles/cards
        # Kleinanzeigen uses article elements with data-adid attribute
        articles = soup.find_all('article', attrs={'data-adid': True})
        
        if not articles:
            # Fallback: try to find listing links directly
            articles = soup.find_all('a', href=re.compile(r'/s-anzeige/'))
        
        seen_ids = set()
        
        for article in articles:
            if len(listings) >= max_listings:
                break
            
            try:
                # Try to get listing ID from data-adid attribute
                listing_id = None
                url = None
                title = ""
                
                if hasattr(article, 'get') and article.get('data-adid'):
                    listing_id = article['data-adid']
                
                # Find the link to the listing
                if article.name == 'a':
                    link = article
                else:
                    link = article.find('a', href=re.compile(r'/s-anzeige/'))
                
                if link and link.get('href'):
                    href = link['href']
                    url = urljoin(BASE_URL, href)
                    title = link.get_text(strip=True)[:100]  # Limit title length
                    
                    # Extract ID from URL if not found
                    if not listing_id:
                        match = self.LISTING_ID_PATTERN.search(href)
                        if match:
                            listing_id = match.group(1)
                
                if listing_id and url and listing_id not in seen_ids:
                    seen_ids.add(listing_id)
                    listings.append(ListingPreview(
                        listing_id=listing_id,
                        url=url,
                        title=title
                    ))
                    
            except Exception as e:
                logger.warning(f"Error parsing listing article: {e}")
                continue
        
        logger.info(f"Parsed {len(listings)} listings from search page")
        return listings
    
    def parse_listing_page(self, html: str, url: str, listing_id: str) -> ListingDetails:
        """
        Parse listing detail page and extract all fields.
        
        Args:
            html: Raw HTML content
            url: Listing URL
            listing_id: Listing ID
            
        Returns:
            ListingDetails object with all extracted fields
        """
        soup = BeautifulSoup(html, 'lxml')
        
        # Extract title
        title = self._extract_title(soup)
        
        # Extract structured data fields
        brand = self._extract_brand(soup, title)
        year = self._extract_year(soup, title)
        price = self._extract_price(soup)
        location = self._extract_location(soup)
        plz = self._extract_plz(soup, location)
        
        return ListingDetails(
            listing_id=listing_id,
            url=url,
            title=title,
            brand=brand,
            year=year,
            price=price,
            location=location,
            plz=plz
        )
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract listing title."""
        # Try h1 heading first
        h1 = soup.find('h1')
        if h1:
            return h1.get_text(strip=True)
        
        # Try og:title meta tag
        og_title = soup.find('meta', property='og:title')
        if og_title and og_title.get('content'):
            return og_title['content']
        
        # Fallback to title tag
        title_tag = soup.find('title')
        if title_tag:
            return title_tag.get_text(strip=True).split('|')[0].strip()
        
        return "—"
    
    def _extract_brand(self, soup: BeautifulSoup, title: str) -> str:
        """Extract car brand from title or structured data."""
        title_lower = title.lower()
        
        # Priority 1: Search in title for known brands (most reliable)
        for known_brand in KNOWN_BRANDS:
            if known_brand.lower() in title_lower:
                return known_brand
        
        # Check for VW as Volkswagen
        if 'vw ' in title_lower or title_lower.startswith('vw') or ' vw' in title_lower:
            return "Volkswagen"
        
        # Priority 2: Try structured "Marke" field from page
        brand = self._find_detail_value(soup, "Marke")
        if brand and brand != "—" and len(brand) < 50:
            brand = brand.strip()
            # Remove "Modell" if it got concatenated
            if brand.lower().startswith("modell"):
                brand = brand[6:].strip()
            if brand:
                return brand
        
        # Priority 3: Try first word from title as brand
        first_word = title.split()[0] if title.split() else ""
        for known_brand in KNOWN_BRANDS:
            if first_word.lower() == known_brand.lower():
                return known_brand
        
        return "—"
    
    def _extract_year(self, soup: BeautifulSoup, title: str) -> str:
        """Extract car year from structured data or title."""
        # Try "Erstzulassung" field (e.g., "Oktober 1991")
        erstzulassung = self._find_detail_value(soup, "Erstzulassung")
        if erstzulassung:
            match = self.YEAR_PATTERN.search(erstzulassung)
            if match:
                return match.group(1)
        
        # Try "Baujahr" field
        baujahr = self._find_detail_value(soup, "Baujahr")
        if baujahr:
            match = self.YEAR_PATTERN.search(baujahr)
            if match:
                return match.group(1)
        
        # Fallback: search for year in title
        match = self.YEAR_PATTERN.search(title)
        if match:
            return match.group(1)
        
        return "—"
    
    def _extract_price(self, soup: BeautifulSoup) -> str:
        """Extract price from listing."""
        # Try to find price element with € symbol
        price_patterns = [
            soup.find('h2', string=re.compile(r'[\d.,]+\s*€')),
            soup.find('span', string=re.compile(r'[\d.,]+\s*€')),
            soup.find(class_=re.compile(r'price|preis', re.I)),
        ]
        
        for element in price_patterns:
            if element:
                text = element.get_text(strip=True)
                # Extract price with currency
                match = re.search(r'([\d.,]+)\s*€', text)
                if match:
                    return f"{match.group(1)} €"
        
        # Try meta tags
        price_meta = soup.find('meta', {'itemprop': 'price'})
        if price_meta and price_meta.get('content'):
            return f"{price_meta['content']} €"
        
        return "—"
    
    def _extract_location(self, soup: BeautifulSoup) -> str:
        """Extract location from listing."""
        # Try to find location from page title (format: "Title in Region - City")
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text()
            # Pattern: "... in Region - City | kleinanzeigen.de"
            match = re.search(r' in ([^|]+?)(?:\s*\||\s*$)', title_text)
            if match:
                location = match.group(1).strip()
                if location:
                    return location
        
        # Try location span/div with class containing 'location'
        location_elements = soup.find_all(class_=re.compile(r'location|standort|adress', re.I))
        for elem in location_elements:
            text = elem.get_text(strip=True)
            if text and len(text) > 3:
                return text[:100]  # Limit length
        
        # Try itemprops
        address_elem = soup.find(itemprop='address')
        if address_elem:
            return address_elem.get_text(strip=True)
        
        return "—"
    
    def _extract_plz(self, soup: BeautifulSoup, location: str) -> str:
        """Extract postal code (PLZ)."""
        # First try to find in location string
        match = self.PLZ_PATTERN.search(location)
        if match:
            return match.group(1)
        
        # Try to find dedicated PLZ element
        plz_elements = soup.find_all(string=self.PLZ_PATTERN)
        for elem in plz_elements:
            match = self.PLZ_PATTERN.search(str(elem))
            if match:
                return match.group(1)
        
        # Try postal code itemprops
        postal_elem = soup.find(itemprop='postalCode')
        if postal_elem:
            text = postal_elem.get_text(strip=True)
            match = self.PLZ_PATTERN.search(text)
            if match:
                return match.group(1)
        
        return "—"
    
    def _find_detail_value(self, soup: BeautifulSoup, label: str) -> Optional[str]:
        """Find a structured detail value by its label."""
        # Search for label text and get adjacent value
        label_elem = soup.find(string=re.compile(rf'^\s*{re.escape(label)}\s*$', re.I))
        if label_elem:
            parent = label_elem.find_parent()
            if parent:
                # Look for sibling or next element with value
                next_elem = parent.find_next_sibling()
                if next_elem:
                    return next_elem.get_text(strip=True)
                # Try parent's next sibling
                parent_next = parent.parent.find_next_sibling() if parent.parent else None
                if parent_next:
                    return parent_next.get_text(strip=True)
        
        # Try data structure with label/value pairs
        for li in soup.find_all('li'):
            text = li.get_text()
            if label.lower() in text.lower():
                # Extract value after label
                parts = re.split(rf'{re.escape(label)}[:\s]*', text, flags=re.I)
                if len(parts) > 1:
                    return parts[1].strip()
        
        return None
