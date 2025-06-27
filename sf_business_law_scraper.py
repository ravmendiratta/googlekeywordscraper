import requests
import json
import pandas as pd
import time
from typing import List, Dict
import csv


class GoogleSearchScraper:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://google.serper.dev/search"
        self.headers = {
            'X-API-KEY': api_key,
            'Content-Type': 'application/json'
        }
    
    def search_keyword(self, keyword: str, city: str, num_results: int = 30) -> List[Dict]:
        results = []
        pages_needed = min(3, (num_results + 9) // 10)
        
        for page in range(pages_needed):
            payload = {
                "q": keyword,
                "gl": "us",
                "hl": "en", 
                "location": city,
                "num": 10,
                "start": page * 10
            }
            
            try:
                response = requests.post(
                    self.base_url,
                    headers=self.headers,
                    data=json.dumps(payload),
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'organic' in data:
                        for i, result in enumerate(data['organic']):
                            rank = (page * 10) + i + 1
                            if rank <= num_results:
                                results.append({
                                    'keyword': keyword,
                                    'url': result.get('link', ''),
                                    'city': city,
                                    'rank': rank
                                })
                    
                    time.sleep(1)
                    
                else:
                    print(f"Error for '{keyword}': {response.status_code} - {response.text}")
                    
            except Exception as e:
                print(f"Error fetching results for '{keyword}': {str(e)}")
        
        return results
    
    def scrape_multiple_keywords(self, keywords: List[str], city: str, num_results: int = 30) -> pd.DataFrame:
        all_results = []
        
        print(f"Starting scraping for {len(keywords)} keywords in {city}")
        
        for i, keyword in enumerate(keywords, 1):
            print(f"Processing keyword {i}/{len(keywords)}: '{keyword}'")
            
            keyword_results = self.search_keyword(keyword, city, num_results)
            all_results.extend(keyword_results)
            
            # Add delay between keywords
            time.sleep(2)
        
        df = pd.DataFrame(all_results)
        return df


# Keywords from the CSV file
keywords = [
    "business attorney",
    "business bankruptcy attorney", 
    "business compliance lawyer",
    "business dispute resolution lawyer",
    "business ethics and compliance counsel",
    "business ethics and compliance lawyer",
    "business incorporation services",
    "business law firm",
    "business legal services",
    "business licensing attorney",
    "business litigation firm",
    "business litigation lawyer",
    "business succession planning attorney",
    "business tax planning lawyer",
    "business transaction attorney",
    "family owned business attorney",
    "mergers and acquisitions attorney",
    "contract lawyers",
    "breach contract lawyer",
    "breach of contract lawyers",
    "breach of contract attorney",
    "business litigations lawyer",
    "business litigation attorneys",
    "attorney business litigation",
    "commercial litigators",
    "commercial litigation attorney",
    "merger and acquisition lawyer",
    "business dispute lawyers",
    "business contract lawyer",
    "attorney for business disputes",
    "business dispute attorney",
    "contract dispute lawyers",
    "m&a attorney",
    "business litigators",
    "merger and acquisition attorney",
    "corporate litigation lawyer",
    "corporate litigation attorney",
    "business contract lawyer near me",
    "merger attorney",
    "merger acquisition lawyer",
    "mergers & acquisitions attorney",
    "mergers & acquisitions lawyer",
    "corporate lawsuit lawyers",
    "mergers acquisitions law firm",
    "business litigation attorney near me",
    "business litigation lawyer near me",
    "breach of contract attorney near me",
    "breach of contract lawyers near me",
    "commercial litigation attorney in san francisco",
    "commercial litigation attorney san francisco",
    "business litigation attorney san francisco",
    "mergers and acquisitions attorney near me",
    "business acquisition attorney near me",
    "commercial litigation attorney near me",
    "business litigation attorney san francisco",
    "litigation attorney los angeles",
    "breach of contract lawyer houston",
    "houston breach of contract attorney",
    "breach of contract attorney houston",
    "business dispute attorney near me",
    "commercial litigation attorney san francisco",
    "business acquisition lawyer near me",
    "attorney for business disputes in san francisco",
    "business litigation lawyer san francisco"
]


def main():
    # API key and settings
    api_key = "00578c104d2374cac5f8bd3e338688b6ded20d01"
    city = "San Francisco, CA"
    
    # Initialize scraper
    scraper = GoogleSearchScraper(api_key)
    
    # Run scraping
    print(f"Starting scrape for {len(keywords)} business law keywords in {city}")
    
    try:
        results_df = scraper.scrape_multiple_keywords(keywords, city, num_results=30)
        
        if not results_df.empty:
            print(f"\nSuccessfully scraped {len(results_df)} total results")
            
            # Export to CSV with timestamp
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            filename = f"san_francisco_business_law_results_{timestamp}.csv"
            
            # Select only required columns and export
            output_df = results_df[['keyword', 'url', 'city', 'rank']].copy()
            output_df.to_csv(filename, index=False)
            
            print(f"Results exported to: {filename}")
            
            # Show summary
            print(f"\nSummary:")
            print(f"Total keywords processed: {len(keywords)}")
            print(f"Total results found: {len(results_df)}")
            print(f"Average results per keyword: {len(results_df) / len(keywords):.1f}")
            
            # Show sample results
            print(f"\nSample results:")
            print(output_df.head(10).to_string(index=False))
            
        else:
            print("No results found")
            
    except Exception as e:
        print(f"Error during scraping: {str(e)}")


if __name__ == "__main__":
    main()


# Instructions for running:
# 1. Save this script as 'sf_business_law_scraper.py'
# 2. Install required packages: pip install requests pandas
# 3. Run: python sf_business_law_scraper.py
# 
# The script will:
# - Process all 64 business law keywords
# - Search from San Francisco, CA location
# - Get up to 30 results per keyword  
# - Export results to CSV with columns: keyword, url, city, rank
# - Include delays to respect API rate limits
#
# Expected runtime: 5-10 minutes (due to rate limiting)
# API usage: ~200 API calls (64 keywords × ~3 calls each for 30 results)