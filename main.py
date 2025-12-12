import asyncio

from crawl4ai import AsyncWebCrawler
from dotenv import load_dotenv

from config import BASE_URL, CSS_SELECTOR, REQUIRED_KEYS
from utils.kafka_producer import send_news_to_kafka
from utils.data_utils import (
    save_news_to_csv
)
from utils.scraper_utils import (
    fetch_and_process_page,
    get_browser_config,
    get_llm_strategynew,
)
import os
load_dotenv()


async def crawl_news():
    """
    Main function to crawl news data from the website.
    """
    browser_config = get_browser_config()
    llm_strategy = get_llm_strategynew()
    session_id = "news_crawl_session"
    
    page_number = 1
    all_news = []
    seen_names = set()
    kafka_topic = os.getenv('KAFKA_TOPIC', 'news-events')

    async with AsyncWebCrawler(config=browser_config) as crawler:
        while True:
            news = []
            for attempt in range(3):
                news, no_results_found = await fetch_and_process_page(
                    crawler, page_number, BASE_URL, CSS_SELECTOR,
                    llm_strategy, session_id, REQUIRED_KEYS, seen_names
                )
                
                if news or no_results_found:
                    break
                
                print(f"Retry {attempt + 1}/3 for page {page_number}")
                await asyncio.sleep(5)

            if no_results_found:
                break

            if not news:
                print(f"No news from page {page_number} after retries.")
                break

            # Send to Kafka immediately after each page
            send_news_to_kafka(news, kafka_topic)
            print(f"Sent {len(news)} news from page {page_number} to Kafka")
            
            all_news.extend(news)

            if page_number > 20:
                break
            
            page_number += 1
            await asyncio.sleep(2)

    # Save complete CSV at the end
    if all_news:
        save_news_to_csv(all_news, "complete_news.csv")
        print(f"Saved {len(all_news)} news to 'complete_news.csv'.")
    else:
        print("No news were found during the crawl.")

    llm_strategy.show_usage()


async def main():
    """
    Entry point of the script.
    """
    await crawl_news()


if __name__ == "__main__":
    asyncio.run(main())
