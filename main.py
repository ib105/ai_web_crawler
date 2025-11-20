import asyncio

from crawl4ai import AsyncWebCrawler
from dotenv import load_dotenv

from config import BASE_URL, CSS_SELECTOR, REQUIRED_KEYS
from utils.data_utils import (
    save_news_to_csv
)
from utils.scraper_utils import (
    fetch_and_process_page,
    get_browser_config,
    get_llm_strategynew,
)

load_dotenv()


async def crawl_news():
    """
    Main function to crawl news data from the website.
    """
    # Initialize configurations
    browser_config = get_browser_config()
    llm_strategy = get_llm_strategynew()
    session_id = "news_crawl_session"

    # Initialize state variables
    page_number = 1
    all_news = []
    seen_names = set()

    # Start the web crawler context
    # https://docs.crawl4ai.com/api/async-webcrawler/#asyncwebcrawler
    async with AsyncWebCrawler(config=browser_config) as crawler:
        while True:
            # Retry logic for API overload
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

            all_news.extend(news)

            if page_number > 20:
                break
            
            page_number += 1
            await asyncio.sleep(2)

    # Save the collected news to a CSV file
    if all_news:
        save_news_to_csv(all_news, "complete_news.csv")
        print(f"Saved {len(all_news)} news to 'complete_news.csv'.")
    else:
        print("No news were found during the crawl.")

    # Display usage statistics for the LLM strategy
    llm_strategy.show_usage()


async def main():
    """
    Entry point of the script.
    """
    await crawl_news()


if __name__ == "__main__":
    asyncio.run(main())
