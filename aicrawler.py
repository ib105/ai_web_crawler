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
            # Fetch and process data from the current page
            news, no_results_found = await fetch_and_process_page(
                crawler,
                page_number,
                BASE_URL,
                CSS_SELECTOR,
                llm_strategy,
                session_id,
                REQUIRED_KEYS,
                seen_names,
            )

            if no_results_found:
                print("No more news found. Ending crawl.")
                break  # Stop crawling when "No Results Found" message appears

            if not news:
                print(f"No news extracted from page {page_number}.")
                break  # Stop if no news are extracted            

            # Add the news from this page to the total list
            all_news.extend(news)

            if page_number > 20:
                print("Reached the maximum number of pages to crawl. Ending crawl.")
                break
            page_number += 1  # Move to the next page

            # Pause between requests to be polite and avoid rate limits
            await asyncio.sleep(2)  # Adjust sleep time as needed

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
