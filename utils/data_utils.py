import csv
import os
from models.mcnews import News


def is_duplicate_news(news_name: str, seen_names: set) -> bool:
    return news_name in seen_names

def is_complete_news(news: dict, required_keys: list) -> bool:
    return all(key in news for key in required_keys)

def save_news_to_csv(news: list, filename: str):
    if not news:
        print("No news to save.")
        return

    # Use field names from the Venue model
    fieldnames = News.model_fields.keys()

    # Check if file exists and is empty
    file_exists = os.path.isfile(filename)
    is_empty = os.stat(filename).st_size == 0 if file_exists else False

    with open(filename, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        # Only write header if file is empty or doesn't exist
        if not file_exists or is_empty:
            writer.writeheader()
        
        writer.writerows(news)

    # with open(filename, mode="w", newline="", encoding="utf-8") as file:
    #     writer = csv.DictWriter(file, fieldnames=fieldnames)
    #     writer.writeheader()
    #     writer.writerows(news)
    print(f"Saved {len(news)} news to '{filename}'.")
