import csv
import os
from models.mcnews import News
from datetime import datetime
from io import StringIO
from azure.storage.blob import BlobServiceClient


def standardize_publishtime(news_items: list) -> list:
    """
    Standardize publishtime to current timestamp in ISO format.
    
    Args:
        news_items: List of news dictionaries
        
    Returns:
        List of news with standardized publishtime
    """
    current_time = datetime.now().isoformat()
    
    for news in news_items:
        news['publishtime'] = current_time
    
    return news_items


def is_duplicate_news(news_name: str, seen_names: set) -> bool:
    return news_name in seen_names


def is_complete_news(news: dict, required_keys: list) -> bool:
    return all(key in news for key in required_keys)