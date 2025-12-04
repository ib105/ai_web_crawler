import csv
import os
from models.mcnews import News
from datetime import datetime
from io import StringIO
from azure.storage.blob import BlobServiceClient


def is_duplicate_news(news_name: str, seen_names: set) -> bool:
    return news_name in seen_names

def is_complete_news(news: dict, required_keys: list) -> bool:
    return all(key in news for key in required_keys)

def save_news_to_csv(news: list, filename: str):
    """Save news to Azure Blob Storage"""
    if not news:
        print("No news to save.")
        return

    # Get Azure Storage connection string from environment
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("AZURE_STORAGE_CONTAINER", "news-data")
    
    if not connection_string:
        print("Warning: No Azure Storage connection string. Saving locally.")
        save_news_to_csv_local(news, filename)
        return

    try:
        # Create blob service client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Add timestamp to filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_name = f"news_{timestamp}.csv"
        
        # Create CSV in memory
        output = StringIO()
        fieldnames = News.model_fields.keys()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(news)
        
        # Upload to blob
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=blob_name
        )
        blob_client.upload_blob(output.getvalue(), overwrite=True)
        
        print(f"Saved {len(news)} news to Azure Blob: {blob_name}")
        
    except Exception as e:
        print(f"Error saving to Azure Blob: {e}")
        print("Falling back to local save")
        save_news_to_csv_local(news, filename)


def save_news_to_csv_local(news: list, filename: str):
    """Fallback: Save to local file"""
    if not news:
        return

    fieldnames = News.model_fields.keys()
    file_exists = os.path.isfile(filename)
    is_empty = os.stat(filename).st_size == 0 if file_exists else False

    with open(filename, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if not file_exists or is_empty:
            writer.writeheader()
        writer.writerows(news)

    print(f"Saved {len(news)} news to local file: '{filename}'.")
