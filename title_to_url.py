import pandas as pd
import requests
import time
import json
import os
from bs4 import BeautifulSoup

headers = {
    "User-Agent": "Mozilla/5.0"
}
proxies = {}  # Fill if needed

def crawl(title):
    url = f"https://pubmed.ncbi.nlm.nih.gov/?term={title}"
    response = requests.get(url, headers=headers, proxies=proxies)
    soup = BeautifulSoup(response.text, 'html.parser')

    uid_meta = soup.find('meta', attrs={'name': 'uid'})
    uid = uid_meta['content'] if uid_meta else None
    url_id = f"https://pubmed.ncbi.nlm.nih.gov/{uid}/" if uid else None

    return {
        "title": title,
        "url_id": url_id,
        "pubmed_id": uid
    }

def main():
    input_file = "url.csv"
    output_file = "output.csv"

    # Read input titles
    df_input = pd.read_csv(input_file)
    titles = df_input["title"].tolist()
    total = len(titles)

    # Check if output file exists
    file_exists = os.path.exists(output_file)
    processed_titles = set()

    if file_exists:
        try:
            df_existing = pd.read_csv(output_file)
            processed_titles = set(df_existing["title"])
        except Exception:
            print("Could not read existing output file. Continuing without filtering.")

    for i, title in enumerate(titles, start=1):
        if title in processed_titles:
            print(f"{i}/{total} skipped (already processed): {title}")
            continue

        try:
            result = crawl(title)
            new_df = pd.DataFrame([result])
            new_df.to_csv(output_file, mode='a', index=False, header=not file_exists)
            file_exists = True  # After first write, prevent further headers
            print(f"{i}/{total} saved: {title}")
            time.sleep(1)
        except Exception as e:
            print(f"{i}/{total} error: {title} -> {e}")
            time.sleep(2)


if __name__ == "__main__":
    main()
