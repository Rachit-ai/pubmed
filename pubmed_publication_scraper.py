import xmltodict
import json
import requests
import pymongo
import random
import time
import pandas as pd
# Load the XML content from a file or string

client = pymongo.MongoClient("mongodb+srv://")
db = client["npi_directory"]
collection = db["pubmed"]

proxies = {
    "http": "http://77.222.58.239:13231"
}

headers = {
    'sec-ch-ua-platform': '"macOS"',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    'sec-ch-ua-mobile': '?0'
}

def convert_xml_to_json(xml_string):
    try:
        parsed_dict = xmltodict.parse(xml_string)        
        json_data = json.dumps(parsed_dict, indent=4)
        return json_data

    except Exception as e:
        print("Error converting XML to JSON:", e)
        return None

def crawl(id):
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={id}"
    xml_content = requests.get(url, headers=headers, proxies=proxies)
    json_output = convert_xml_to_json(xml_content.text)    
    json_output = json.loads(json_output)
    json_output = json_output["PubmedArticleSet"]["PubmedArticle"]
    return json_output


def main():
    # df = pd.read_csv("chunk_1.csv", encoding='ISO-8859-1')
    df = pd.read_csv("chunk_1.csv")
    data = json.loads(df.to_json(orient="records"))

    for idx, row in enumerate(data):
        title = row['title']
        url = row['url']
        id = row['id']
        print("\n")
        print(f"crawling pubmed id : {id}")
        is_exist = collection.find_one({"url": str(url)})        
        if is_exist is None:
            try:
                pubmed_data = crawl(id)
            except Exception as e:
                print(e)
                pubmed_data = None
                time.sleep(2)

            if pubmed_data:
                _payload = {
                    "title": title,
                    "url": url,
                    "id": str(id),
                    "data": pubmed_data
                }

                print(f"Dumping data for url: {url}, index= {idx}")
                try:
                    print(json.dumps(_payload, indent=4))
                    # collection.insert_one(_payload)
                    print(f"data uploaded for PUBMED ID: {id}, url: {url}")
                except Exception as e:
                    print(e)

                wai = random.randint(20, 30)
                print(f"rest-time: {wai}")
                time.sleep(wai)

if __name__ == "__main__":
    main()
