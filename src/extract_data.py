import os
import json
import time
import logging
import io
import zipfile
from pathlib import Path
import requests

BASE_URL = "https://tacviewnew.growlingsidewinder.com"
MISSION_NAME = "Operation_Urban_Thunder"
DOWNLOAD_DIR = Path('data')

resp = requests.get(f"{BASE_URL}/api/replay")
resp.raise_for_status()


for i in range(10):
    data = resp.json()[i]
    id = data['id']
    print(MISSION_NAME in data['title'], id)
    if MISSION_NAME not in data['title']:
        continue
        
    content = requests.get(f"{BASE_URL}/api/replay/{id}/download")
    content.raise_for_status()
    raw_data = content.content
    
    if not zipfile.is_zipfile(io.BytesIO(raw_data)):
        with open(f'{DOWNLOAD_DIR}/{id}.txt.acmi', 'wb') as f:
            f.write(content.content)
        print(f"=== Downloaded {data['title']} with id {id}. ===")
        continue
                
    with zipfile.ZipFile(io.BytesIO(raw_data)) as z:
        with open(f"{DOWNLOAD_DIR}/{id}.txt.acmi", "wb") as f:
            f.write(z.read(z.namelist()[0]))
        print(f"=== Downloaded {data['title']} (ZIP) with id {id}. ===")
            
            
    

            
    
    
    


