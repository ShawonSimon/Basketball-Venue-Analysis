import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from geopy import Nominatim
import time
from datetime import datetime
from geopy.geocoders import MapQuest
from airflow.models import Variable

#URL of the Wikipedia page
url="https://en.wikipedia.org/wiki/List_of_basketball_arenas"

NO_IMAGE="https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png"

# Function to get the Wikipedia page content
def get_wikipedia_page(url):

    print("getting wikipedia page...", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        return response.text
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return None
    
# Function that cleans the extracted data
def clean_text(text):
    text = str(text).strip() 
    if text.find('*'):
        text = text.split('*')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' \\'):
        text = text.split('\\')[0]
    return text
    
                 
# Function to extract table data from the Wikipedia page
def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    soup = BeautifulSoup(html, 'html.parser')

    #Debug: Print a part of the HTML to verify the structure
    #print("HTML Snippet:", soup.prettify()[:1000])
    tableBody = soup.find_all("tbody")[1]
    
    data = []

    for row in tableBody.find_all('tr'):
        col = row.find_all('td')
        
        # Debug: Print the columns to verify their content
        #print("Row Columns:", [c.text.strip() for c in col])
        values = {
              "Venue": clean_text(col[1].text if len(col) > 1 else None), 
              "Capacity": clean_text(col[2].text if len(col) > 2 else None).replace(',', ''), 
              "Basketballteam(s)": clean_text(col[3].text if len(col) > 3 else None),
              "Location": clean_text(col[4].text if len(col) > 4 else None), 
              "Country": clean_text(col[5].text if len(col) > 5 else None),
              "Image": 'https://' + col[6].find("img").get("src").split("//")[1] if len(col) > 6 and col[6].find("img") else None,
        }
        data.append(values)

    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return 'OK'
API_KEY = Variable.get("API_KEY")
def get_lat_long(country, location):
    # Initialize MapQuest geolocator with API key
    geolocator = MapQuest(api_key=API_KEY)
    location = geolocator.geocode(f'{location}, {country}')

    # Return latitude and longitude if found
    if location:
        time.sleep(3)  # Sleep for 3 seconds to avoid rate limits
        return location.latitude, location.longitude

    return None

def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_wikipedia_data')
    data = json.loads(data)

    Venue_df = pd.DataFrame(data)
    Venue_df['Image'] = Venue_df['Image'].apply(lambda x: x if x not in ['', None] else NO_IMAGE)
    Venue_df['LatLong'] = Venue_df.apply(lambda x: get_lat_long(x['Country'], x['Venue']), axis=1)
    Venue_df = Venue_df.drop(index=0).reset_index(drop=True)
    
    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=Venue_df.to_json())

    return 'OK'

def write_wikipedia_data(**kwargs):
    # Pull data from XCom
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    # Parse JSON and convert to DataFrame
    data = json.loads(data)
    data = pd.DataFrame(data)

    # Generate filename
    file_name=f'venues_cleaned_{datetime.now().strftime("%Y-%m-%d_%H_%M_%S")}.csv'

    # Get ADLS connection details from Airflow Variables
    tenant_id = Variable.get("tenant_id")
    client_id = Variable.get("client_id")
    client_secret = Variable.get("client_secret")

    data.to_csv(f'abfs://venues@basketballvenuessa.dfs.core.windows.net/raw_data/{file_name}',
            storage_options={
                'tenant_id': tenant_id,
                'client_id': client_id,
                'client_secret': client_secret,
            },
            index=False)

    print(f"File {file_name} uploaded to ADLS")
