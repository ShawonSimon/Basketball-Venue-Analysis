import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
# URL of the Wikipedia page
url="https://en.wikipedia.org/wiki/List_of_basketball_arenas"

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
def get_wikipedia_data(html): 

    soup = BeautifulSoup(html, 'html.parser')

    # Debug: Print a part of the HTML to verify the structure
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
        

    Data_df = pd.DataFrame(data)
    Data_df = Data_df.drop(index=0) # Drop the first row
    Data_df.to_csv("output.csv", index=False)
    print(data)
    

    json_rows = json.dumps(data)
    kwargs['t1'].xcom_push(key='rows', value=json_rows)

    return 'OK'

def get_lat_long(Country, location):
    geolocator = Nominatim(user_agent='geoapiExercises')
    location = geolocator.geocode(f'{Country}, {location}')

    if location:
        return  location.latitude, location.longitude
    
    return None

def transform_wikipedia_data(**kwargs):
    data = kwargs['t1'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)

    Venue_df = pd.DataFrame(data)

    Venue_df['Image'] = Venue_df['Image'].apply(lambda x: x if x not in ['', None] else NO_IMAGE)
    Venue_df['Location'] = Venue_df.apply(lambda x: get_lat_long(x['Country'], x['Venue']), axis=1)

    # push to xcom
    kwargs['t1'].xcom_push(key='rows', value=Venue_df.to_json())

    return 'OK'

def write_wikipedia_data(**kwargs):
    from datetime import datetime
    data = kwargs['t1'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    data = json.loads(data)
    data = pd.DataFrame(data)

    filename=('venues_cleand_' + str(datetime.now().date()) 
              + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')
    
    data.to_csv(filename, index=False)


# Function to execute the data extraction 
def extract_wikipedia_data(url):
    html = get_wikipedia_page(url)
    data = get_wikipedia_data(html)

# Call the function to extract data 
extract_wikipedia_data(url)
