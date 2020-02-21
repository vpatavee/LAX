from bs4 import BeautifulSoup
import urllib.request
import ssl
import time
import json
from datetime import datetime
import pandas as pd


database = "database.json"

def get_datetime_page(soup):
    try:
        return soup.find_all("table")[0].find_all("div")[0].text
    except:
        return ""
    
def get_table_data(soup):
    """
    @param soup object of a page
    @return list of dict : [{...}, {...}, {...}]
    each object in list represent each row in the table
    
    {
        "airline": "",
        "flight": "",
        "from": "",
        "scheduled": "",
        "actual": "",
        "gate": "",
    
    }
    """
    table = soup.find_all("table", class_="my_flight")
    parsed_table = list()
    for row in table:
        if not row.contents: # malform row
            continue
        parsed_row = dict()
        columns = row.contents[0].find_all("td")
        if len(columns) != 10: # malform row
            continue
        parsed_row["airline"] = columns[1].text
        parsed_row["flight"] = columns[2].text
        parsed_row["airport"] = columns[4].text
        parsed_row["city"] = columns[5].text
        parsed_row["scheduled"] = columns[6].text
        parsed_row["actual"] = columns[7].text
        parsed_row["gate"] = columns[8].text
        parsed_row["status"] = columns[9].text
        parsed_table.append(parsed_row)
            
    return parsed_table


def scraped():
    """
    @return {"<date as shown on the top of the page>" : <list of dict as return from get_table_data>}
    """
    ssl._create_default_https_context = ssl._create_unverified_context
    url = "https://www.airport-la.com/lax/arrivals?t="
    htmls = dict()

    for i in range(-10,21):
        with urllib.request.urlopen(url + str(i)) as response:
            html = response.read()
        soup = BeautifulSoup(html, 'html.parser')
        date = get_datetime_page(soup)
        if date:
            data = get_table_data(soup)
            htmls[date] = data     
    return htmls 


def sort_flight_by_time(db, airport_locations, country_code):
    """
    
    @param db 
    db: dict -> k=time_scraped (epochs), v=scraped_result
    scraped_result: dict -> k=table_date (string), v=table (list or row)
    row: dict -> k:column_name, v:value
    @param airport_locations dict -> k=IATA code, v=object return from get_lat_log
    @return return pandas dataframe
    
    
    """
    seen = set() # set of tuple (flight, date)
    list_of_fligts = list()

    
    for time_scraped, scraped_result in db.items():
        year = "2020" # TODO: get year from time_scraped
        for table_date, table in scraped_result.items():
            date_month = table_date.split("from")[0].strip()
            for row in table:
                if (row["flight"], date_month) not in seen and row["status"] == "Landed":
                    seen.add((row["flight"], date_month))          
                    
                    location = airport_locations.get(row["airport"].strip(), dict())
                    flight_object = [
                        datetime.strptime("{} {} {} PST".format(date_month, year, row["scheduled"]), '%A, %d %B %Y %H:%M %Z'), 
                        datetime.strptime("{} {} {} PST".format(date_month, year, row["actual"]), '%A, %d %B %Y %H:%M %Z') , 
                        row["flight"].strip(), 
                        row["gate"].strip(),
                        row["airport"].strip(), 
                        row["city"].strip(), 
                        country_code.get(location.get("country", ""), ""),
                        location.get("lat", ""), 
                        location.get("lon", ""), 
                        location.get("display_name", "")
                    ]
                    list_of_fligts.append(flight_object)
                
    df = pd.DataFrame(list_of_fligts, columns=[
        "dt_scheduled", 
        "dt_actual", 
        "flight",
        "gate",
        "airport", 
        "city", 
        "country",
        "lat", 
        "long", 
        "display name"])
    
    df = df.sort_values(by=['dt_scheduled'])
    return df
        

def daily(fname=database):
    now = time.time()

    with open(database, "r") as f:
        old = json.load(f)

    old[now] = scraped()

    with open(database, "w") as f:
        json.dump(old, f)
        
        
if __name__ == "__main__":
    daily()