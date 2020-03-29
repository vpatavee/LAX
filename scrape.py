from bs4 import BeautifulSoup
import urllib.request
import ssl
import time
import json
from datetime import datetime, timedelta
import pandas as pd
import re
import sys


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

    for i in range(-20,21):
        with urllib.request.urlopen(url + str(i)) as response:
            html = response.read()
        soup = BeautifulSoup(html, 'html.parser')
        date = get_datetime_page(soup)
        if date:
            data = get_table_data(soup)
            htmls[date] = data     
    return htmls 
    

def convert_to_dt(time, date, tz="PST", year="2020"):
    """
    @param time string like 03:31
    @param date string as shonw in the top of table like Wednesday, 19 February from 20:00 to 22:00 
    @return datetime object
    If span across midnight, like "Wednesday, 19 February from 23:00 to 01:00", must add aditional 1 day
    
    Example:
    date = Wednesday, 19 February from 20:00 to 22:00
    time = 21:31
    return datetime(2020,02,19,21,31,00,PST)

    date = Wednesday, 19 February from 23:00 to 01:00
    time = 00:15
    return datetime(2020,02,20,00,15,00,PST)
    
    """
    if not date.strip() or not time.strip():
        print("error in date format", date, time)
        return None           
    
    date_from_to = re.findall("(.*) +from +(\d+:\d+) +to +(\d+:\d+)", date)
    
    if len(date_from_to) != 1 or len(date_from_to[0]) != 3:
        print("error in date format", date)
        return None   
    
    date_, from_, _ = date_from_to[0]
    
    res_dt = datetime.strptime("{} {} {} {}".format(date_, year, time, tz), '%A, %d %B %Y %H:%M %Z')
    from_dt = datetime.strptime("{} {} {} {}".format(date_, year, from_, tz), '%A, %d %B %Y %H:%M %Z')
    
    if res_dt<from_dt:
        return res_dt + timedelta(days=1)
    else:
        return res_dt
        
    

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
        for table_date, table in scraped_result.items():
            for row in table:                                                                                 
                location = airport_locations.get(row["airport"].strip(), dict())
                flight_object = [
                    convert_to_dt(row["scheduled"], table_date), 
                    convert_to_dt(row["actual"], table_date), 
                    row["flight"].strip(), 
                    row["gate"].strip(),
                    row["airport"].strip(), 
                    row["city"].strip(), 
                    country_code.get(location.get("country", ""), ""),
                    location.get("lat", ""), 
                    location.get("lon", ""), 
                    location.get("display_name", ""),
                    row["status"]
                ]

                if (flight_object[0].date(), flight_object[2]) not in seen and row["status"] in {"Landed", "En Route"}:  # scraped date can be duplicated. 
                    list_of_fligts.append(flight_object)
                    seen.add((flight_object[0].date(), flight_object[2]))  
                
                
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
        "display name",
        "status"])
    
    df = df.sort_values(by=['dt_scheduled'])
    return df
        

def daily(fname=database):
    now = time.time()
    print(fname)
    with open(fname, "r") as f:
        old = json.load(f)

    old[now] = scraped()

    with open(fname, "w") as f:
        json.dump(old, f)
        
        
if __name__ == "__main__":
    if len(sys.argv) == 1:
        fname = database
    elif len(sys.argv) == 2:
        fname = sys.argv[1]
    else:
        print("Invalid Usage")
        exit()
    try:
        daily(fname)
        print("scrape successfully at", time.time())
    except Exception as e:
        print("scrape unsuccessfully at", time.time())
        print("Unexpected error:", str(e))

