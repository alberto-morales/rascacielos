from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from time import sleep
from kafka import KafkaProducer
from datetime import date, timedelta
import os
from pyvirtualdisplay import Display

from webscraper.result_parser import ed_01

class Scrapper():

    def __init__(self):
        pass 

    def extract(self, origin, destination, extraction_date, flight_date):
        print(extraction_date)        
        print(flight_date)
        print(origin)
        print(destination)
        # initializing resources
        print("Starting...")
        # code
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(chrome_options=chrome_options,executable_path='/bin/chromedriver')
        #
        driver.implicitly_wait(20)
        # la_URL = "https://www.edreams.es/travel/#results/type=R;dep=2020-08-15;from=MAD;to=LCG;ret=2020-08-15;collectionmethod=false;airlinescodes=false;internalSearch=true"
        # la_URL = "https://www.edreams.es/travel/#results/type=O;dep=2020-08-15;from=MAD;to=LCG;internalSearch=true"
        la_URL = f"https://www.edreams.es/travel/#results/type=O;dep={flight_date};from={origin};to={destination};internalSearch=true"
        print(la_URL)
        driver.delete_all_cookies()
        driver.get(la_URL)
        sleep(5.5)  # Time in seconds
        # Selenium hands the page source to parser
        page_source = driver.page_source
        #
        # f = open("page_source.htm", "w", encoding='utf8')
        # f.write(page_source)
        # f.close()
        #
        parser = ed_01()
        df = parser.parse(page_source)
        print(df) 
        return df       

if __name__ == "__main__":
    scrapper = Scrapper()
    scrapper.extract('MAD', 'LCG', date.today().strftime("%Y-%m-%d"), (date.today() + timedelta(days=20)).strftime("%Y-%m-%d"))        