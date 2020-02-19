from selenium import webdriver
from time import sleep
from webscraper. result_parser import ed_01

def main():
    # initializing resources
    print("Starting...")
    # code
    driver = webdriver.Firefox()
    driver.implicitly_wait(20)
    la_URL = "https://www.edreams.es/travel/#results/type=R;dep=2020-02-20;from=MAD;to=LCG;ret=2020-02-21;collectionmethod=false;airlinescodes=false;internalSearch=true"
    driver.delete_all_cookies()
    driver.get(la_URL)
    sleep(0.5)  # Time in seconds
    # Selenium hands the page source to parser
    page_source = driver.page_source
    parser = ed_01()
    df = parser.parse(page_source)

##code to execute in for loop goes here
if __name__ == '__main__':
    main()
