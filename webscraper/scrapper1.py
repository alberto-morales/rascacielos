from selenium import webdriver
from time import sleep
from webscraper. result_parser import ed_01

def main():
    # initializing resources
    print("Starting...")
    # code
    driver = webdriver.Firefox()
    driver.implicitly_wait(20)
    # la_URL = "https://www.edreams.es/travel/#results/type=R;dep=2020-08-15;from=MAD;to=LCG;ret=2020-08-15;collectionmethod=false;airlinescodes=false;internalSearch=true"
    la_URL = "https://www.edreams.es/travel/#results/type=O;dep=2020-08-15;from=MAD;to=LCG;internalSearch=true"
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

##code to execute in for loop goes here
if __name__ == '__main__':
    main()
