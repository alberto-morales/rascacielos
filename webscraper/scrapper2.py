from selenium import webdriver
from time import sleep
from kafka import KafkaProducer
from datetime import datetime

def date_time():
    now = datetime.now()  # current date and time
    date_time = now.strftime("%Y-%m-%d,%H:%M:%S")
    print("date and time:", date_time)
    return date_time

def main():
    # initializing resources
    print("Starting...")
    # code
    driver = webdriver.Firefox()
    driver.implicitly_wait(20)
    la_URL = "https://www.edreams.es/travel/#results/type=R;dep=2020-08-15;from=MAD;to=LCG;ret=2020-08-15;collectionmethod=false;airlinescodes=false;internalSearch=true"
    driver.delete_all_cookies()
    driver.get(la_URL)
    sleep(1.5)  # Time in seconds
    # Selenium hands the page source to parser
    page_source = driver.page_source
    #
    # f = open("page_source.htm", "w", encoding='utf8')
    # f.write(page_source)
    # f.close()
    #

    origin = 'MAD'
    destination = 'LCG'
    topic = origin + '-' + destination + '-htm'
    key = '2020-08-15' + ',' + '2020-08-15' + ',' + date_time()
    key = bytearray(key, 'utf8')
    value = bytearray(page_source, 'utf8')
    producer = KafkaProducer(bootstrap_servers='certik:9092')
    producer.send(topic, key=key, value=value)
    producer.flush(timeout=5) #5seg

##code to execute in for loop goes here
if __name__ == '__main__':
    main()
