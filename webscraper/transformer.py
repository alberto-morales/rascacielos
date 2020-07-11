import json
from time import sleep
from collections import namedtuple
import os
import yaml

from kafka import KafkaConsumer, KafkaProducer

from dao import FlightDAO
from webscraper.result_parser import ed_01

if __name__ == '__main__':
    print('Running Consumer..')
    #
    FlightRecord = namedtuple('FlightRecord', 'origin destination flight_date extraction_date_time df')
    #
    origin = 'MAD'
    destination = 'LCG'
    parser = ed_01()
    #    
    parsed_records = []
    origin_topic_name = origin + '-' + destination + '-htm'    
    #
    bootstrap_servers = None
    with open(os.path.dirname(os.path.abspath(__file__)) + '/webscrapper.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        bootstrap_servers = config['persistence']['bootstrap_servers']    

    consumer = KafkaConsumer(origin_topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=bootstrap_servers, consumer_timeout_ms=1000)
    for msg in consumer:
        #
        key = msg.key.decode('utf8')
        key_tokens = key.split(',')
        flight_date = key_tokens[0]
        extraction_date = key_tokens[1]
        extraction_time = key_tokens[2]
        extraction_date_time = extraction_date + ',' + extraction_time
        #
        page_source = msg.value.decode('utf8')
        df = None
        # try:
        df = parser.parse(page_source)
        # except expression as identifier:
        #     pass            
        print(f'df es {df}') 
        #
        record = FlightRecord(origin, destination, flight_date, extraction_date_time, df)
        parsed_records.append(record)
    consumer.close()
    sleep(5)

    if len(parsed_records) > 0:
        print('Publishing records..')
        dao = FlightDAO()
        for rec in parsed_records:
            origin = rec.origin
            destination = rec.destination
            flight_date = rec.flight_date
            extraction_date_time = rec.extraction_date_time
            df = rec.df
            if df is None:
                raise Exception("no se ha podido parsear.")
            dao.persist(df, 
                        origin,
                        destination,
                        extraction_date_time,
                        flight_date
                        )






      



