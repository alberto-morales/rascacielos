import os
import yaml
import pandas as pd
from datetime import date, timedelta

from time import sleep
from kafka import KafkaProducer

from confluent_kafka import avro as avro
from confluent_kafka.avro import AvroProducer

from confluent_kafka.serialization import StringSerializer


key_schema_str = """
{
   "namespace": "eu.albertomorales.tfm",
   "name": "flightkey",
   "type": "record",
   "fields" : [
     {
       "name" : "key",
       "type" : "string"
     }   
   ]
}
"""

value_schema_str = """
{
   "namespace": "eu.albertomorales.tfm",
   "name": "flightvalue",
   "type": "record",
   "fields" : [
     {
       "name" : "flight_date",
       "type" : "string"
     },     
     {
       "name" : "extraction_date_time",
       "type" : "string"
     },     
     {
       "name" : "price",
       "type" : "string"
     },
     {
       "name" : "flight_number",
       "type" : "string"
     },
     {
       "name" : "airline",
       "type" : "string"
     },
     {
       "name" : "departure_time",
       "type" : "string"
     },
     {
       "name" : "arrival_time",
       "type" : "string"
     },
     {
       "name" : "origin_airport",
       "type" : "string"
     },
     {
       "name" : "destination_airport",
       "type" : "string"
     }               
   ]
}
"""


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def create_test_df():
    #
    res_price = []
    res_flight_number = []
    res_airline = []
    res_departure_time = []
    res_arrival_time = []
    res_origin_airport = []              
    res_destination_airport = []              
    res_index = []
    #
    price = '178.83'
    flight_number = '0,VY1017,VY1290'
    airline = 'Vueling'
    departure_time = '21:15'
    arrival_time = '21:10 +1 día'
    origin_airport = 'Adolfo Suárez Madrid - Barajas, Madrid (MAD)'
    destination_airport = 'La Coruna, La Coruña (LCG)'
    index = 0
    #
    res_price.append(price)
    res_flight_number.append(flight_number.strip())
    res_airline.append(airline.strip())
    res_departure_time.append(departure_time.strip())
    res_arrival_time.append(arrival_time.strip())
    res_origin_airport.append(origin_airport.strip())
    res_destination_airport.append(destination_airport.strip())
    res_index.append(index)
    #
    res_price = { 'price': res_price}
    res_flight_number = { 'flight_number': res_flight_number}
    res_airline = { 'airline': res_airline}
    res_departure_time = { 'departure_time': departure_time}
    res_arrival_time = { 'arrival_time': arrival_time}
    res_origin_airport = { 'origin_airport': res_origin_airport}
    res_destination_airport = { 'destination_airport': res_destination_airport}
    #
    df_price = pd.DataFrame(res_price, index = res_index)
    df_flight_number = pd.DataFrame(res_flight_number, index = res_index)
    df_airline = pd.DataFrame(res_airline, index = res_index)
    df_departure_time = pd.DataFrame(res_departure_time, index = res_index)
    df_arrival_time = pd.DataFrame(res_arrival_time, index = res_index)
    df_origin_airport = pd.DataFrame(res_origin_airport, index = res_index)
    df_destination_airport = pd.DataFrame(res_destination_airport, index = res_index)
    #
    df = df_price
    df = df.join(df_flight_number)
    df = df.join(df_airline)
    df = df.join(df_departure_time)
    df = df.join(df_arrival_time)
    df = df.join(df_origin_airport)
    df = df.join(df_destination_airport)    
    return df

    
class FlightDAO():
    def __init__(self):
        """The following configuration elements are required:

            persistence.bootstrap_servers
            persistence.schema_registry_url
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + '/webscrapper.yaml') as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
            self._BOOTSTRAP_SERVERS   = config['persistence']['bootstrap_servers']
            self._SCHEMA_REGISTRY_URL = config['persistence']['schema_registry_url']
            self._TOPIC_NAME_PREFIX   = config['persistence']['topic_name_prefix']            
        value_schema = avro.loads(value_schema_str)
        key_schema = avro.loads(key_schema_str)
        self._producer = AvroProducer({
            'bootstrap.servers': self._BOOTSTRAP_SERVERS,
            'on_delivery': delivery_report,
            'schema.registry.url': self._SCHEMA_REGISTRY_URL
            }, default_key_schema=key_schema, default_value_schema=value_schema)        
        print('FlightDAO initiated')

    def persist(self, df, origin, destination, extraction_date_time, flight_date):
        print(df)
        print(origin)
        print(destination)
        print(extraction_date_time)
        print(flight_date)
        topic_name = self._TOPIC_NAME_PREFIX + origin + '-' + destination + '-json'
        #
        for i in range(len(df)) : 
          index = i + 1
          price = df.loc[index, 'price']
          flight_number = df.loc[index, 'flight_number']
          airline = df.loc[index, 'airline']
          departure_time = df.loc[index, 'departure_time']
          arrival_time = df.loc[index, 'arrival_time']
          origin_airport = df.loc[index, 'origin_airport']
          destination_airport = df.loc[index, 'destination_airport']         
          #
          key_str = flight_date + ',' + extraction_date_time
          key = {
                  "key": key_str
                }
          value = {
                      "flight_date": flight_date,
                      "extraction_date_time": extraction_date_time,
                      "price": price,
                      "flight_number": flight_number,
                      "airline": airline,
                      "departure_time": departure_time,
                      "arrival_time": arrival_time,
                      "origin_airport": origin_airport,
                      "destination_airport": destination_airport
                  }      
          #
          self._producer.produce(topic=topic_name, key=key, value=value)
          self._producer.flush(timeout=5)  #5seg   


class RawDAO():
    def __init__(self):
        """The following configuration elements are required:

            persistence.bootstrap_servers
            persistence.topic_name_prefix
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + '/webscrapper.yaml') as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
            self._BOOTSTRAP_SERVERS   = config['persistence']['bootstrap_servers']
            print(f'_BOOTSTRAP_SERVERS initialized with {self._BOOTSTRAP_SERVERS}')
            self._TOPIC_NAME_PREFIX   = config['persistence']['topic_name_prefix']
            print(f'_TOPIC_NAME_PREFIX initialized with {self._TOPIC_NAME_PREFIX}')
        self._producer = KafkaProducer(bootstrap_servers=self._BOOTSTRAP_SERVERS)     
        print('RawDAO initiated')    


    def persist(self, page_source, origin, destination, extraction_date_time, flight_date):
        print(f"page_source.len() es '{page_source.__len__()}'")
        print(origin)
        print(destination)
        print(extraction_date_time)
        print(flight_date)
        topic_name = self._TOPIC_NAME_PREFIX + origin + '-' + destination + '-htm'
        #
        key = flight_date + ',' + extraction_date_time
        key = bytearray(key, 'utf8')
        value = bytearray(page_source, 'utf8')
        #
        self._producer.send(topic=topic_name, key=key, value=value)
        self._producer.flush(timeout=5)  #5seg


if __name__ == "__main__":   
    #
    # df = create_test_df()
    # dao = FlightDAO()
    # dao.persist(df, 'MAD', 'LCG', date.today().strftime("%Y-%m-%d"), (date.today() + timedelta(days=20)).strftime("%Y-%m-%d"))      
    #
    dao = RawDAO()
    dao.persist('<html><body>test</body></html>', 'MAD', 'LCG', date.today().strftime("%Y-%m-%d"), (date.today() + timedelta(days=20)).strftime("%Y-%m-%d"))      
    #
