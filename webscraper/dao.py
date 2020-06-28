import os
import yaml
import pandas as pd
from datetime import date, timedelta

from time import sleep
from datetime import datetime
from kafka import KafkaProducer

from confluent_kafka import avro as avro
from confluent_kafka.avro import AvroProducer


key_schema_str = """
{
   "namespace": "eu.albertomorales.tfm",
   "name": "value",
   "type": "record",
   "fields" : [
     {
       "name" : "flight_date",
       "type" : "string"
     }   
   ]
}
"""

value_schema_str = """
{
   "namespace": "eu.albertomorales.tfm",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "extraction_date",
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

def date_time():
    now = datetime.now()  # current date and time
    date_time = now.strftime("%Y-%m-%d,%H:%M:%S")
    print("date and time:", date_time)
    return date_time
    
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
        value_schema = avro.loads(value_schema_str)
        key_schema = avro.loads(key_schema_str)
        self._producer = AvroProducer({
            'bootstrap.servers': self._BOOTSTRAP_SERVERS,
            'on_delivery': delivery_report,
            'schema.registry.url': self._SCHEMA_REGISTRY_URL
            }, default_key_schema=key_schema, default_value_schema=value_schema)        


    def persist(self, df, origin, destination, extraction_date, flight_date):
        print(df)
        print(origin)
        print(destination)
        print(extraction_date)
        print(flight_date)
        topic_name = origin + '-' + destination + '-json'
        #
        extraction_date = date_time()
        #
        for i in range(len(df)) : 
          price = df.loc[i, 'price']
          flight_number = df.loc[i, 'flight_number']
          airline = df.loc[i, 'airline']
          departure_time = df.loc[i, 'departure_time']
          arrival_time = df.loc[i, 'arrival_time']
          origin_airport = df.loc[i, 'origin_airport']
          destination_airport = df.loc[i, 'destination_airport']         
          #
          key = {
                  "flight_date": flight_date
                }
          value = {
                      "extraction_date": extraction_date,
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

            planner.number_of_days
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + '/webscrapper.yaml') as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
            self._BOOTSTRAP_SERVERS   = config['persistence']['bootstrap_servers']
        self._producer = KafkaProducer(bootstrap_servers=self._BOOTSTRAP_SERVERS)         


    def persist(self, page_source, origin, destination, extraction_date, flight_date):
        print(f"page_source.len() es '{page_source.__len__()}'")
        print(origin)
        print(destination)
        print(extraction_date)
        print(flight_date)
        topic_name = origin + '-' + destination + '-htm'
        #
        topic = origin + '-' + destination + '-htm'
        key = {
                "flight_date": flight_date,
                "extraction_date_time": date_time()
              }        
        key = bytearray(key, 'utf8')
        value = bytearray(page_source, 'utf8')
        #
        self._producer.send(topic, key=key, value=value)
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
