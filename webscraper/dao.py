import os
import yaml
import pandas as pd
from datetime import date, timedelta

from confluent_kafka import avro as avro
from confluent_kafka.avro import AvroProducer

key_schema_str = """
{
   "namespace": "eu.albertomorales.tfm",
   "name": "value",
   "type": "record",
   "fields" : [
     {
       "name" : "extraction_date",
       "type" : "string"
     },
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
       "name" : "price",
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

class FlightDAO():
    def __init__(self, params=None):
        """The following configuration elements are required:

            planner.number_of_days
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + '/webscrapper.yaml') as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
            self._BOOTSTRAP_SERVERS   = config['persistence']['bootstrap_servers']
            self._SCHEMA_REGISTRY_URL = config['persistence']['schema_registry_url']
        value_schema = avro.loads(value_schema_str)
        key_schema = avro.loads(key_schema_str)
        self._avroProducer = AvroProducer({
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
        topic_name = origin + '-' + destination + '-htm'
        key = {
                "extraction_date": extraction_date,
                "flight_date": flight_date
              }
        value = {
                    "price": "69,69"
                }      
        self._avroProducer.produce(topic=topic_name, key=key, value=value)
        self._avroProducer.flush()     


if __name__ == "__main__":
    dao = FlightDAO()
    res_price = []
    res_index = []    
    res_price.append('68,86')
    res_index.append(1)    
    res_data = {'price' : res_price}    
    df = pd.DataFrame(res_data, index = res_index)
    dao.persist(df, 'MAD', 'LCG', date.today().strftime("%Y-%m-%d"), (date.today() + timedelta(days=20)).strftime("%Y-%m-%d"))      