import yaml
import os 
from datetime import date, timedelta

from task_creator_one_way import Planner
from scrapper import Scrapper
from dao import RawDAO, FlightDAO

from pandas import DataFrame

class Processor():

    def __init__(self, params, planner : Planner):
        """params is a dict. Within it, the following key-value pairs are required:

            origin
            destination
        """
        try:
            self._origin = params['origin']
            self._destination = params['destination']
            self._planner = planner
            #
            with open(os.path.dirname(os.path.abspath(__file__)) + '/webscrapper.yaml') as f:
                config = yaml.load(f, Loader=yaml.FullLoader)
                self._persistence_format = config['persistence']['format']     
        except Exception:                     
            raise ValueError("origin & destination values required")        

    def play(self):
        # print(f'Origin is "{self._origin}"')
        # print(f'Destination is "{self._destination }"')
        #
        # gets the dates dataframe
        df_fechas = self._planner.create_tasks()
        # and shuffles it
        df_fechas = df_fechas.sample(frac=1).reset_index(drop=True)
        # print(df_fechas)
        scrapper = Scrapper()
        for index, row in df_fechas.iterrows():
            print('iterrows:')
            print(f"Extraction date: {row['extraction_date']}")
            flight_date = row['flight_date']
            flight_date = (date.today() + timedelta(days=20)).strftime("%Y-%m-%d") # esto para debug solo
            print(f"Flight date: {flight_date}")
            page_source, df = scrapper.extract(self._origin,  
                                               self._destination,
                                               row['extraction_date'],
                                               flight_date
                                              )
            if self._persistence_format == 'raw':
                dao = RawDAO()
                dao.persist(page_source, 
                            self._origin,
                            self._destination,
                            row['extraction_date'],
                            flight_date
                           )
            else:
                if df is None:
                    raise Exception("no se ha podido parsear.")
                dao = FlightDAO()
                dao.persist(df, 
                            self._origin,
                            self._destination,
                            row['extraction_date'],
                            flight_date
                           )

if __name__ == "__main__":
    processor = Processor({'origin': 'MAD', 'destination': 'LCG'}, Planner())
    processor.play()
              