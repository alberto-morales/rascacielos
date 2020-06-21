import yaml
import os 
from datetime import date, timedelta

from task_creator_one_way import Planner

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
        print(df_fechas)


if __name__ == "__main__":
    processor = Processor({'origin': 'MAD', 'destination': 'SLV'}, Planner())
    processor.play()
              