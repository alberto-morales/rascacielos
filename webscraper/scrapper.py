import yaml
import os 
from datetime import date, timedelta

from pandas import DataFrame

class Scrapper():

    def __init__(self, params=None):
        """The following params are required:

            origin
            destination
        """
        try:
            self._origin = params['origin']
            self._destination = params['destination']
        except Exception:                     
            raise ValueError("origin & destination values required")        

    def play(self):
        print(f'Origin is "{self._origin}"')
        print(f'Destination is "{self._destination }"')

if __name__ == "__main__":
    scrapper = Scrapper({'origin': 'MAD', 'destination': 'SLV'})
    scrapper.play()
              