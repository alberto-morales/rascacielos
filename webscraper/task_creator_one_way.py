import yaml
import os 
from datetime import date, timedelta

from pandas import DataFrame

class Planner():
    def __init__(self, params=None):
        """The following configuration elements are required:

            planner.number_of_days
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + '/webscrapper.yaml') as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
            self._NUMBER_OF_DAYS = config['planner']['number_of_days']
            print(f'NUMBER_OF_DAYS initialized with {self._NUMBER_OF_DAYS}')


    def create_tasks(self):
        dates = DataFrame(columns=('extraction_date','flight_date',))
        today = date.today()
        days_list = [today + timedelta(days=d) for d in range(1, self._NUMBER_OF_DAYS + 1)]
        for flight_date in days_list:
            # print(f'el {flight_date.strftime("%Y-%m-%d")} ')
            dates.loc[len(dates)]=[today.strftime("%Y-%m-%d"), flight_date.strftime("%Y-%m-%d")]
        return dates

if __name__ == '__main__':
    planner = Planner()
    dates = planner.create_tasks()
    print(dates)