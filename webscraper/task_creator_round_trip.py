import yaml
import os 
from datetime import date, timedelta

from pandas import DataFrame

class Planner():
    def __init__(self, params=None):
        """The following configuration elements are required:

            planner.number_of_days
            planner.max_days_of_stay
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + '/webscrapper.yaml') as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
            self._NUMBER_OF_DAYS = config['planner']['number_of_days']
            self._MAX_DAYS_OF_STAY = config['planner']['max_days_of_stay']


    def create_tasks(self):
        dates = DataFrame(columns=('outbound_date', 'inbound_date'))
        today = date.today()
        outer_days_list = [today + timedelta(days=d) for d in range(1, self._NUMBER_OF_DAYS + 1)]
        for outbound_date in outer_days_list:
            inner_days_list = [outbound_date + timedelta(days=d) for d in range(self._MAX_DAYS_OF_STAY + 1)]
            for inbound_date in inner_days_list:
#               print(f'de {outbound_date.strftime("%d/%m/%y")} a {inbound_date.strftime("%d/%m/%y")}')
                dates.loc[len(dates)]=[outbound_date.strftime("%d/%m/%y"),inbound_date.strftime("%d/%m/%y")]
        return dates

if __name__ == '__main__':
    planner = Planner()
    dates = planner.create_tasks()
    print(dates)