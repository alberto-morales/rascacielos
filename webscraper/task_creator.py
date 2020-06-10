from configparser import ConfigParser

from datetime import date, timedelta

from pandas import DataFrame

config = ConfigParser()
config.read('webscrapper.cfg')
NUMBER_OF_DAYS   = int(config.get('planner','number_of_days'))
MAX_DAYS_OF_STAY = int(config.get('planner','max_days_of_stay'))

class Planner():

    def create_tasks(self):
        dates = DataFrame(columns=('start_date', 'end_date'))
        today = date.today()
        outer_days_list = [today + timedelta(days=d) for d in range(1, NUMBER_OF_DAYS + 1)]
        for start_date in outer_days_list:
            inner_days_list = [start_date + timedelta(days=d) for d in range(MAX_DAYS_OF_STAY + 1)]
            for end_date in inner_days_list:
#               print(f'de {start_date.strftime("%d/%m/%y")} a {end_date.strftime("%d/%m/%y")}')
                dates.loc[len(dates)]=[start_date.strftime("%d/%m/%y"),end_date.strftime("%d/%m/%y")]
        return dates

if __name__ == '__main__':
    planner = Planner()
    dates = planner.create_tasks()
    print(dates)