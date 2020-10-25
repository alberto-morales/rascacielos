import logging
import joblib
import os
from datetime import datetime
from http import HTTPStatus
from http import HTTPStatus
from flask import request, jsonify
from flask_swagger import swagger

from serving.microservice import application
from webscraper.scrapper import Scrapper

BASE_MODELS_PATH = os.path.dirname(os.path.abspath(__file__)) + "/../../models"

@application.route('/')
def ping():
    return "System up & running."

@application.route('/rascacielos')
def recomendacion():
    try:
        orig_param = request.args.get('destination')
        dest_param = request.args.get('origin')
        date_param = request.args.get('flight_date')
        if (dest_param and orig_param and date_param):
            r_message, r_code, r_infered_min_price, r_actual_price = _generate_result(orig_param, dest_param, date_param)
        else: 
            raise Exception("origin & destination & flight_date params required.")
    except Exception as e:
        custom_exception = Exception(e)
        return _ko_msg(custom_exception), HTTPStatus.INTERNAL_SERVER_ERROR.value
    return _ok_msg(r_message, r_code, r_infered_min_price, r_actual_price)

def _generate_result(origin, destination, date_param):
    model = None
    if (origin == 'MAD' and destination == 'BCN'):
        logging.getLogger().info("MAD==>BCN")
        model_name = "MAD-BCN.bin"
    elif (origin == 'BCN' and destination == 'MAD'):
        logging.getLogger().info("BCN==>MAD")
        model_name = "BCN-MAD.bin"
    elif (origin == 'MAD' and destination == 'LCG'):
        logging.getLogger().info("MAD==>LCG")
        model_name = "MAD-LCG.bin"
    elif (origin == 'LCG' and destination == 'MAD'):
        logging.getLogger().info("LCG==>MAD")
        model_name = "LCG-MAD.bin"
    else:
        raise Exception("Route not supported.")
    model_path = os.path.join(BASE_MODELS_PATH, model_name)
    model = joblib.load(model_path)
    #
    now = datetime.now()
    today = now.date()
    flight_date = datetime.strptime(date_param, '%Y-%m-%d').date()
    lag = (flight_date - today).days
    #
    scrapper = Scrapper()
    page_source, df = scrapper.extract(origin, destination, date_param)
    min_price = _calculate_min_price(df)
    #
    x_pred = (min_price, lag)
    y_pred = model.predict(x_pred)
    print(f"Prediccion de ({x_pred}) = {y_pred}")
    #
    r_infered_min_price = y_pred[0]
    # r_infered_min_price = 
    #
    if (r_infered_min_price < min_price):
        r_message = "Recomendation: wait.", 
        r_code = 0
    else: 
        r_message = "Recomendation: buy.", 
        r_code = 1

    r_actual_price = min_price
    return r_message, r_code, r_infered_min_price, r_actual_price

def _calculate_min_price(df):
    min_price = 999999
    for n in range(df.shape[0]):
        row = df.iloc[n]
        price_str = row['price']
        departure_time = row['departure_time']
        arrival_time = row['arrival_time']
        airline = row['airline']
        #
        if (arrival_time.find('+') < 0 and airline != 'Renfe'):
            departure_hour = int(departure_time[:2])
            arrival_hour = int(arrival_time[:2])
            duration = arrival_hour - departure_hour
            if (duration < 4):
                price = float(price_str)
                if (price < min_price):
                    min_price = price
    # min_price = 
    return min_price

def _days_between(d1, d2):
    return abs(d2 - d1).days    

def _ko_msg(e):
    try:
        message = e.message()
    except Exception:
        message = str(e)
    result = {
        "message": message
    }
    return result

def _ok_msg(message, code, infered_min_price, actual_price):
    result = {
        "message": message,
        "recomendation-code": code,
        "infered-min-price": infered_min_price,
        "acutal-price": actual_price
    }
    return result