from bs4 import BeautifulSoup
import pandas as pd

class ResultParser:
    def __init__(self):
        pass

    def parse(selfSelf, page_source):
        pass

class ed_01(ResultParser):

    def __init__(self):
        pass

    def parse(selfSelf, page_source):
        res_price = []
        res_flight_number = []
        res_airline = []
        res_departure_time = []
        res_arrival_time = []
        res_origin_airport = []              
        res_destination_airport = []              
        res_index = []
        print(f"la longitud de la page_source es '{page_source.__len__()}'")
        soup_level = BeautifulSoup(page_source)
        #
        resultados = soup_level.find_all('div', {'class': 'result_wrapper'})
        esElPrimero = True
        index = 0
        for iteration in resultados: # itrera entre los resultados
            index = index + 1
            if (True): # esElPrimero (esto se pondria para que en vez todos, parsee únicamente el primer resultado)
                for content in iteration.contents: # itera entre las cajitas que componen UN resultado
                    result = None
                    try:
                       price = content['data-price']
                       result = content
                    except Exception as e:
                        pass
                    if result is not None:
                        groups = result.find_all('div', class_="itinerary_group")  
                        # print(f"son {len(groups)} groups")
                        n_itinerary_group = 0
                        for itinerary_group in groups:
                            if n_itinerary_group == 0:
                                print("IDA:")
                            elif n_itinerary_group == 1:
                                print("VUELTA:")
                            else:
                                print("PROBLEMA g-o-r-d-o de IDA y VUELTA")
                            rows = itinerary_group.find_all('div', class_="itinerary_row")
                            # print(f"son {len(rows)} rows")
                            for itinerary_row in rows:
                                departure_arrival_time = ""
                                origin_destination_airport = {}
                                flight_number = itinerary_row['data-flight-number']
                                tooltips = itinerary_row.find_all('img', class_='od-resultpage-segment-itinerary-title-carrier-logo')
                                airline = ""
                                for airline_tooltip in tooltips:
                                    airline = airline_tooltip.attrs['alt']
                                segments = itinerary_row.find_all('div', class_='od-resultpage-segment-info-tip')
                                # print(f"tiene {len(segments)} segments")
                                for segment_info in segments:
                                    # print(segment_info)
                                    segment_tips = segment_info.find_all('div')
                                    # print(f"tiene {len(segment_tips)} tips")
                                    if len(segment_tips) == 3:
                                        departure_arrival_time_tip = segment_tips[0]
                                        departure_arrival_time = departure_arrival_time_tip.get_text().strip()
                                        #
                                        origin_destination_airport_tip = segment_tips[2]
                                        origin_destination_airport_tip_children = origin_destination_airport_tip.find_all(
                                            'span')
                                        if len(origin_destination_airport_tip_children) == 2:
                                            origin_destination_airport['origin'] = origin_destination_airport_tip_children[
                                                0].get_text().strip()
                                            origin_destination_airport['destination'] = origin_destination_airport_tip_children[
                                                1].get_text().strip()
                                        else:
                                            print("tenemos un problemilla")
                                    else:
                                        print("TENEMOS UN PROBLEMA")
                                # print(f"   FLIGHT NUMBER: {flight_number}")
                                # print(f"   AIRLINE: {airline}")
                                # print(f"   TIME: {departure_arrival_time}")
                                # print(f"   ITINERARY: {origin_destination_airport}")
                                # print("^^^^^^ fin-itinerary_row ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                            n_itinerary_group = n_itinerary_group + 1
                            # print("^^^^^^ fin-itinerary_group ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                            print(f"   PRICE: {price}")
                            print(f"   FLIGHT NUMBER: {flight_number}")
                            print(f"   AIRLINE: {airline}")
                            times = departure_arrival_time.split(' - ', -1)
                            departure_time = times[0]
                            arrival_time = times[1]
                            arrival_time = arrival_time.replace('\n\n\n', ' ')
                            print(f"   DEPARTURE TIME: {departure_time}")
                            print(f"   ARRIVAL TIME: {arrival_time}")
                            origin_airport = origin_destination_airport['origin']
                            destination_airport = origin_destination_airport['destination']
                            print(f"   ORIGIN AIRPORT: {origin_airport}")
                            print(f"   DESTINATION AIRPORT: {destination_airport}")
                            res_price.append(price)
                            res_flight_number.append(flight_number.strip())
                            res_airline.append(airline.strip())
                            res_departure_time.append(departure_time.strip())
                            res_arrival_time.append(arrival_time.strip())
                            res_origin_airport.append(origin_airport.strip())
                            res_destination_airport.append(destination_airport.strip())
                            res_index.append(index)
                            print("============================")

            esElPrimero = False
        res_price = { 'price': res_price}
        res_flight_number = { 'flight_number': res_flight_number}
        res_airline = { 'airline': res_airline}
        res_departure_time = { 'departure_time': departure_time}
        res_arrival_time = { 'arrival_time': arrival_time}
        res_origin_airport = { 'origin_airport': res_origin_airport}
        res_destination_airport = { 'destination_airport': res_destination_airport}
        df_price = pd.DataFrame(res_price, index = res_index)
        df_flight_number = pd.DataFrame(res_flight_number, index = res_index)
        df_airline = pd.DataFrame(res_airline, index = res_index)
        df_departure_time = pd.DataFrame(res_departure_time, index = res_index)
        df_arrival_time = pd.DataFrame(res_arrival_time, index = res_index)
        df_origin_airport = pd.DataFrame(res_origin_airport, index = res_index)
        df_destination_airport = pd.DataFrame(res_destination_airport, index = res_index)
        df = df_price
        df = df.join(df_flight_number)
        df = df.join(df_airline)
        df = df.join(df_departure_time)
        df = df.join(df_arrival_time)
        df = df.join(df_origin_airport)
        df = df.join(df_destination_airport)
        print(df)
        return df