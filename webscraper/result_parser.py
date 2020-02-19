from bs4 import BeautifulSoup

class ResultParser:
    def __init__(self):
        pass

    def parse(selfSelf, page_source):
        pass

class ed_01(ResultParser):

    def __init__(self):
        pass

    def parse(selfSelf, page_source):
        print(f"la longitud de la page_source es '{page_source.__len__()}'")
        soup_level = BeautifulSoup(page_source, features='lxml')
        #
        resultados = soup_level.find_all('div', class_="result")
        esElPrimero = True
        for result in resultados:
            if (esElPrimero):
                price = result['data-price']
                groups = result.find_all('div', class_="itinerary_group")  # od-primary-flight-info-cities
                # print(f"son {len(groups)} groups")
                n_itinerary_group = 0
                for itinerary_group in groups:
                    if n_itinerary_group == 0:
                        print("IDA:")
                    elif n_itinerary_group == 1:
                        print("VUELTA:")
                    else:
                        print("PROBLEMA g-o-r-d-o de IDA y VUELTA")
                    # print("^^^^^^ ini-itinerary_group ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                    rows = itinerary_group.find_all('div', class_="itinerary_row")
                    # print(f"son {len(rows)} rows")
                    for itinerary_row in rows:
                        # print("^^^^^^ ini-itinerary_row ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
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
                                    print("tenemos un problemilla ke te kagas(*)")
                            else:
                                print("TENEMOS UN PROBLEMA KE TE KAGAS")
                        print(f"   FLIGHT NUMBER: {flight_number}")
                        print(f"   AIRLINE: {airline}")
                        print(f"   TIME: {departure_arrival_time}")
                        print(f"   ITINERARY: {origin_destination_airport}")
                        print("    --")
                        # print("^^^^^^ fin-itinerary_row ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                    n_itinerary_group = n_itinerary_group + 1
                    # print("^^^^^^ fin-itinerary_group ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                print(f"   PRICE: {price}")
                print("============================")
            esElPrimero = True
        return None