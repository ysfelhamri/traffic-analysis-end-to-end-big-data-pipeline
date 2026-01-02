import numpy as np
import pandas as pd

def generate_data(num_events):
    highway_zones = {
        1: ("Rabat", "Mohammédia","Casablanca","Azemmour","El Jadida","Jorf Lasfar","Oualidia","Safi"),
        2: ("Rabat","Tiflet","Khémisset","Meknès","Fès","Taza","Guercif","Taourirt","Oujda"),
        3: ("Casablanca","Aéroport Mohammed V","Berrechid","Settat","Ben Guerir","Marrakech","Chichaoua","Agadir"),
        4: ("Berrechid","Ben Ahmed","Khouribga","Oued Zem","Bejaâd","Fkih Ben Saleh","Kasba Tadla","Beni Mellal"),
        5: ("Port Tanger Med","Tanger","Assilah","Larache","Kénitra","Salé","Rabat"),
        7: ("Tétouan","M'Diq","Marina Smir","Fnideq"),
        31: ("Tit Mellil","Médiouna","Deroua","Berrechid")
    }
    event_time = np.datetime64('now')
    data = []
    for _ in range(num_events):
        # Getting a random road type (A = Autoroute - Highway , AV = Avenue , S = Street)
        road_type = str(np.random.choice(['A','AV','S']))
        road_num = 0
        road_id = ''
        sensor_id = 'SE' + road_type
        vehicle_count = 0
        # Adding a delay (30 secs to 2 mins) between each event
        event_time += np.random.randint(30,120)
        event_timestamp = pd.Timestamp(event_time)
        dawn_timestamp = event_timestamp.replace(hour=6, minute=0, second=0)
        evening_timestamp = event_timestamp.replace(hour=18, minute=0, second=0)
        if road_type == 'A':
            # Generating a random road number between 1 and 7 for the highway type (highway number 6 does not exist in Morocco so it is replaces by highway 31)
            road_num = np.random.randint(1,8)
            if road_num == 6: 
                road_num = 31
            # Converting the road number to a String so it can be added to the road type
            road_id = road_type + str(road_num)
            # Getting a random zone from the highway_zones dictionary 
            zone = str(np.random.choice(highway_zones[road_num]))
            # Generating a sensor id based on the road type (from 1 to 9 for highways) and the road numbers
            sensor_id += str(np.random.randint(1,10)) + str(road_num)
            vehicle_count = np.random.randint(100,450)
            average_speed = np.random.normal(90,10)
            # Adding differences at night
            if event_timestamp > evening_timestamp and event_timestamp < dawn_timestamp:
                # Assuming that less vehicles are circulating at night
                vehicles_to_remove = np.random.randint(50,100)
                if vehicle_count > vehicles_to_remove:
                    vehicle_count -= vehicles_to_remove 
                # Assuming that the average speed is higher at night
                average_speed += np.random.randint(5,10)
            # Assuming that the highway road can accommodate 500 vehicles per minute
            occupancy_rate = vehicle_count / 500
        else:
            # Generating a random road number between 100 and 799 for avenues and streets (this is fully synthetic)
            road_num = np.random.randint(100,800)
            road_id = road_type + str(road_num)
            # Getting the hundreds of the road number (replacing 6 with 31 to use the highway_zones dict)
            road_num_hund = road_num // 100
            print(road_num_hund)
            if road_num_hund == 6: 
                road_num_hund = 31
            # Using the same highway_zone dict to get zones for avenues and streets
            zone = str(np.random.choice(highway_zones[road_num_hund]))
            if road_type == 'AV':
                # 11 to 19 for avenues
                sensor_id += str(np.random.randint(11,20)) + str(road_num)
                vehicle_count = np.random.randint(20,150)
                average_speed = np.random.normal(60,5)
                if event_timestamp > evening_timestamp and event_timestamp < dawn_timestamp:
                    vehicles_to_remove = np.random.randint(10,50)
                    if vehicle_count > vehicles_to_remove:
                        vehicle_count -= vehicles_to_remove 
                    average_speed += np.random.randint(5,10)
                occupancy_rate = vehicle_count / 200
            if road_type == 'S':
                # 21 to 29 for streets
                sensor_id += str(np.random.randint(21,30)) + str(road_num)
                vehicle_count = np.random.randint(1,50)
                average_speed = np.random.normal(45,5)
                if event_timestamp > evening_timestamp and event_timestamp < dawn_timestamp:
                    vehicles_to_remove = np.random.randint(0,50)
                    if vehicle_count > vehicles_to_remove:
                        vehicle_count -= vehicles_to_remove
                    average_speed += np.random.randint(5,10)
                occupancy_rate = vehicle_count / 60

            
        data.append([sensor_id, road_id, road_type, zone, vehicle_count, average_speed, occupancy_rate, event_time])
    dataframe = pd.DataFrame(data, columns=['sensor_id', 'road_id', 'road_type', 'zone', 'vehicle_count', 'average_speed', 'occupancy_rate', 'event_time'])

    return dataframe.to_json(orient = "records")

