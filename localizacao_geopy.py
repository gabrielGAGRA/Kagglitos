import time
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.distance import distance
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderInsufficientPrivileges
        
DATA_URL = r"C:\Users\gabri\Documents\PROJETOS\PY\PJ_Code\DE\Data\dados_limpos.csv"
df = pd.read_csv(DATA_URL) 

cidadesDict= {}

for customerCity, sellerCity in zip(df['customer_city'], df['seller_city']):
    if pd.isna(customerCity):
        continue
    if customerCity != sellerCity: 
        cidadesSet = tuple(sorted({customerCity, sellerCity}))
        if cidadesSet not in cidadesDict:
            cidadesDict[cidadesSet] = 1

cidadesUnicas = set()
for city_pair in cidadesDict.keys():
    cidadesUnicas.update(city_pair)

geolocator = Nominatim(user_agent="geolocation_meumano")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

city_coordinates = {}
iteration_count = 0
dynamic_delay = 1

for city in cidadesUnicas:
    try:
        iteration_count += 1
        location = geocode(city)
        if location and not (pd.isna(location.latitude) or pd.isna(location.longitude)):
            city_coordinates[city] = (location.latitude, location.longitude)
        else:
            continue
    except GeocoderInsufficientPrivileges as e:
        print(f"Access denied on iteration {iteration_count}. Error: {e}")
        dynamic_delay *= 2  # Double the delay
        print(f"Increasing delay to {dynamic_delay} seconds.")
        time.sleep(dynamic_delay)  # Wait for a longer period before the next request
    except Exception as e:
        print(f"Error geocoding {city}: {e}")
    finally:
        print(f"Iteration {iteration_count}. Current delay: {dynamic_delay} seconds.")
        time.sleep(dynamic_delay)  # Ensure the dynamic delay is applied

city_pair_distances = {}
for city_pair in cidadesDict.keys():
    coords_1 = city_coordinates.get(city_pair[0])
    coords_2 = city_coordinates.get(city_pair[1])
    if coords_1 and coords_2:
        city_pair_distances[city_pair] = distance(coords_1, coords_2).km

df['distance'] = df.apply(lambda row: city_pair_distances.get(tuple(sorted([row['customer_city'], row['seller_city']])), pd.NA), axis=1)
df.loc[df['customer_city'] == df['seller_city'], 'distance'] = 0

df.to_csv('updated_distances.csv', index=False)