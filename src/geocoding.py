import requests
import numpy as np
from tqdm import tqdm

class GeoCoder:
    """
    Uses Google Maps API to get geocodes (lat/long) of city/statenames.

    Takes about 30 min for 60,000 records due to caching.

    """
    def __init__(self, apikey):
        self._apikey = apikey
        self.cache = {}

    def get_geocode(self, city: str, state: str):
        """
        Returns latitude and longitude for cities.

        :param city:
        :param state:
        :return:
        """
        cityname = city.lower()
        statename = state.lower()

        key = cityname, statename
        if key in self.cache:
            return self.cache[key]

        pattern = 'https://maps.googleapis.com/maps/api/geocode/json?address={},+{}&key={}'
        raw = requests.get(pattern.format(cityname, statename, self._apikey))
        try:
            js = raw.json()['results'][0]['geometry']['location']

            lat, lng = js['lat'], js['lng']
            self.cache[key] = lat, lng
            return lat, lng

        except:
            print(key)
            return np.nan, np.nan


if __name__ == '__main__':
    import pandas as pd
    with open('/Users/chris/PycharmProjects/defense_spending/src/apikey.config') as f:
        apikey = f.read()

    gc = GeoCoder(apikey)

    data = pd.read_csv('/Users/chris/PycharmProjects/defense_spending/data/dataset_1.csv')

    longs = []
    lats = []

    for ct, st in zip(tqdm(data.city), data.stxate):
        lat, lng = gc.get_geocode(ct, st)
        lats.append(lat)
        longs.append(lng)

    data['lng'] = longs
    data['lat'] = lats
    data.to_csv('/Users/chris/PycharmProjects/defense_spending/data/dataset_2.csv')

