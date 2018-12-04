import datetime
import pickle
import requests
import geopy.distance
import time
import random
import csv
import copy
import tweepy


def update(apikey):
    with open('ferries.pickle', 'rb') as file:
        ferries = pickle.load(file)
    last = copy.deepcopy(ferries)
    response = requests.get('https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/ferries?debug=true', headers={'Authorization': apikey})
    data = parse(response, last)
    ferries = enrich(data, last)
    with open('ferries.pickle', 'wb') as file:
        pickle.dump(ferries, file)
    return ferries


def parse(response, last):
    ferries = copy.deepcopy(last)
    for entry in response.text.split('entity'):
        if 'label' not in entry:
            continue
        d = {}
        for i, element in enumerate(entry.split('\n')):
            if ':' in element:
                key, value = element.strip().split(': ')
                if key == 'timestamp':
                    value = int(value)
                elif key in ('latitude', 'longitude', 'bearing'):
                    value = float(value)
                elif key == 'speed':
                    value = float(value)
                else:
                    value = value.strip('"')
                d[key] = value
        if time.time() - d['timestamp'] > 120:
            pass
        elif ferries.get(d['label']) and last.get(d['label']) and ferries[d['label']]['timestamp'] < d['timestamp'] and last[d['label']]['timestamp'] < d['timestamp']:
            ferries[d['label']].update(d)
        elif last.get(d['label']) == None and ferries.get(d['label']) and ferries[d['label']]['timestamp'] < d['timestamp']:
            ferries[d['label']].update(d)
        elif last.get(d['label']) and ferries.get(d['label']) == None and last[d['label']]['timestamp'] < d['timestamp']:
            ferries[d['label']] = d
        elif last.get(d['label']) == None and ferries.get(d['label']) == None:
            ferries[d['label']] = d
    return ferries


def enrich(ferries, last):
    trips, routes, stops = info()
    for ferry in ferries:
        ferries[ferry]['destination'] = trips[ferries[ferry]['trip_id']]
        ferries[ferry]['route'], ferries[ferry]['service'] = routes[ferries[ferry]['trip_id'][:2]]
        ferries[ferry]['wharf'], ferries[ferry]['wharf_distance'] = closest_wharf((ferries[ferry]['latitude'], ferries[ferry]['longitude']), stops)
        if last.get(ferry) and last[ferry]['timestamp'] < ferries[ferry]['timestamp']:
            ferries[ferry]['last_wharf_distance'] = last[ferry]['wharf_distance']
            ferries[ferry]['last_wharf'] = last[ferry]['wharf']
            ferries[ferry]['last_speed'] = last[ferry]['speed']
            last[ferry]['speeds'].extend([ferries[ferry]['speed']])
            ferries[ferry]['speeds'] = last[ferry]['speeds'][-3:]
            ferries[ferry]['avg_speed'] = sum(ferries[ferry]['speeds']) / len(ferries[ferry]['speeds'])
            ferries[ferry]['arriving'], ferries[ferry]['departing'] = last[ferry]['arriving'], last[ferry]['departing']
        elif last.get(ferry) == None:
            ferries[ferry]['last_wharf_distance'] = ferries[ferry]['wharf_distance']
            ferries[ferry]['last_wharf'] = ferries[ferry]['wharf']
            ferries[ferry]['speeds'] = [ferries[ferry]['speed']]
            ferries[ferry]['last_speed'] = ferries[ferry]['speed']
            ferries[ferry]['avg_speed'] = ferries[ferry]['speed']
            ferries[ferry]['arriving'], ferries[ferry]['departing'] = (0, 0)
    return ferries


def info():
    trips = {}
    with open('trips.txt') as file:
        reader = csv.DictReader(file)
        for row in reader:
            trips[row['trip_id']] = row['trip_headsign']

    routes = {}
    with open('routes.txt') as file:
        reader = csv.DictReader(file)
        for row in reader:
            routes[row['route_id']] = (row['route_short_name'], row['route_long_name'])

    stops = {}
    with open('stops.txt') as file:
        reader = csv.DictReader(file)
        for row in reader:
            stops[row['stop_name']] = (float(row['stop_lat']), float(row['stop_lon']))

    return (trips, routes, stops)


def closest_wharf(ferry_location, stops):
    wharf = ''
    wharf_distance = 10**10
    for stop in stops:
        distance = geopy.distance.distance(ferry_location, stops[stop]).meters
        if distance < wharf_distance:
            wharf_distance = distance
            wharf = stop
    return (wharf, wharf_distance)


def tweet_maker(ferries, ferry):
    ferry = ferries[ferry]
    if is_arriving(ferry):
        ferry['arriving'] = ferry['timestamp']
        with open('ferries.pickle', 'wb') as file:
            pickle.dump(ferries, file)
        return arriving(ferry)
    elif is_departing(ferry):
        ferry['departing'] = ferry['timestamp']
        with open('ferries.pickle', 'wb') as file:
            pickle.dump(ferries, file)
        return departing(ferry)


def is_arriving(ferry):
    if ferry['avg_speed'] > 2 and ferry['speed'] < 2 and ferry['wharf_distance'] < 50 and ferry['timestamp'] - ferry['arriving'] > 120:
        return True


def is_departing(ferry):
    if ferry['last_speed'] < 2 and ferry['speed'] > 2 and ferry['last_wharf_distance'] < 50 and ferry['timestamp'] - ferry['departing'] > 120:
        return True


def arriving(ferry):
    ferry, wharf, latitude, longitude = ferry['label'], ferry['wharf'], ferry['latitude'], ferry['longitude']
    textfile = 'arriving.txt'
    tweets = []
    with open(textfile) as file:
        for line in file:
            tweets.append(line.format(ferry=ferry, wharf=wharf).strip())
    return f"{random.choice(tweets)}\n({latitude}, {longitude})"


def departing(ferry):
    ferry, wharf, latitude, longitude = ferry['label'], ferry['wharf'], ferry['latitude'], ferry['longitude']
    textfile = 'departing.txt'
    tweets = []
    with open(textfile) as file:
        for line in file:
            tweets.append(line.format(ferry=ferry, wharf=wharf).strip())
    return f"{random.choice(tweets)}\n({latitude}, {longitude})"


def get_api(cfg):
    auth = tweepy.OAuthHandler(cfg['consumer_key'], cfg['consumer_secret'])
    auth.set_access_token(cfg['access_token'], cfg['access_token_secret'])
    return tweepy.API(auth)


def main():
    ferry = 'Friendship'

    apikey = 'apikey YOUR_APIKEY_HERE'

    cfg = {
        'consumer_key': 'YOUR_CONSUMER_KEY_HERE',
        'consumer_secret': 'YOUR_CONSUMER_SECRET_HERE',
        'access_token': 'YOUR_ACCESS_TOKEN_HERE',
        'access_token_secret': 'YOUR_ACCESS_TOKEN_SECRET_HERE'
    }

    api = get_api(cfg)

    ferries = update(apikey)

    tweet = tweet_maker(ferries, ferry)
    if tweet:
        status = api.update_status(status=tweet)
        with open('tweets.txt', 'a') as file:
            file.write(f'{datetime.datetime.now()}: {tweet}\n')


if __name__ == '__main__':
    main()
