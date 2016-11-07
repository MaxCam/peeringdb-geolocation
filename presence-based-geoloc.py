# coding=latin-1
import requests, time, sys, pickle, gzip, random
from json import dumps, loads, JSONEncoder, JSONDecoder
from collections import OrderedDict
from geopy import distance
from geopy import Point
from geopy.geocoders import Nominatim
from datetime import datetime
import numpy as np
import geoip2.database
from ripe.atlas.cousteau import (
  Ping,
  AtlasCreateRequest,
  AtlasSource,
  AtlasStream,
  ProbeRequest
)
from ripe.atlas.cousteau.source import MalFormattedSource
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()
import PeeringDB

def on_result_response(*args):
    """
    Function that will be called every time we receive a new result.
    Args is a tuple, so you should use args[0] to access the real message.
    """
    global location_rtt
    global probe_location
    min_rtt = sys.maxint
    result = args[0]['result']
    print probe_location[args[0]["prb_id"]], result
    for reply in result:
        if "rtt" in reply:
            rtt = reply["rtt"]
            if rtt < min_rtt:
                min_rtt = rtt

    location = probe_location[args[0]["prb_id"]]
    if location not in location_rtt:
        location_rtt[location] = list()
    if min_rtt != sys.maxint:
        location_rtt[location].append(min_rtt)


class PythonObjectEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (list, dict, str, unicode, int, float, bool, type(None))):
            return JSONEncoder.default(self, obj)
        return {'_python_object': pickle.dumps(obj)}


target_ip = sys.argv[1]
target_asn = int(sys.argv[2])
probes_num = int(sys.argv[3])
packets_num = int(sys.argv[4])
ATLAS_API_KEY = sys.argv[5]

ping = Ping(af=4, target=target_ip, description="Presence-informed RTT geolocation", packets=packets_num)

peeringdb_api = PeeringDB.API()
asn_location = peeringdb_api.get_asn_locations(target_asn).locations

# Get the possible location according to MaxMind
reader = geoip2.database.Reader('geolocation/GeoLite2-City.mmdb')
response = reader.city(target_ip)
maxmind_city = response.city.name
if maxmind_city is not None:
    maxmind_city = maxmind_city.lower()
maxmind_country = response.country.iso_code.lower()

# if maxmind indicates a country but the city is 'none', find the city with the largest population in that country
if str(maxmind_city) == "None" and str(maxmind_country) != "None":
    with gzip.open("geolocation/worldcitiespop.txt.gz") as fin:
        maxmind_city = None
        largest_city_pop = 0
        for line in fin:
            lf = line.strip().split(",")
            if len(lf) > 0 and maxmind_country == lf[0]:
                try:
                    if int(lf[4]) > largest_city_pop:
                        largest_city_pop = int(lf[4])
                        maxmind_city = lf[1].lower()
                        print largest_city_pop
                except ValueError, e:
                    continue

maxmind_location = "%s|%s" % (maxmind_city, maxmind_country)
asn_location.add(maxmind_location)

print "Possible locations according to PeeringDB:"
for location in asn_location:
    print location

# Get the longitude and latitude of the cities
city_coordinates = dict()
with open("geolocation/world_cities.csv", "r") as fin:
    for line in fin:
        lf = line.strip().split(",")
        if len(lf) > 0:
            location = "%s|%s" % (lf[0].lower(), lf[6].lower())
            city_coordinates[location] = (lf[3], lf[2]) # long, lat

# TODO Order countries by number of presences to find the main country from which we start the measurements

target_asn_probes = set()
candidate_probes = dict()
probe_location = dict()
geolocator = Nominatim() # The geopy geolocator
for location in asn_location:
    location = location.lower()
    candidate_probes[location] = set()
    if location not in city_coordinates:
        print "We do not have the coordinates for the location: %s" % location
        continue
    coordinates = city_coordinates[location]
    city = location.split("|")[0]
    country = location.split("|")[1]
    # Get the probes in the same city
    filters = {"country_code": country, "status": 1}
    probes = ProbeRequest(**filters)

    for probe in probes:
        if probe["asn_v4"] is not None and probe["geometry"]["type"] == "Point":
            probe_lon = probe["geometry"]["coordinates"][0]
            probe_lat = probe["geometry"]["coordinates"][1]
            if probe["asn_v4"] == target_asn:
                target_asn_probes.add(probe["id"])
                reverse_location = geolocator.reverse("%s, %s" % (probe_lat, probe_lon), language='en')
                probe_location[probe["id"]] = reverse_location.address
            else:
                p1 = Point("%s %s" % (coordinates[0], coordinates[1]))
                p2 = Point("%s %s" % (probe_lon, probe_lat))
                result = distance.distance(p1, p2).kilometers
                if result <= 40:
                    candidate_probes[location].add(probe["id"])
                    probe_location[probe["id"]] = location

selected_probes = set()
location_rtt = dict()
for location in asn_location:
    if len(candidate_probes[location]) == 0:
        print "Error: Could not find the appropriate probes for the location: %s" % location
        #print "Exiting ..."
        #sys.exit(-1)
    else:
        if probes_num > len(candidate_probes[location]):
            selected_probes |= set(candidate_probes[location])
        else:
            selected_probes |= set(random.sample(candidate_probes[location], probes_num))

selected_probes |= target_asn_probes

source = AtlasSource(
    value=','.join(str(x) for x in selected_probes),
    requested=len(selected_probes),
    type="probes"
)

atlas_request = AtlasCreateRequest(
    start_time=datetime.utcnow(),
    key=ATLAS_API_KEY,
    measurements=[ping],
    sources=[source],
    is_oneoff=True
)

try:
    (is_success, response) = atlas_request.create()

    measurement_id = response["measurements"][0]

    atlas_stream = AtlasStream()
    atlas_stream.connect()
    # Measurement results
    channel = "result"
    # Bind function we want to run with every result message received
    atlas_stream.bind_channel(channel, on_result_response)
    # Subscribe to new stream for 1001 measurement results
    stream_parameters = {"msm": measurement_id}
    atlas_stream.start_stream(stream_type="result", **stream_parameters)

    # Timeout all subscriptions after 5 secs. Leave seconds empty for no timeout.
    # Make sure you have this line after you start *all* your streams
    atlas_stream.timeout(seconds=120)
    # Shut down everything
    atlas_stream.disconnect()

    prv_min_rtt = sys.maxint
    closest_location = "undecided"
    for location in location_rtt:
        min_rtt = min(location_rtt[location])
        if min_rtt < prv_min_rtt:
            prv_min_rtt = min_rtt
            closest_location = location

    if prv_min_rtt < 10:
        print target_ip, closest_location, prv_min_rtt
    else:
        print "Error: Couldn't converge to a target. Possibly incomplete presence data"

except MalFormattedSource, e:
    print "Unable to create Atlas measurement. Error:\n%s\n" % str(e)