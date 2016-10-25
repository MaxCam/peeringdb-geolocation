# coding=utf-8
import requests, time, sys, pickle, gzip, random
from json import dumps, loads, JSONEncoder, JSONDecoder
from collections import OrderedDict
from geopy import distance
from geopy import Point
from datetime import datetime
from ripe.atlas.cousteau import ProbeRequest
import numpy as np
from ripe.atlas.cousteau import (
  Ping,
  AtlasCreateRequest,
  AtlasSource,
  AtlasStream
)

import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()
reload(sys)
sys.setdefaultencoding("utf-8")


def on_result_response(*args):
    """
    Function that will be called every time we receive a new result.
    Args is a tuple, so you should use args[0] to access the real message.
    """
    global location_rtt
    global probe_location
    min_rtt = sys.maxint
    result = args[0]['result']
    print result
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


ATLAS_API_KEY = "<ATLAS_KEY>"

class PythonObjectEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (list, dict, str, unicode, int, float, bool, type(None))):
            return JSONEncoder.default(self, obj)
        return {'_python_object': pickle.dumps(obj)}

class IXP(object):
    def __init__(self, id, name, name_long, org_id, city, country, region_continent):
        self.id = id
        self.name = name
        self.name_long = name_long
        self.org_id = org_id
        self.city = city
        self.country = country
        self.region_continent = region_continent
        self.members = set()
        self.facilities = set()

    def add_member(self, member_id):
        self.members.add(member_id)

    def add_facility(self, facility_id):
        self.facilities.add(facility_id)

    def get_facilities(self):
        return self.facilities


def get_request(endpoint):
    base_url = "https://peeringdb.com/api/"
    query = base_url + endpoint
    response = requests.get(query)
    return response.json()

target_ip = sys.argv[1]
target_asn = int(sys.argv[2])
probes_num = int(sys.argv[3])
packets_num = int(sys.argv[4])

ping = Ping(af=4, target=target_ip, description="Presence-informed RTT geolocation", packets=packets_num)

asn_location = set()

# Get the IXPs
ix = get_request("ix")
id_ix_mapping = dict()
ix_id_mapping = dict()
for ix_object in ix["data"]:
    ixp = IXP(ix_object["id"], ix_object["name"].strip(), ix_object["name_long"], ix_object["org_id"], ix_object["city"],
              ix_object["country"], ix_object["region_continent"])
    ix_id_mapping[ix_object["name"].strip().encode("utf-8")] = ix_object["id"]
    id_ix_mapping[ix_object["id"]] = ixp


# Get cities where the ASN has facility presences
netfac = get_request("netfac")
for netfac_object in netfac["data"]:
    if netfac_object["local_asn"] == target_asn:
        city = netfac_object["city"]
        country = netfac_object["country"]
        location = "%s|%s" % (city, country)
        asn_location.add(location)

# Get the cities where the ASN has IXP presences
netixlan = get_request("netixlan")
for netixlan_object in netixlan["data"]:
    if netixlan_object["asn"] == target_asn:
        ix_id = netixlan_object["ix_id"]
        ix_object = id_ix_mapping[ix_id]
        city = ix_object.city
        country = ix_object.country
        location = "%s|%s" % (city, country)
        asn_location.add(location)

print "Possible locations according to PeeringDB:"
for location in asn_location:
    print location

# Get the longitude and latitude of the cities
city_coordinates = dict()
with open("geolocation/world_cities.csv", "r") as fin:
    for line in fin:
        lf = line.strip().split(",")
        if len(lf) > 0:
            location = "%s|%s" % (lf[0], lf[6])
            city_coordinates[location] = (lf[3], lf[2]) # long, lat

candidate_probes = dict()
probe_location = dict()
for location in asn_location:
    candidate_probes[location] = set()
    if location not in city_coordinates: continue
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

prv_median_min_rtt = sys.maxint
closest_location = "undecided"
for location in location_rtt:
    data = np.array(location_rtt[location])
    median_min_rtt = np.median(data)
    if median_min_rtt < prv_median_min_rtt:
        prv_median_min_rtt = median_min_rtt
        closest_location = location

if prv_median_min_rtt < 10:
    print target_ip, closest_location, prv_median_min_rtt
else:
    print "Error: Couldn't converge to a target. Possibly incomplete presence data"