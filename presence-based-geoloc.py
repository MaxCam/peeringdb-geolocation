# coding=latin-1
import sys, gzip, random
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy import geocoders
import numpy as np
import geoip2.database
import PeeringDB
from Atlas import Atlas

target_ip = sys.argv[1]
target_asn = int(sys.argv[2])
probes_num = int(sys.argv[3])
packets_num = int(sys.argv[4])
ATLAS_API_KEY = sys.argv[5]
GMAP_API_KEY = sys.argv[6]

gmap_geolocator = geocoders.GoogleV3(api_key = GMAP_API_KEY)


def get_location_coordinates(target_location):
    """
    Looks up the coordinates for the target location
    :param target_location:
    :return: a dictionary with the latitude, longitude, city name and country code according to Google Maps API
    """
    city_coordinates = dict()
    location = gmap_geolocator.geocode(target_location, timeout=30)
    if location is not None:
        if "geometry" in location.raw and "location" in location.raw["geometry"]:
            city_coordinates["lat"] = location.raw["geometry"]["location"]["lat"]
            city_coordinates["lng"] = location.raw["geometry"]["location"]["lng"]
        if "address_components" in location.raw:
            for address_component in location.raw["address_components"]:
                if "types" in address_component:
                    if "locality" in address_component["types"]:
                        city_coordinates["city"] = address_component["short_name"]
                    elif "country" in address_component["types"]:
                        city_coordinates["country"] = address_component["short_name"]

    if len(city_coordinates) == 4:
        return city_coordinates
    else:
        return False


peeringdb_api = PeeringDB.API()
asn_location = peeringdb_api.get_asn_locations(target_asn).locations

# Get the possible location according to MaxMind
reader = geoip2.database.Reader('data/GeoLite2-City.mmdb')
response = reader.city(target_ip)
maxmind_city = response.city.name
if maxmind_city is not None:
    maxmind_city = maxmind_city.lower()
maxmind_country = response.country.iso_code.lower()

# if maxmind indicates a country but the city is 'none', find the city with the largest population in that country
if str(maxmind_city) == "None" and str(maxmind_country) != "None":
    with gzip.open("data/worldcitiespop.txt.gz") as fin:
        maxmind_city = None
        largest_city_pop = 0
        for line in fin:
            lf = line.strip().split(",")
            if len(lf) > 0 and maxmind_country == lf[0]:
                try:
                    if int(lf[4]) > largest_city_pop:
                        largest_city_pop = int(lf[4])
                        maxmind_city = lf[1].lower()
                except ValueError, e:
                    continue

maxmind_location = "%s|%s" % (maxmind_city, maxmind_country)
asn_location.add(maxmind_location)

print "Possible locations:"
for location in asn_location:
    print location

# Get the longitude and latitude of the cities
city_coordinates = dict()
with open("data/world_cities.csv", "r") as fin:
    for line in fin:
        lf = line.strip().split(",")
        if len(lf) > 0:
            location = "%s %s" % (lf[0].lower(), lf[6].lower())
            city_coordinates[location] = (lf[3], lf[2]) # long, lat

#TODO Order countries by number of presences to find the main country from which we start the measurements

atlas_api = Atlas(ATLAS_API_KEY)
target_asn_probes = set()
candidate_probes = dict()
probe_location = dict()
geolocator = Nominatim() # The geopy geolocator
for location in asn_location:
    location = location.lower()
    # Get the coordinates for this location
    location_data = get_location_coordinates(location)
    if location_data is not False:
        # Get the probes in this location
        available_probes = atlas_api.select_probes_in_location(
            location_data["lat"],
            location_data["lng"],
            location_data["country"],
            40
        )

        if len(available_probes) > 0:
            gmap_location = "%s|%s" % (location_data["city"], location_data["country"])
            if gmap_location not in candidate_probes:
                candidate_probes[gmap_location] = set()

            for probe_id in available_probes:
                candidate_probes[gmap_location].add(probe_id)
                probe_location[probe_id] = gmap_location
        else:
            print "Warning: No available probes in the location %s %s: " % (location_data["city"], location_data["country"])
    else:
        print "Warning: Could not find the coordinates for %s: " % location

# Get the probes in the target ASN
for probe_object in atlas_api.select_probes_in_asn(target_asn):
    target_asn_probes.add(probe_object.id)
    reverse_location = geolocator.reverse("%s, %s" % (probe_object.lat, probe_object.lng), language='en')
    probe_location[probe_object.id] = reverse_location.address

print candidate_probes.keys()
selected_probes = set()
location_rtt = dict()
for location in candidate_probes:
    if probes_num > len(candidate_probes[location]):
        selected_probes |= set(candidate_probes[location])
    else:
        selected_probes |= set(random.sample(candidate_probes[location], probes_num))

selected_probes |= target_asn_probes

if len(selected_probes) > 0:
    af = 4
    description="Presence-informed RTT geolocation"

    ping_results = atlas_api.ping_measurement(af, target_ip, description, packets_num, selected_probes)

    prv_min_rtt = sys.maxint
    closest_probe = 0
    for probe_id in ping_results:
        probe_min_rtt =  min(ping_results[probe_id])
        if probe_min_rtt < prv_min_rtt:
            prv_min_rtt = probe_min_rtt
            closest_probe = probe_id

    if prv_min_rtt < 10:
        print target_ip, probe_id, probe_location[closest_probe], prv_min_rtt
    else:
        print "Error: Couldn't converge to a target. Possibly incomplete presence data"
        print "The closest probe for %s is %s in %s with RTT %s", (target_ip, probe_id, probe_location[closest_probe], prv_min_rtt)

else:
    print "Error: couldn't find any Atlas probe in the requested locations"
