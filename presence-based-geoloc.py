# coding=latin-1
import sys, gzip, random
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy import geocoders
import numpy as np
import geoip2.database
import pyasn
import ConfigParser
import PeeringDB
from Atlas import Atlas
import logging

target_ip = sys.argv[1]
ipasn_file = sys.argv[2]

# Map the target IP to the corresponding ASN
asndb = pyasn.pyasn(ipasn_file)
target_asn, prefix = asndb.lookup(target_ip)


def read_config():
    """
    Reads the configuration parameters and maps each section and option in the configuration file to a dictionary
    :return: the dictionary including all the configuration parameters
    """
    config = dict()
    config_parser = ConfigParser.ConfigParser()
    config_parser.read("config/config.ini")
    for section in config_parser.sections():
        options = config_parser.options(section)
        config[section] = dict()
        for option in options:
            try:
                config[section][option] = config_parser.get(section, option)
            except:
                config[section][option] = None
    return config


def write_location_coordinates(location_id, location_data, coordinates_file):
    """
    Append the data provided by the Google Maps API for a specific PeeringDB location to the corresponding file
    :param location: The location id, in the format of city|country_2-letter_iso_code
    :param location_data: The dictionary with the Google Maps data on the location indicated by :location_id
    :param coordinates_file the file where the data will be stored
    :return: the success status of appending to file (true or false)
    """
    success = True
    try:
        with open(coordinates_file, "a+") as fout:
            outline = u'%s\t%s\t%s\t%s\t%s\n' % (
                location_id,
                location_data["lat"],
                location_data["lng"],
                location_data["city"],
                location_data["country"]
            )

            fout.write(outline.encode('utf-8'))

    except (IOError, UnicodeEncodeError) as e:
        logging.error("Appending to file %s failed with error: %s" % (coordinates_file, str(e)))
        success = False
    return  success


def read_location_coordinates(coordinates_file):
    """
    Read the coordinates, city name and country iso code according to Google maps for PeeringDB locations that have been
    encountered in past geolocations
    :param coordinates_file: the file where the location data are stored
    :return: a dictionary that maps PeeringDB locations to the stored data obtained through the Google Maps API
    """
    location_coordinates = dict()
    try:
        with open(coordinates_file) as fin:
            for line in fin:
                lf = line.strip().split("\t")
                if len(lf) > 0:
                    location_id = lf[0]
                    location_coordinates[location_id] = {
                        "lat": lf[1],
                        "lng": lf[2],
                        "city": lf[3],
                        "country": lf[4]
                    }
    except IOError:
        logging.error("Could not read file %s" % coordinates_file)

    return location_coordinates


def write_coordinates_location(lat, lng, coorindates_data, probes_locations_file):
    """
    Append the data provided from the Google Maps API for a latitude and longitude in the correspoding file
    :param coorindates_data: the data to append (latitude, longitude, city name, country iso code)
    :param probes_locations_file: the path to the file where the coordinates data will be appended
    :return: the probes_locations_file status of appending to the file (true or false)
    """
    success = True
    try:
        with open(probes_locations_file, "a+") as fout:
            outline = u'%s\t%s\t%s\t%s\t%s\n' % (
                lat,
                lng,
                coorindates_data["locality"],
                coorindates_data["admn_lvl_2"],
                coorindates_data["country"]
            )

            fout.write(outline.encode('utf-8'))

    except (IOError, UnicodeEncodeError) as e:
        logging.error("Appending to file %s failed with error: %s" % (probes_locations_file, str(e)))
        success = False

    return success


def read_coordinates_location(probes_locations_file):
    """
    Read the city name and country iso code according to Google maps for probes coordinates that have been
    encountered in past geolocations
    :param probes_locations_file: the file with the coordinates data
    :return: a dictionary that maps the coordinates to the corresponding data
    """
    probes_locations = dict()
    try:
        with open(probes_locations_file) as fin:
            for line in fin:
                lf = line.strip().split("\t")
                if len(lf) > 0:
                    location_id = "%s,%s" % (lf[0], lf[1])
                    probes_locations[location_id] = {
                        "locality": lf[2],
                        "admn_lvl_2": lf[3],
                        "country": lf[4]
                    }
    except IOError:
        logging.error("Could not read file %s" % probes_locations_file)

    return probes_locations


def query_location_coordinates(target_location):
    """
    Queries the Google Maps API for the coordinates for the target location
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


def query_coordinates_location(lat, lng):
    """
    Queries the Google Maps API the location of a set of coordinates and returns the city name and country iso code
    :param lat: The latitude of the location
    :param lng: The longitude of the location
    :return: a dictionary with the city name and the country iso code
    """
    reverse_location = gmap_geolocator.reverse("%s, %s" % (lat, lng), exactly_one = True, language='en')
    coordinates_data = {
        "admn_lvl_2": False,
        "locality": False,
        "country": False
    }

    if "address_components" in reverse_location.raw:
        for address_component in reverse_location.raw["address_components"]:
            if "types" in address_component:
                if "administrative_area_level_2" in address_component["types"]:
                    coordinates_data["admn_lvl_2"] = address_component["short_name"]
                if "locality" in address_component["types"]:
                    coordinates_data["locality"] = address_component["short_name"]
                if "country" in address_component["types"]:
                    coordinates_data["country"] = address_component["short_name"]
    return coordinates_data

# Read the configuration parameters
config = read_config()
probes_num = int(config["PingParameters"]["probes_per_city"])
packets_num = int(config["PingParameters"]["packets_number"])
ip_version = int(config["PingParameters"]["ip_version"])
ATLAS_API_KEY = config["ApiKeys"]["atlas_key"]
GMAP_API_KEY = config["ApiKeys"]["gmap_key"]
cached_coordinates_file = config["FilePaths"]["city_coordinates"]
cached_probes_locations_file = config["FilePaths"]["probes_locations"]

# Read the coordinates for locations that have been encountered in past runs
cached_location_coordinates = read_location_coordinates(cached_coordinates_file)
cached_probes_locations = read_coordinates_location(cached_probes_locations_file)
# Create the Google Maps API geolocator
gmap_geolocator = geocoders.GoogleV3(api_key = GMAP_API_KEY)

peeringdb_api = PeeringDB.API()
asn_location = peeringdb_api.get_asn_locations(target_asn).locations

# Get the possible location according to MaxMind
reader = geoip2.database.Reader('data/GeoLite2-City.mmdb')
response = reader.city(target_ip)
if response.country is not None:
    maxmind_city = response.city.name
    if maxmind_city is not None:
        maxmind_city = maxmind_city.lower()
    maxmind_country = response.country.iso_code

    # if maxmind indicates a country but the city is 'none', find the city with the largest population in that country
    if str(maxmind_city) == "None" and str(maxmind_country) != "None":
        maxmind_country = maxmind_country.lower()
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

#TODO Order countries by number of presences to find the main country from which we start the measurements

atlas_api = Atlas(ATLAS_API_KEY)
target_asn_probes = set()
candidate_probes = dict()
probe_location = dict()
geolocator = Nominatim() # The geopy geolocator
for location in asn_location:
    location = location.lower()
    # Get the coordinates for this location
    if location in cached_location_coordinates:
        # if we have found the coordinates for this location before read it from the cached coordinates file ...
        location_data = cached_location_coordinates[location]
    else:
        # ... otherwise query the Google Maps API for the coordinates ...
        location_data = query_location_coordinates(location)
        # ... and store the coordinates in the corresponding file
        write_location_coordinates(location, location_data, cached_coordinates_file)

    if location_data is not False:
        print "Getting probes for location: %s" % location
        # Get the probes in this location
        available_probes = atlas_api.select_probes_in_location(
            location_data["lat"],
            location_data["lng"],
            location_data["country"],
            40
        )
        gmap_locations = set()
        if len(available_probes) > 0:
            gmap_location = "%s|%s" % (location_data["city"], location_data["country"])

            for probe_object in available_probes:

                # Check if we have obtained the location for the probe coordinates previously ...
                probe_coordinates = "%s,%s" % (probe_object.lat, probe_object.lng)
                if probe_coordinates in cached_probes_locations:
                    print "Using cached location for probe %s" % (probe_coordinates)
                    if cached_probes_locations[probe_coordinates]["admn_lvl_2"] != "False":
                        gmap_location = "%s|%s" % (
                            cached_probes_locations[probe_coordinates]["admn_lvl_2"],
                            cached_probes_locations[probe_coordinates]["country"]
                        )
                    elif cached_probes_locations[probe_coordinates]["locality"] != "False":
                        gmap_location = "%s|%s" % (
                            cached_probes_locations[probe_coordinates]["locality"],
                            cached_probes_locations[probe_coordinates]["country"]
                        )
                # ... otherwise query the Google Maps API for the address of the probe coordinates
                else:
                    reverse_location = query_coordinates_location(probe_object.lat, probe_object.lng)
                    # write the reverse location in the probes_locations file
                    write_coordinates_location(probe_object.lat, probe_object.lng, reverse_location, cached_probes_locations_file)
                    cached_probes_locations[probe_coordinates] = {
                        "locality": reverse_location["locality"],
                        "admn_lvl_2": reverse_location["admn_lvl_2"],
                        "country": reverse_location["country"]
                    }

                    # If the Google Maps API returns a location at the administrative_level_2 use this as city name ...
                    if reverse_location["admn_lvl_2"] is not False and reverse_location["country"] is not False:
                        gmap_location = "%s|%s" % (reverse_location["admn_lvl_2"], reverse_location["country"])
                    # ... otherwise use the locality (more specific than administrative_level_2,
                    # will lead to more probes selected)
                    elif reverse_location["locality"] is not False and reverse_location["country"] is not False:
                        gmap_location = "%s|%s" % (reverse_location["locality"], reverse_location["country"])

                gmap_locations.add(gmap_location)
                gmap_location = location
                if gmap_location not in candidate_probes:
                    candidate_probes[gmap_location] = set()

                candidate_probes[gmap_location].add(probe_object.id)
                probe_location[probe_object.id] = gmap_location
        else:
            print "Warning: No available probes in the location %s %s: " % (location_data["city"], location_data["country"])
    else:
        print "Warning: Could not find the coordinates for %s: " % location

# Get the probes in the target ASN
for probe_object in atlas_api.select_probes_in_asn(target_asn):
    target_asn_probes.add(probe_object.id)
    reverse_location = geolocator.reverse("%s, %s" % (probe_object.lat, probe_object.lng), language='en')
    #query_coordinates_location(probe_object.lat, probe_object.lng)
    probe_location[probe_object.id] = reverse_location.address

selected_probes = set()
location_rtt = dict()
for location in candidate_probes:
    if probes_num > len(candidate_probes[location]):
        selected_probes |= set(candidate_probes[location])
    else:
        selected_probes |= set(random.sample(candidate_probes[location], probes_num))
    print location, len(selected_probes), probes_num
selected_probes |= target_asn_probes
print "Number of candidate probes: %s" % len(selected_probes)
print "Number of candidate cities: %s" % len(candidate_probes.keys())
print "Number of candidate cities inferred from Google Maps: %s" % len(gmap_locations)
print "Number of probes in the target ASN: %s" % len(target_asn_probes)

if len(selected_probes) > 0:
    af = ip_version
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
        print target_ip, closest_probe, probe_location[closest_probe], prv_min_rtt
    else:
        print "Error: Couldn't converge to a target. Possibly incomplete presence data"
        print "The closest probe for %s is %s in %s with RTT %s", (target_ip, closest_probe, probe_location[closest_probe], prv_min_rtt)

else:
    print "Error: couldn't find any Atlas probe in the requested locations"
