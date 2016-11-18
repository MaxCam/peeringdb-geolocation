# coding=latin-1
import sys, random
from datetime import datetime
import numpy as np
import pyasn
import logging
# My classes
import ConfigParser
import PeeringDB
from Atlas import Atlas
from GeoEncoder import GeoEncoder

target_ip = sys.argv[1]
ipasn_file = sys.argv[2]

# Map the target IP to the corresponding ASN
try:
    asndb = pyasn.pyasn(ipasn_file)
    target_asn, prefix = asndb.lookup(target_ip)
except IOError:
    logging.critical("Could not read the pyasn file `%s`. "
                     "Please enter the correct file location." % ipasn_file)
    sys.exit(-1)


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


# Read the configuration parameters
config = read_config()
probes_num = int(config["PingParameters"]["probes_per_city"])
packets_num = int(config["PingParameters"]["packets_number"])
ip_version = int(config["PingParameters"]["ip_version"])
ATLAS_API_KEY = config["ApiKeys"]["atlas_key"]
GMAP_API_KEY = config["ApiKeys"]["gmap_key"]
maxmind_db_file = config["FilePaths"]["maxmind_db"]
worldcities_pop = config["FilePaths"]["worldcities_population"]
cached_coordinates_file = config["FilePaths"]["city_coordinates"]
cached_probes_locations_file = config["FilePaths"]["probes_locations"]

geo_encoder = GeoEncoder(GMAP_API_KEY, maxmind_db_file, cached_coordinates_file, cached_probes_locations_file, worldcities_pop)
# Read the coordinates for locations that have been encountered in past runs
cached_location_coordinates = geo_encoder.read_location_coordinates()
cached_probes_locations = geo_encoder.read_coordinates_location()

peeringdb_api = PeeringDB.API()
asn_location = peeringdb_api.get_asn_locations(target_asn).locations

# Add the location provided by MaxMind in the list of possible locations in which we should ping
maxmind_location = geo_encoder.query_maxmind_location(target_ip)
if maxmind_location is not False:
    asn_location.add(maxmind_location)

print "Possible locations:"
for location in asn_location:
    print location

#TODO Order countries by number of presences to find the main country from which we start the measurements

atlas_api = Atlas(ATLAS_API_KEY)
target_asn_probes = set()
candidate_probes = dict()
probe_objects = dict()

for location in asn_location:
    location = location.lower()
    # Get the coordinates for this location
    if location in cached_location_coordinates:
        # if we have found the coordinates for this location before read it from the cached coordinates file ...
        location_data = cached_location_coordinates[location]
    else:
        # ... otherwise query the Google Maps API for the coordinates ...
        location_data = geo_encoder.query_location_coordinates(location)
        # ... and store the coordinates in the corresponding file
        if location_data is not False:
            geo_encoder.write_location_coordinates(location, location_data)

    if location_data is not False:
        print "Getting probes for location: %s" % location
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
            for probe_object in available_probes:
                candidate_probes[gmap_location].add(probe_object.id)
                probe_objects[probe_object.id] = probe_object
        else:
            print "Warning: No available probes in the location %s %s: " % (location_data["city"], location_data["country"])
    else:
        print "Warning: Could not find the coordinates for %s: " % location

# Get the probes in the target ASN
for probe_object in atlas_api.select_probes_in_asn(target_asn):
    target_asn_probes.add(probe_object.id)
    probe_objects[probe_object.id] = probe_object

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

    # Get the location of the closes probe
    # Check if we have obtained the location for the probe coordinates previously ...
    probe_coordinates = "%s,%s" % (probe_objects[closest_probe].lat, probe_objects[closest_probe].lng)
    if not probe_coordinates in cached_probes_locations:
        reverse_location = geo_encoder.query_coordinates_location(probe_objects[closest_probe].lat, probe_objects[closest_probe].lng)
        # write the reverse location in the probes_locations file
        geo_encoder.write_coordinates_location(probe_objects[closest_probe].lat, probe_objects[closest_probe].lng, reverse_location)
        cached_probes_locations[probe_coordinates] = {
            "locality": reverse_location["locality"],
            "admn_lvl_2": reverse_location["admn_lvl_2"],
            "country": reverse_location["country"]
        }

    probe_location = "%s|%s|%s" % (
        cached_probes_locations[probe_coordinates]["locality"],
        cached_probes_locations[probe_coordinates]["admn_lvl_2"],
        cached_probes_locations[probe_coordinates]["country"]
    )


    if prv_min_rtt < 10:
        print target_ip, closest_probe, probe_location, probe_coordinates, prv_min_rtt
    else:
        print "Error: Couldn't converge to a target. Possibly incomplete presence data"
        print "The closest probe for %s is %s in %s with RTT %s", (target_ip, closest_probe, probe_location, probe_coordinates, prv_min_rtt)

else:
    print "Error: couldn't find any Atlas probe in the requested locations"
