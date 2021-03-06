# coding=latin-1
import sys, random
from datetime import datetime
from time import time
import numpy as np
import pyasn
import logging
import bz2
import argparse
# My modules
import ConfigParser
import PeeringDB
from Atlas import Atlas
from GeoEncoder import GeoEncoder
import arg_parser


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


def find_neighboring_probes(candidate_probes, target_asn, as_relationships):
    """
    Finds the probes in ASes with a visible interdomain link with the AS that owns the target IP address
    :param candidate_probes: a list of Atlas.Probe objects
    :param target_asn: the ASN for which we want to find probes in neighboring ASes
    :param as_relationships: dictionary with the mapping between AS links and AS relationship types
    :return: a list of probes
    """
    neighboring_probes = set()

    for probe in candidate_probes:
        link_to_test = "%s %s" % (probe.asn, target_asn)
        if link_to_test in as_relationships:
            neighboring_probes.add(probe.id)

    return neighboring_probes


def slice_selected_probes(selected_probes, n):
    """
    Slices the list of selected probes to chunks of 50s to avoid using
    :param selected_probes: the entire set of selected probes to slice
    :param n: the size of each chunk
    :return: the list of chunks
    """
    n = max(1, n)
    return (selected_probes[i:i+n] for i in xrange(0, len(selected_probes), n))

'''
Step 1: Initialization
'''
logger = logging.getLogger("Main")
logger.setLevel(logging.INFO)
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
ixp_lan_addresses = peeringdb_api.get_ixp_ips()

atlas_api = Atlas(ATLAS_API_KEY)

target_ips, asndb, as_relationships, extra_locations, already_geolocated_ips, output_file = arg_parser.read_user_arguments()

# Group the geo-location targets per ASN
geolocation_targets = dict()
asn_locations = dict()

siblings = {
    #16625: 20940,
    702: 701
}

counter = 0

print "Querying Maxmind for IP locations"
maxmind_locations = geo_encoder.query_maxmind_batch(target_ips ^ already_geolocated_ips)

for target_ip in target_ips:
    counter += 1
    if target_ip in already_geolocated_ips:
        logger.info("Skipping IP %s because it is already geolocated." % target_ip)
    else:
        # First check if the IP belongs to an IXP
        if target_ip in ixp_lan_addresses:
            target_asn = ixp_lan_addresses[target_ip].asn
        else:
            target_asn, prefix = asndb.lookup(target_ip)
            original_asn = target_asn
            if target_asn in siblings:
                target_asn = siblings[target_asn]

        if target_asn not in geolocation_targets:
            geolocation_targets[target_asn] = set()
            asn_locations[target_asn] = set()
        geolocation_targets[target_asn].add(target_ip)

        # Add the location provided by MaxMind in the list of possible locations in which we should ping
        if target_ip in maxmind_locations:
            asn_locations[target_asn].add(maxmind_locations[target_ip])


print "Collect the active Atlas probes per ASN and per country"
atlas_api.collect_active_probes()

candidate_probes = dict()
probes_facility = dict()
probe_objects = dict()
for target_asn in geolocation_targets:
    '''
    Step 2: Get the candidate AS locations based on presence information at IXPs and Facilities
    '''
    print("Getting the locations of AS%s" % target_asn)

    asn_locations[target_asn] |= peeringdb_api.get_asn_locations(target_asn).locations
    if target_asn in extra_locations:
        asn_locations[target_asn] |= extra_locations[target_asn]

    #print "Possible candidate locations: "
    #for location in asn_locations[target_asn]:
    #    print location

    '''
    Step 3: Get the available Atlas probes in the candidate locations
    '''
    target_asn_probes = set()
    available_locations = set()
    for location in asn_locations[target_asn]:
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
            gmap_location = "%s|%s" % (location_data["city"], location_data["country"])
            # If we have found the probes in this location in a previous iteration don't search again
            if gmap_location not in candidate_probes:
                print "Getting probes for location: %s" % gmap_location
                # Get the probes in this location
                if gmap_location in atlas_api.city_probes:
                    available_probes = atlas_api.city_probes[gmap_location]
                else:
                    available_probes = atlas_api.select_probes_in_location(
                        location_data["lat"],
                        location_data["lng"],
                        location_data["country"],
                        40
                    )
                    atlas_api.city_probes[gmap_location] = available_probes

                if len(available_probes) > 0:
                    available_locations.add(gmap_location)
                    if gmap_location not in candidate_probes:
                        candidate_probes[gmap_location] = set()
                    for probe_object in available_probes:
                        candidate_probes[gmap_location].add(probe_object.id)
                        probes_facility[probe_object.id] = gmap_location
                        probe_objects[probe_object.id] = probe_object
                else:
                    print "Warning: No available probes in the location: %s %s" % (
                    location_data["city"], location_data["country"])
            else:
                available_locations.add(gmap_location)
        else:
            print "Warning: Could not find the coordinates for: %s" % location

    # Get the probes in the target ASN
    if target_asn in atlas_api.asn_probes:
        for probe_object in atlas_api.asn_probes[target_asn]:
            target_asn_probes.add(probe_object.id)
            probe_objects[probe_object.id] = probe_object

    # Get the probes in ASes that are neighboring to the target ASN
    neighboring_probes = find_neighboring_probes(probe_objects.values(), target_asn, as_relationships)
    #print "Number of probes in neighboring ASes: ", len(neighboring_probes)


    for target_ip in  geolocation_targets[target_asn]:

        logger.info("Running geolocation for IP %s in AS%s" % (target_ip, target_asn))
        # TODO Order countries by number of presences to find the main country from which we start the measurements

        '''
        Step 4: Sample the available Atlas probes in the candidate cities to meet the querying budget restrictions
        This step is repeated for every IP address even if it's under the same AS to minimize artifacts caused by
        biases in the sampling process
        '''
        location_rtt = dict()
        selected_probes = set()
        for location in available_locations:
            # Start the probe selection by getting probes in neighboring ASes

            selected_neighboring_asns = set()
            selected_neighboring_probes = set()
            for probe_id in candidate_probes[location]:
                if probe_id in neighboring_probes:
                    probe_asn = probe_objects[probe_id].asn
                    if probe_asn not in selected_neighboring_asns:
                        selected_neighboring_probes.add(probe_id)
                        selected_neighboring_asns.add(probe_asn)
                    if len(selected_neighboring_probes) >= probes_num:
                        break

            selected_probes |= selected_neighboring_probes

            # If we need more probes sample randomly
            if len(selected_neighboring_probes) < probes_num:
                if (probes_num - len(selected_neighboring_probes)) > len(candidate_probes[location]):
                    selected_probes |= set(candidate_probes[location])
                else:
                    # consider only probes not already selected
                    remaining_probes = [p for p in candidate_probes[location] if p not in selected_probes]
                    # pick probes in as many ASes as possible
                    candidate_probe_asns = dict()
                    for p in remaining_probes:
                        p_asn = probe_objects[p].asn
                        if p_asn not in candidate_probe_asns:
                            candidate_probe_asns[p_asn] = list()
                        candidate_probe_asns[p_asn].append(p)

                    candidate_asns = dict()
                    selected_probes |= set(random.sample(candidate_probes[location], (probes_num - len(selected_neighboring_probes))))
        selected_probes |= target_asn_probes
        print "Total number of selected probes: %s" % len(selected_probes)
        '''
        Step 5: Run the RTT-based geolocation
        '''
        chunk_size = 100 # TODO put chunk size in configuration file
        if len(selected_probes) > 0:
            prv_min_rtt = sys.maxint
            closest_probe = 0
            probes_slices = slice_selected_probes(list(sorted(selected_probes)), chunk_size)
            for index, probes_slice in enumerate(probes_slices):
                print "Querying probes %s - %s" % (1*(index+1), chunk_size*(index+1))
                af = ip_version
                description="Presence-informed RTT geolocation"

                ping_results = atlas_api.ping_measurement(af, target_ip, description, packets_num, probes_slice)

                for probe_id in ping_results:
                    probe_min_rtt = min(ping_results[probe_id])
                    if probe_min_rtt < prv_min_rtt:
                        prv_min_rtt = probe_min_rtt
                        closest_probe = probe_id
                # If we found a probe with very low RTT we don't need to run all the pings
                if prv_min_rtt < 2:
                    break

            if closest_probe == 0:
                logger.error(
                    "The destination IP %s was unreachable from every probe." % target_ip
                )
            else:
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

                nearest_facility_city = "False"
                if closest_probe in probes_facility:
                    nearest_facility_city = probes_facility[closest_probe].split("|")[0]
                output_line = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"

                if prv_min_rtt < 5:
                    print "Target [%s,%s] | Closest Probe [%s,%s, %s] | Closest Facility [%s] | Min. RTT [%s] " % \
                          (target_ip, original_asn, closest_probe, probe_location, probe_coordinates,
                           nearest_facility_city, prv_min_rtt)
                else:
                    logger.warning(
                        "Couldn't converge to a target for IP %s. Possibly incomplete presence data." % target_ip)
                    logger.info("The closest probe for [%s,%s] is %s in %s with RTT %s" %
                                (target_ip, original_asn, closest_probe, probe_location, prv_min_rtt))
                    output_line = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s # Too high minimum RTT\n"

                # Save all result, even those above the RTT threshold. Since RTT is part of the output
                # it can be used to decide if geolocation was successful or not

                # Write output to file
                current_timestamp = int(time())
                current_datetime = datetime.utcfromtimestamp(current_timestamp)
                with open(output_file, "a+") as fout:
                    fout.write(output_line %
                        (target_ip,                                                # Column 1: IP address
                         target_asn,                                               # Column 2: ASN
                         cached_probes_locations[probe_coordinates]["locality"],   # Column 3: City name of closest probe
                         cached_probes_locations[probe_coordinates]["admn_lvl_2"], # Column 4: Administrative area of closest probe
                         cached_probes_locations[probe_coordinates]["country"],    # Column 5: Country ISO code of closest probe
                         probe_objects[closest_probe].lat,                         # Column 6: Latitude of the closest probe
                         probe_objects[closest_probe].lng,                         # Column 7: Longitude of the closest probe
                         prv_min_rtt,                                              # Column 8: Measured minimum RTT
                         nearest_facility_city,                                    # Column 9: City of nearest facility
                         current_timestamp,                                        # Column 10: Current timestamp
                         current_datetime                                          # Column 11: Current datetime (added to facilitate readability)
                         ) )
                    fout.flush()
                    fout.close()

        else:
            print "Error: couldn't find any Atlas probe in the requested locations"
