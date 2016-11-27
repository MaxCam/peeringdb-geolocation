import logging
import argparse
import socket
import sys
import os.path
import bz2
import pyasn


def read_presence_data(presence_file):
    """
    Reads the presence data provided in the corresponding file
    :param presence_file: the value of the -p/--presence argument
    :return: a dictionary that maps ASNs to city|country locations
    """
    global logger
    extra_locations = dict()
    if not os.path.isfile(presence_file):
        logger.error("The file `%s` provided by the -p/--presence argument does not exist." % presence_file )
    else:
        try:
            with open(presence_file) as fin:
                for line in fin:
                    if not line.startswith("#"):
                        lf = line.strip().split()
                        if len(lf) > 2: # expected format: ASN<tab>City<tab>Country [possible comment]
                            asn = int(lf[0])
                            if asn not in extra_locations:
                                extra_locations[asn] = set()
                            extra_locations[asn].add("%s|%s" % (lf[1], lf[2]))
        except IOError, e:
            logger.critical("Failed to read the file `%s` provided by the -p/--file presence. Error: %s" %
                            presence_file, str(e))
    return extra_locations


def is_valid_ipv4_address(address):
    """
    Checks if a string represents a valid IPv4 address
    :param address: the string to validate
    :return: True if the string is a valid IPv4 address, False otherwise
    """
    try:
        socket.inet_pton(socket.AF_INET, address)
    except AttributeError:  # no inet_pton here, sorry
        try:
            socket.inet_aton(address)
        except socket.error:
            return False
        return address.count('.') == 3
    except socket.error:  # not a valid address
        return False

    return True


def read_geolocation_targets(target_ip, target_file):
    """
    Reads and validates the input given as target for the geolocation (either a single IP or a file with IPs)
    :param target_ip: the value of the -i/--ip user argument
    :param target_file: the value of the -f/--file user argument
    :return: a list of valid IP addresses
    """
    global logger
    target_addresses = set()
    # Check that the user input is correct
    if target_ip is not None:
        if is_valid_ipv4_address(target_ip):
            target_addresses.add(target_ip)
        else:
            logger.critical("The provided -i/--ip argument `%s` is not a valid IPv4 address." % target_ip)
    elif target_file is not None:
        # First check that the file exists and then check if the address in the file are valid IPv4 addresses
        if not os.path.isfile(target_file):
            logger.critical("The file `%s` provided by the -f/--file argument does not exist." % target_file)
        else:
            try:
                with open(target_file) as fin:
                    line_counter = 0
                    for line in fin:
                        line_counter += 1
                        ip = line.strip()
                        if len(ip) > 0 and is_valid_ipv4_address(ip):
                            target_addresses.add(ip)
                        elif len(ip) > 0:
                            logger.warning("Skipping line %s in the `%s` file because "
                                           "it is not a valid IPv4 address." % (line_counter, target_file))
            except IOError, e:
                logger.critical("Failed to read the file `%s` provided by the -f/--file argument. Error: %s" %
                                target_file, str(e))

    return target_addresses


def read_as_relationships(relationships_file):
    """
    Reads and validates the AS relationships
    :param relationships_file: the value of the -r/--relations argument
    :return: a dictionary with the mapping between AS links and the corresponding relationship type
    """
    global logger
    as_relationships = dict()
    if not os.path.isfile(relationships_file):
        logger.error("The file `%s` provided by the -r/--relations argument does not exist." % relationships_file)
    else:
        try:
            relatioships_data = bz2.BZ2File(relationships_file)
            lines = relatioships_data.readlines()
            line_counter = 0
            for line in lines:
                line_counter += 1
                line = line.strip()
                if not line.startswith("#"):
                    lf = line.split("|")
                    if len(lf) == 4:
                        try:
                            as_link = "%s %s" % (lf[0], lf[1])
                            as_relationships[as_link] = int(lf[2])
                            reverse_as_link = "%s %s" % (lf[1], lf[0])
                            as_relationships[reverse_as_link] = int(lf[2]) * -1
                        except ValueError:
                            logger.warning("Skipping line %s in the `%s` file because "
                                           "it does not correspond to a valid AS relationship type." %
                                           (line_counter, relationships_file))
        except IOError, e:
            logger.error("Failed to read the file `%s` provided by the -r/--relations argument. Error: %s" %
                            relationships_file, str(e))

    # First check that the file exists
    return as_relationships


def validate_output_file(output_file):
    """
    Checks if the output file is writable, and if it exists reads IPs already geolocated in it
    :param output_file:
    :return:
    """
    already_geolocated = set()
    # Read the provided presence AS data
    if not os.access(os.path.dirname(output_file), os.W_OK):
        logging.critical("The programe does not have write permissions to the output file location `%s` "
                         "provided by the -o/--output argument. " % args.output)
        sys.exit(-1)
    elif os.path.isfile(output_file):
        try:
            with open(output_file) as fin:
                for line in fin:
                    if not line.startswith("#"):
                        lf = line.strip().split("\t")
                        if len(lf) > 0:
                            already_geolocated.add(lf[0])
        except IOError:
            pass

    return already_geolocated

def read_user_arguments():
    """
    Reads and validates the command-line arguments provided by the user
    :return: the parsed values of the command-line arguments
    """
    global logger
    # Initialize the argument parser
    description = 'Geo-locates border IP addresses based on latency measurements from RIPE Atlas'
    parser = argparse.ArgumentParser(description=description)
    # Add the permitted arguments
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-i', '--ip',
                        type=str,
                        help="A single IP address to geo-locate")

    group.add_argument('-f', '--file',
                        type=str,
                        help="The path to a file with IP address to geo-locate (one address per line)")

    parser.add_argument('-a', '--ipasn',
                        type=str,
                        required=True,
                        help="The path to a pyasn database file that maps IP prefixes to AS numbers")

    parser.add_argument('-r', '--relations',
                        type=str,
                        required=True,
                        help="The path to the file with the AS relationships in CAIDA format")
                        # the CAIDA serial-2 file with the AS relationships:
                        # http://data.caida.org/datasets/as-relationships/serial-2/

    parser.add_argument('-p', '--presence',
                        type=str,
                        required=False,
                        help="Path to file with AS presence data")
                        # Format ASN<tab>City<tab>Country

    parser.add_argument('-o', '--output',
                        type=str,
                        required=True,
                        help="The path to the file where the geolocation output will be written")

    args = parser.parse_args()

    # Read and validate the provided IP geolocation targets
    target_addresses = read_geolocation_targets(args.ip, args.file)
    if len(target_addresses) == 0:
        logger.critical("Program exits because no valid IP address was provided as geo-location target.")
        sys.exit(-1)

    as_relationships = read_as_relationships(args.relations)
    if len(as_relationships) == 0:
        logger.error("The provided AS relationships file is invalid. "
                     "The feature of probe selection based on AS relationships will be deactivated which may lead "
                     "to lower geo-location accuracy.")

    # Read the provided pyasn file
    try:
        asndb = pyasn.pyasn(args.ipasn)
    except IOError:
        logging.critical("Could not read the pyasn file `%s` provided by the -a/--ipasn argument. "
                         "Please enter the correct file location." % args.ipasn)
        sys.exit(-1)

    # Read the provided presence AS data
    presence_data = dict()
    if args.presence is not None:
        presence_data = read_presence_data(args.presence)

    already_geolocated_ips = validate_output_file(args.output)
    '''
    # Linux permits pretty much any character in the file name so the filename check bellow may be unnecessary.
    # So, I will leave it commented-out unless we experience probles related with filenaming conventions in
    # other operatin systems.
    else:
        import string
        valid_chars = "-_.()/+=: %s%s" % (string.ascii_letters, string.digits)
        valid_chars = frozenset(valid_chars)
        invalid_chars = {c for c in args.output if c not in valid_chars}
        if len(invalid_chars) > 0:
            logging.critical("The output filename `%s` provided by the -o/--output argument "
                             "contains invalid charachters: %s. " % (args.output, ','.join(invalid_chars)))
            sys.exit(-1)
    '''

    return target_addresses, asndb, as_relationships, presence_data, already_geolocated_ips, args.output

logging.basicConfig()
logger = logging.getLogger("ArgParser")