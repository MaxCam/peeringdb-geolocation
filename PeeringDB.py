import logging
import requests


class AutSys(object):
    def __init__(self, asn, ixps, facilities, locations):
        self.asn = asn
        self.ixps = ixps
        self.facilities = facilities
        self.locations = locations


class API(object):

    def __init__(self):
        logging.basicConfig()
        self.logger = logging.getLogger("PeeringDB")

    def get_asn_locations(self, target_asn):
        """
        Retrieves the presence information in terms of IXPs, Facilities and Cities for an ASN
        :param target_asn: the target ASN
        :return: an AutSys object
        """

        # Get the IXPs where the ASN is present
        ixp_presences = self.get_asn_ixps(target_asn)
        # Get the facilities where the ASN is present
        facility_presences, facility_locations = self.get_asn_facilities(target_asn)
        # Get the locations of the IXPs where the ASN is present
        ixp_locations = set()
        for ixp_id in ixp_presences:
            ixp_locations |= self.get_ixp_locations(ixp_id)

        asn_locations = facility_locations | ixp_locations

        #TODO Get the IXP neighbors of the ASN

        return AutSys(target_asn, ixp_presences, facility_presences, asn_locations)

    def get_asn_ixps(self, asn):
        """
        Get the IXPs where an ASN is present
        :param asn: The requested ASN
        :return: The set of IXP IDs
        """
        endpoint = "netixlan?asn=%s" % asn
        netixlan_info = self.get_request(endpoint)
        ixp_presences = set()
        for ixlan in netixlan_info["data"]:
            ixp_presences.add(ixlan["ix_id"])

        return ixp_presences

    def get_asn_facilities(self, asn):
        """
        Get the facilities where the ASN is present, and the corresponding locations
        :param asn: The requested ASN
        :return: The set of facility IDs, and the set of locations where these facilities are present
        """
        endpoint = "netfac?local_asn=%s" % asn
        netfac_info = self.get_request(endpoint)
        facility_presences = set()
        facility_locations = set() # Also get the cities where the facilities are in
        for netfac in netfac_info["data"]:
            facility_presences.add(netfac["fac_id"])
            location = ("%s|%s" % (netfac["city"], netfac["country"])).lower()
            facility_locations.add(location)

        return facility_presences, facility_locations

    def get_ixp_locations(self, ixp_id):
        """
        Returns the locations where an IXP has presence
        :param ixp_id: The PeeringDB ID of the IXP
        :return: the set of locations where the IXP or its facilities are present
        """
        endpoint = "ix/%s" % ixp_id
        ixp_info = self.get_request(endpoint)
        ixp_locations = set()
        ixp_city = ixp_info["data"][0]["city"]
        ixp_country = ixp_info["data"][0]["country"]
        ixp_location = ("%s|%s" % (ixp_city, ixp_country)).lower()
        ixp_locations.add(ixp_location)
        # Get the locations of the IXP's facilities
        for fac in ixp_info["data"][0]["fac_set"]:
            fac_location = ("%s|%s" % (fac["city"], fac["country"])).lower()
            ixp_locations.add(fac_location)

        return ixp_locations

    def get_request(self, endpoint):
        """
        Sends a GET HTTP request to the PeeringDB RESTful API
        :param endpoint: the API endpoint that will receive the GET request
        :return: The API response in JSON format, or False if the request failed
        """
        base_url = "https://peeringdb.com/api/"
        query = base_url + endpoint
        try:
            response = requests.get(query)
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error("GET request to %s failed with error %s", endpoint, str(e))
            return False

