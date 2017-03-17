# coding=latin-1
import random, sys, logging, time, collections
from ujson import dumps, loads
from geopy import distance
from geopy import Point
from datetime import datetime
from ripe.atlas.cousteau import ProbeRequest
from ripe.atlas.cousteau import (
  Ping,
  AtlasCreateRequest,
  AtlasSource,
  AtlasStream,
  AtlasResultsRequest,
  AtlasRequest,
  MeasurementRequest
)
from geopy import geocoders
from ripe.atlas.cousteau.source import MalFormattedSource
from ripe.atlas.cousteau.exceptions import  APIResponseError
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()
reload(sys)
sys.setdefaultencoding('utf-8')

class Probe:

    def __init__(self, id, asn, lat, lng, country):
        self.id = id
        self.asn = asn
        self.lat = lat
        self.lng = lng
        self.country = country


class Atlas:

    def __init__(self, atlas_key):
        logging.basicConfig()
        self.logger = logging.getLogger("Atlas")
        self.ATLAS_API_KEY = atlas_key
        self.ping_rtts = dict()

    def on_result_response(self, *args):
        """
        Function that will be called every time we receive a new result.
        :param args: a tuple, so you should use args[0] to access the real message.
        """
        result = args[0]['result']
        for reply in result:
            if "rtt" in reply:
                rtt = reply["rtt"]
                if args[0]["prb_id"] not in self.ping_rtts:
                    self.ping_rtts[args[0]["prb_id"]] = list()
                self.ping_rtts[args[0]["prb_id"]].append(rtt)

    def parse_results(self, result):
        """
        Function that will be called every time we receive a new result.
        :param result: The result of the ping measurement encoded in JSON format
        """
        for reply in result:
            if "result" in reply:
                for packet in reply["result"]:
                    if "rtt" in packet:
                        rtt = packet["rtt"]
                        if reply["prb_id"] not in self.ping_rtts:
                            self.ping_rtts[reply["prb_id"]] = list()
                        self.ping_rtts[reply["prb_id"]].append(rtt)

    @staticmethod
    def select_probes_in_asn(target_asn):
        """
        Returns a set of Atlas probe IDs in the target ASN
        :param target_asn: the ASN in which the function searches for probes
        :return: a set of Atlas probe IDs
        """
        candidate_probes = set()
        filters = {"asn_v4": target_asn, "status": 1}
        probes = ProbeRequest(**filters)
        for probe in probes:
            if probe["geometry"] is not None:
                candidate_probes.add(
                    Probe(
                        probe["id"],
                        probe["asn_v4"],
                        probe["geometry"]["coordinates"][1],
                        probe["geometry"]["coordinates"][0],
                        probe["country_code"]
                    )
                )

        return candidate_probes


    def select_probes_in_location(self, lat, lng, country, radius):
        """
        Returns a set of Atlas probe IDs in the radius around the lat/long of the target city/country.
        :param lat: the latitude of the target location
        :param lng: the longitude of the target location
        :param country: the country of the target location (used as filter to the Atlas API request)
        :param radius: the maximum radius in km around the target city where the function searches for Atlas probes
        :return: a set of Atlas probe IDs
        """
        candidate_probes = set()

        filters = {"country_code": country, "status": 1}
        probes = ProbeRequest(**filters)

        try:
            for probe in probes:
                if probe["asn_v4"] is not None and probe["geometry"]["type"] == "Point":
                    probe_lon = probe["geometry"]["coordinates"][0]
                    probe_lat = probe["geometry"]["coordinates"][1]
                    p1 = Point("%s %s" % (lng, lat))
                    p2 = Point("%s %s" % (probe_lon, probe_lat))
                    result = distance.distance(p1, p2).kilometers
                    if result <= radius:
                        candidate_probes.add(
                            Probe(
                                probe["id"],
                                probe["asn_v4"],
                                probe["geometry"]["coordinates"][1],
                                probe["geometry"]["coordinates"][0],
                                probe["country_code"]
                            )
                        )
        except APIResponseError, e:
            self.logger.error("RIPE Atlas API request failed when requesting probes for coordinates: %s,%s" % (lat, lng))
        return candidate_probes

    def ping_measurement(self, af, target_ip, description, packets_num, probes_list):
        """
        Creates a new Ping measurement
        :param af: The IP address family (4 or 6)
        :param target_ip: The IP to be queried
        :param description: The description of the measurement
        :param packets_num:
        :param probes_list:
        :return:
        """
        self.ping_rtts = dict()
        ping = Ping(af=af, target=target_ip, description=description, packets=packets_num)

        if len(probes_list) > 0:
            source = AtlasSource(
                value=','.join(str(x) for x in probes_list),
                requested=len(probes_list),
                type="probes"
            )

            atlas_request = AtlasCreateRequest(
                start_time=datetime.utcnow(),
                key=self.ATLAS_API_KEY,
                measurements=[ping],
                sources=[source],
                is_oneoff=True
            )

            try:
                (is_success, response) = atlas_request.create()

                #print response, len(','.join(str(x) for x in probes_list))
                #print response
                # Example of error response:
                # {u'error': {u'status': 400, u'code': 104, u'detail': u'value: Ensure this value has at most 8192 characters (it has 11948).', u'title': u'Bad Request'}}
                if "error" in response:
                    self.logger.critical("The RIPE Atlas measurement failed due to`%s` error with message: \"%s\"."
                                            % (response["error"]["title"], response["error"]["detail"]))
                    sys.exit(-1)
                else:
                    measurement_id = response["measurements"][0]
                    url_path = "/api/v2/measurements/%s/" % measurement_id

                    measurement_is_active = True
                    minutes_passed = 0
                    while measurement_is_active is True:
                        time.sleep(60)
                        minutes_passed += 1
                        request = AtlasRequest(**{"url_path": url_path})
                        result = collections.namedtuple('Result', 'success response')
                        (is_success, response) = request.get()
                        if not is_success:
                            self.logger.error("Unsuccessful API request for measurement ID %s", measurement_id)
                            break

                        status = response["status"]["name"]
                        if status == "Stopped" or minutes_passed > 3:
                            measurement_is_active = Fasle
                            kwargs = {
                                "msm_id": measurement_id
                            }

                            is_success, results = AtlasResultsRequest(**kwargs).create()

                            if is_success:
                                self.parse_results(results)


            except MalFormattedSource, e:
                self.logger.critical("Unable to create RIPE Atlas measurement. Error: %s" % str(e))
                sys.exit(-1)
            except KeyError:
                self.logger.critical("The RIPE Atlas API returned a malformatted measurement reply.")
                sys.exit(-1)

        return self.ping_rtts
