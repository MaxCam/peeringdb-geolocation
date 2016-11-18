import logging
import gzip
import sys
import geopy
import geoip2.database
from maxminddb.errors import InvalidDatabaseError


class GeoEncoder(object):
    """
    This class offers geolocation functionality from different APIs and databases
    """

    def __init__(self, gmap_api_key, maxmind_db_file, coordinates_file, probes_locations_file, worldcities_pop):
        logging.basicConfig()
        self.logger = logging.getLogger("GeoEncoder")
        self.maxmind_reader = False
        self.GMAP_API_KEY = gmap_api_key

        try:
            self.maxmind_reader = geoip2.database.Reader(maxmind_db_file)
        except (IOError, InvalidDatabaseError) as e:
            self.logger.error("Reading Maxmind DB filed failed with error: %s" % str(e))

        self.worldcities_pop = worldcities_pop
        self.coordinates_file = coordinates_file
        self.probes_locations_file = probes_locations_file
        # Create the Google Maps API geolocator
        self.gmap_geolocator = geopy.geocoders.GoogleV3(api_key=self.GMAP_API_KEY)

    def write_location_coordinates(self, location_id, location_data):
        """
        Append the data provided by the Google Maps API for a specific PeeringDB location to the corresponding file
        :param location_id: The location id, in the format of city|country_2-letter_iso_code
        :param location_data: The dictionary with the Google Maps data on the location indicated by :location_id
        :return: the success status of appending to file (true or false)
        """
        success = True
        try:
            with open(self.coordinates_file, "a+") as fout:
                outline = u'%s\t%s\t%s\t%s\t%s\n' % (
                    location_id,
                    location_data["lat"],
                    location_data["lng"],
                    location_data["city"],
                    location_data["country"]
                )

                fout.write(outline.encode('utf-8'))

        except (IOError, UnicodeEncodeError) as e:
            self.logger.error("Appending to file `%s` failed with error: %s" % (self.coordinates_file, str(e)))
            success = False
        return success

    def read_location_coordinates(self):
        """
        Read the coordinates, city name and country iso code according to Google maps for PeeringDB locations
        that have been encountered in past geolocations
        :return: a dictionary that maps PeeringDB locations to the stored data obtained through the Google Maps API
        """
        location_coordinates = dict()
        try:
            with open(self.coordinates_file) as fin:
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
            self.logger.error("Could not read file `%s`" % self.coordinates_file)
        except IndexError:
            self.logger.error("The coordinates file `%s` is malformatted" % self.coordinates_file)
        return location_coordinates

    def write_coordinates_location(self, lat, lng, coorindates_data):
        """
        Append the data provided from the Google Maps API for a latitude and longitude in the correspoding file
        :param lat: the latitude of the location
        :param lng: the longitude of the location
        :param coorindates_data: the data to append (city name, country iso code) for the given latitude and longitude
        :return: the status of appending to the file (true or false)
        """
        success = True
        try:
            with open(self.probes_locations_file, "a+") as fout:
                outline = u'%s\t%s\t%s\t%s\t%s\n' % (
                    lat,
                    lng,
                    coorindates_data["locality"],
                    coorindates_data["admn_lvl_2"],
                    coorindates_data["country"]
                )

                fout.write(outline.encode('utf-8'))

        except (IOError, UnicodeEncodeError) as e:
            self.logger.error("Appending to file `%s` failed with error: %s" % (self.probes_locations_file, str(e)))
            success = False

        return success

    def read_coordinates_location(self):
        """
        Read the city name and country iso code according to Google maps for probes coordinates that have been
        encountered in past geolocations
        :return: a dictionary that maps the coordinates to the corresponding data
        """
        probes_locations = dict()
        try:
            with open(self.probes_locations_file) as fin:
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
            self.logger.error("Could not read file `%s`" % self.probes_locations_file)
        except IndexError:
            self.logger.error("The probes' locations file `%s` is malformatted" % self.probes_locations_file)

        return probes_locations

    def query_location_coordinates(self, target_location):
        """
        Queries the Google Maps API for the coordinates for the target location
        :param target_location:
        :return: a dictionary with the latitude, longitude, city name and country code according to Google Maps API
        """
        city_coordinates = dict()
        try:
            location = self.gmap_geolocator.geocode(target_location, timeout=30)
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
        except geopy.exc.GeocoderQueryError, e:
            self.logger.critical("The Google Maps API request was denied with message %s\n"
                                 "Make sure you have provided the correct API key in the config/config.ini file." %
                                 str(e))
            sys.exit(-1)

        if len(city_coordinates) == 4:
            return city_coordinates
        else:
            return False

    def query_coordinates_location(self, lat, lng):
        """
        Queries the Google Maps API the location of a set of coordinates and returns the city name and country iso code
        :param lat: The latitude of the location
        :param lng: The longitude of the location
        :return: a dictionary with the city name and the country iso code
        """
        reverse_location = self.gmap_geolocator.reverse("%s, %s" % (lat, lng), exactly_one=True, language='en')
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

    def query_maxmind_location(self, target_ip):
        """
        Returns the location of an IP address (city name and country 2-letter ISO code) based on Maxmind's Ddtabase
        :param target_ip: the IP to geolocate
        :return: a string with the location of the IP
        """
        maxmind_location = False
        if self.maxmind_reader is not False:
            response = self.maxmind_reader.city(target_ip)
            if response.country is not None:
                maxmind_city = response.city.name
                if maxmind_city is not None:
                    maxmind_city = maxmind_city.lower()
                maxmind_country = response.country.iso_code

                # if maxmind indicates a country but the city is 'none', find the city with the largest population in that country
                if str(maxmind_city) == "None" and str(maxmind_country) != "None":
                    maxmind_country = maxmind_country.lower()
                    try:
                        with gzip.open(self.worldcities_pop) as fin:
                            maxmind_city = None
                            largest_city_pop = 0
                            for line in fin:
                                lf = line.strip().split(",")
                                if len(lf) > 0 and maxmind_country == lf[0]:
                                    try:
                                        if int(lf[4]) > largest_city_pop:
                                            largest_city_pop = int(lf[4])
                                            maxmind_city = lf[1].lower()
                                    except ValueError:
                                        # If we have no data about the city's population simly move on to next city
                                        continue
                    except IOError, e:
                        self.logger.error("Could not read file `%s`. %s" % (self.probes_locations_file, str(e)))

                if str(maxmind_city) != "None" and str(maxmind_country) != "None":
                    maxmind_location = "%s|%s" % (maxmind_city, maxmind_country)

        return maxmind_location