"""
utilities for processing geo data. 


Created on Jul 12, 2013
@author: zul110
"""

import math
import urllib
from googlegeocoder import GoogleGeocoder
GOOGLE_MAP_API_PREFIX = 'http://maps.googleapis.com/maps/api/geocode/json?address='
GOOGLE_MAP_API_POSTFIX = '&sensor=false'
import json


def get_geocode_by_parsing(address):
    lat = None
    lng = None
    address = address.replace(' ', '+')
    try:
        api = GOOGLE_MAP_API_PREFIX + address + GOOGLE_MAP_API_POSTFIX
        googleResponse = urllib.urlopen(api)
        jsonResponse = json.loads(googleResponse.read())
        test = json.dumps([s['geometry']['location'] for s in jsonResponse['results']], indent=3)
        tlist = eval(test)
        if len(tlist) > 0:
            lat = tlist[0]['lat']
            lng = tlist[0]['lng']
    except:
        pass

    return lat, lng


def get_geo_code(address):
    lat = None
    lng = None
    formatted_address = None
    zip = None

    try:
        geocoder = GoogleGeocoder()
        search = geocoder.get(address)

        if len(search) > 0:
            loc = search[0].geometry.location
            lat = loc.lat
            lng = loc.lng

            formatted_address = search[0].formatted_address

            components = search[0].address_components
            for comp in components:
                if u'postal_code' in comp.types:
                    zip = comp.long_name
    except:
        pass

    return lat, lng, formatted_address, zip


def distance_on_unit_sphere(lat1, long1, lat2, long2):
    """
    REFERENCE: http://www.johndcook.com/python_longitude_latitude.html
    """
    # Convert latitude and longitude to 
    # spherical coordinates in radians.
    degrees_to_radians = math.pi / 180.0

    # phi = 90 - latitude
    phi1 = (90.0 - lat1)*degrees_to_radians
    phi2 = (90.0 - lat2)*degrees_to_radians

    # theta = longitude
    theta1 = long1*degrees_to_radians
    theta2 = long2*degrees_to_radians

    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) + 
           math.cos(phi1)*math.cos(phi2))
    if (cos > 1.0): cos = 1.0
    arc = math.acos( cos )

    # To get the distance in miles, multiply by 3960. To get the distance in kilometers, multiply by 6373
    return arc


def test_distance():
    lat1 = float(40.765982)
    long1 = float(-74.002258)
    lat2 = float(38.809751)
    long2 = float(-77.174561)
    print distance_on_unit_sphere(lat1, long1, lat2, long2)


def test_get_geocode():
    fulladdress = "san jose, CA, USA"
    lat, lng, formatted_address, zip = get_geo_code(fulladdress)
    print "address for san jose, CA, USA : "
    print lat, lng, formatted_address, zip

    fulladdress = "new york city, USA"
    lat, lng, formatted_address, zip = get_geo_code(fulladdress)
    print "address for new york city, USA : "
    print lat, lng, formatted_address, zip


if __name__ == '__main__':
    test_distance()
    test_get_geocode()
