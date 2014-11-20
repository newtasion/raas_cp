"""
procedures for computing similarities for fields. 

Created on Jul 05, 2013
@author: zul110
"""

from match_engine.common import geo_utils, date_utils, utils
from utils import text_utils
from sets import Set

CONSINE = 'CONSINE'
SAME = 'SAME'
SUBSTRACT = 'SUBSTRACT'
DATE_OVERLAPPING = 'DATE_OVERLAPPING'
DATE_DIFFERENCE = 'DATE_DIFFERENCE'
LIST_OVERLAPPING = 'LIST_OVERLAPPING'
GEO_DISTANCE = 'GEO_DISTANCE'

SimilarityFunctions = [CONSINE, SAME, SUBSTRACT, DATE_OVERLAPPING, DATE_DIFFERENCE, LIST_OVERLAPPING, GEO_DISTANCE]
NORM_SUBSTRACT = 1000   # need to do stat analysis


class FieldSimilarity:
    @staticmethod
    def get_similarity(tvfield1, tvfield2, method):
        """
        given two vectors, get the similarity score.
        example of term vector: 
        {'termVector': {'math': 1, 'physical': 2}, 'name': 'summary'}
        """
        if method in ['cosine', 'COSINE']:
            featureValue = \
            FieldSimilarity.get_similarity_consine(tvfield1, tvfield2)

        elif method in ['substract', 'SUBSTRACT']:
            featureValue = \
            FieldSimilarity.get_similarity_substract(tvfield1, tvfield2)

        elif method in ['date_overlapping', 'DATE_OVERLAPPING']:
            featureValue = \
            FieldSimilarity.get_similarity_date_overlapping(tvfield1, tvfield2)

        elif method in ['date_difference', 'DATE_DIFFERENCE']:
            featureValue = \
            FieldSimilarity.get_similarity_date_difference(tvfield1,tvfield2)

        elif method in ['same', 'SAME']:
            featureValue = FieldSimilarity.get_similarity_same(tvfield1, tvfield2)

        elif method in ['list_overlapping_bool', 'LIST_OVERLAPPING_BOOL',
                        'list_overlapping', 'LIST_OVERLAPPING']:
            featureValue = \
            FieldSimilarity.get_similarity_list_overlapping_bool(tvfield1, tvfield2)

        elif method in ['list_overlapping_size', 'LIST_OVERLAPPING_SIZE']:
            featureValue = \
            FieldSimilarity.get_similarity_list_overlapping_size(tvfield1, tvfield2)

        elif method in ['distance', 'DISTANCE']:
            featureValue = \
            FieldSimilarity.get_similarity_distance(tvfield1, tvfield2)

        else:
            featureValue = 0.0

        return featureValue

    @staticmethod
    def get_similarity_consine(vector1, vector2):
        cosine = text_utils.get_cosine(vector1, vector2)
        return cosine

    @staticmethod
    def get_similarity_substract(vector1, vector2):
        if vector1 == None or vector2 == None:
            return 0.5    #unknown

        if len(vector1) == 0 or len(vector2) == 0:
            return 0.5   #unknown

        keyList1 = vector1.keys()
        keyList2 = vector2.keys()

        if utils.is_number(str(keyList1[0])) == False or utils.is_number(str(keyList2[0])) == False:
            return 0.5

        diff = abs(float(keyList1[0]) - float(keyList2[0]))
        return 1 - (diff * 1.0 / NORM_SUBSTRACT)

    @staticmethod
    def get_similarity_same(vector1, vector2):
        if vector1 == None or vector2 == None:
            return 0
        if len(vector1) == 0 or len(vector2) == 0:
            return 0
        try:
            keyList1 = vector1.keys()
            keyList2 = vector2.keys()
            if keyList1[0] == keyList2[0]:
                return 1
            else:
                return 0
        except:
            return 0

    @staticmethod
    def get_similarity_distance(vector1, vector2):
        """ vector is of the following schema:
        {'termVector': {lat:-122.1660756, long: 37.42410599999999},
        'name': 'location'}
        """
        if vector1 == None or vector2 == None:
            return 0.5
        if len(vector1) == 0 or len(vector2) == 0:
            return 0.5

        lat1 = vector1['lat']
        long1 = vector1['lng']
        lat2 = vector2['lat']
        long2 = vector2['lng']
        return 1.0 - geo_utils.distance_on_unit_sphere(lat1, long1, lat2, long2)

    @staticmethod
    def get_similarity_list_overlapping_bool(vector1, vector2):
        """
        e.g. input: {'tag1': 1, 'tag2': 1, 'tag3': 1, 'tag4': 1}
        """
        res = 0
        if vector1 == None or vector2 == None:
            #  if missing value, no overlapping
            res = 0

        if len(vector1) == 0 or len(vector2) == 0:
            res = 0
        for k in vector1.keys():
            if k in vector2:
                res = 1
                break
        return res

    @staticmethod
    def get_similarity_list_overlapping_size(vector1, vector2):
        """
        e.g. input: {'tag1': 1, 'tag2': 1, 'tag3': 1, 'tag4': 1}
        Jaccad distance.
        """
        res = 0
        if vector1 == None or vector2 == None:
            #  if missing value, no overlapping
            return 0.0

        len1 = len(vector1)
        len2 = len(vector2)

        if len1 == 0 or len2 == 0:
            return 0.0
        else:
            vs1 = Set(vector1.keys())
            vs2 = Set(vector2.keys())
            union_set = vs1.union(vs2)
            intersection_set = vs1.intersection(vs2)
            return (len(intersection_set) * 1.0) / len(union_set)

    @staticmethod
    def get_similarity_date_difference(vector1, vector2):
        """
        {'termVector': {'date_start': '2013-07-13'}, 'name': 'date_start'}
        """
        dd = 50
        if vector1 == None or vector2 == None:
            #if missing value, say 50 days
            dd = 50

        if len(vector1) == 0 or len(vector2) == 0:
            dd = 50

        dates1 = vector1.values()
        dates2 = vector2.values()

        try:
            day1 = date_utils.str_to_date(dates1[0], "%Y-%m-%d")
            day2 = date_utils.str_to_date(dates2[0], "%Y-%m-%d")
            dd = abs((day1 - day2).days)
        except:
            dd = 50

        difference = dd * (1.0) / 365.0
        simScore = 1 - difference
        if simScore < 0: 
            simScore = 0
        return simScore

    @staticmethod
    def get_similarity_date_overlapping(vector1, vector2):
        """
        {'termVector': {'date_end': '2013-08-02', 'date_start': '2013-07-13'}, 'name': 'date_range'}
        """
        odays = 1.0 / 365.0

        if vector1 == None or vector2 == None:
            odays = 1.0 / 365.0   # if missing value, say 1 days

        if len(vector1) == 0 or len(vector2) == 0:
            odays = 1.0 / 365.0

        startdate1 = vector1['date_start']
        startdate2 = vector2['date_start']
        enddate1 = vector1['date_end']
        enddate2 = vector2['date_end']

        try:
            startd1 = date_utils.str_to_date(startdate1, "%Y-%m-%d")
            startd2 = date_utils.str_to_date(startdate2, "%Y-%m-%d")
            end1 = date_utils.str_to_date(enddate1, "%Y-%m-%d")
            end2 = date_utils.str_to_date(enddate2, "%Y-%m-%d")

            odays = date_utils.overlap_ratio_for_two_date_ranges(startd1, end1, startd2, end2) 
        except:
            odays = 1.0 / 365.0

        return odays


def test():
    CONSINEA = {'ancient': 1, 'language': 1, 'egyptian': 1, 'university': 1, 'chicago': 1, 'culture': 1, 'history': 1}
    CONSINEB = {'biotechnology': 1, 'university': 1, 'century': 1, '21st': 1, 'chicago': 1}
    print '>> test consine similarity'
    print FieldSimilarity.get_similarity_consine(CONSINEA, CONSINEB)

    DATEA = {'date_start': '2012-07-13'}
    DATEB = {'date_start': '2013-07-13'}
    print '>> test date similarity'
    print FieldSimilarity.get_similarity_date_difference(DATEA, DATEB)

    RANGEA = {'date_end': '2013-09-13', 'date_start': '2013-08-21'}
    RANGEB = {'date_end': '2013-09-12', 'date_start': '2013-08-22'}
    print '>> test date overlapping'
    print FieldSimilarity.ge_similarity_date_overlapping(RANGEA, RANGEB)

    A = {3: 1}
    B = {3: 1}
    print '>> test substract'
    print FieldSimilarity.get_similarity_substract(A, B)

    C = {'9': 1, '11': 1, '12': 1}
    D = {'11': 1}
    print '>> test list overlapping'
    print FieldSimilarity.get_similarity_list_overlapping(C, D)

    E = {'University of Chicago': 1}
    F = {'University of Chicago': 2}
    print '>> test same'
    print FieldSimilarity.getSimilarity_same(E, F)

    G = {'lat':37.3393857, 'long': -121.8949555}  # san jose
    H = {'lat':40.7143528, 'long': -74.0059731}   # new york city
    I = {'lat':37.3393857, 'long': -121.8949555}  # san jose
    print '>> test location diff'
    print FieldSimilarity.getSimilarity_distance(I, G)

if __name__ == "__main__":
    test()
