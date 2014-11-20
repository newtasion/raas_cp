"""
Different types of fileds.

Created on Aug 2013
@author: zul110
"""

from collections import Counter

from whoosh.analysis import StemmingAnalyzer
from jieba.analyse import ChineseAnalyzer

from match_engine.exceptions import MatchEngineError
from match_engine.common.date_utils import *
from match_engine.functions.text_filter import TextFilter, EN_ID, CN_ID
from tv_field import TvField
import utils.text_utils as text_utils

EMPTY_VALUE = "None"


class FieldError(MatchEngineError):
    """Raised when a backend can not be found."""
    pass


class Field(object):
    """The base implementation of a search field."""

    FIELD_TYPE_UNKNOWN = 0
    FIELD_TYPE_TEXT = 1
    FIELD_TYPE_NUMERIC = 2
    FIELD_TYPE_DATE = 3
    FIELD_TYPE_DATETIME = 4
    FIELD_TYPE_NUM_RANGE = 5
    FIELD_TYPE_DATE_RANGE = 6
    FIELD_TYPE_TAGS = 7
    FIELD_TYPE_DICT = 8
    FIELD_TYPE_ID = 9
    FIELD_TYPE_SINGLE = 10
    FIELD_TYPE_LOCATION = 11
    FIELD_TYPE_INTEGER = 12
    FIELD_TYPE_FLOAT = 13
    FIELD_TYPE_BOOLEAN = 14
    FIELD_TYPE_KEYWORDS = 15
    FIELD_TYPE_RESERVED4 = 16
    FIELD_TYPE_RESERVED5 = 17

    FIELD_TYPES = (
        (FIELD_TYPE_UNKNOWN, 'unknown'),
        (FIELD_TYPE_TEXT, 'text'),
        (FIELD_TYPE_NUMERIC, 'numeric'),
        (FIELD_TYPE_DATE, 'date'),
        (FIELD_TYPE_DATETIME, 'datetime'),
        (FIELD_TYPE_NUM_RANGE, 'numeric_range'),
        (FIELD_TYPE_DATE_RANGE, 'date_range'),
        (FIELD_TYPE_TAGS, 'tags'),
        (FIELD_TYPE_DICT, 'dict'),
        (FIELD_TYPE_ID, 'id'),
        (FIELD_TYPE_SINGLE, 'single'),
        (FIELD_TYPE_LOCATION, 'location'),
        (FIELD_TYPE_INTEGER, 'integer'),
        (FIELD_TYPE_FLOAT, 'float'),
        (FIELD_TYPE_BOOLEAN, 'boolean'),
        (FIELD_TYPE_KEYWORDS, 'keywords'),
        (FIELD_TYPE_RESERVED4, 'Reserved4'),
        (FIELD_TYPE_RESERVED5, 'Reserved5'))

    field_type = None
    field_dict = {}

    def __init__(self, props={}):
        # Track what the index thinks this field is called.
        self.props = props

    def convert(self, value):
        if value is None:
            return None
        try:
            return value.decode('utf-8', 'ignore')
        except UnicodeEncodeError:
            return unicode(value)

    @staticmethod
    def get_field_class_name(field):
        """Given a field name, get the field class name"""
        return field.capitalize() + "Field"

    @staticmethod
    def get_field_type(field):
        """Given a field id, get the field name"""
        if (field < len(Field.FIELD_TYPES)):
            return Field.FIELD_TYPES[field][1]
        return 'unknown'

    @staticmethod
    def to_tvrec_field(name, field_type, value, props):
        """Returns a :class:`TvField` object.

        :param name: the field name.
        :param field_type: the name of the type of the field.
        :param value: the value of the field.
        :param props: other properties.
        """
        fieldClass = globals()[Field.get_field_class_name(field_type)]
        fieldInst = fieldClass(props=props)
        return fieldInst.to_tvrec_field(name, value)

    @classmethod
    def get_field_dict(cls):
        if not cls.field_dict:
            for k, v in cls.FIELD_TYPES:
                cls.field_dict[v] = k
        return cls.field_dict


class IdField(Field):
    def to_tvrec_field(self, name, value):
        value = self.convert(value)
        info_dict = {value: 1}
        vector = Counter(info_dict)
        termVector = dict(vector.items())
        inst = TvField(name, termVector)
        return inst


class SingleField(Field):
    def to_tvrec_field(self, name, value):
        value = self.convert(value)
        info_dict = {value: 1}
        vector = Counter(info_dict)
        termVector = dict(vector.items())
        inst = TvField(name, termVector)
        return inst


class TextField(Field):
    def to_tvrec_field(self, name, value):
        """
        convert a text to a TvField object.
        """
        value = self.convert(value)

        if 'analyzer' in self.props:
            termVector = TextFilter.text_to_dict(value, self.props['analyzer'])
        else:
            termVector = TextFilter.text_to_dict(value, EN_ID)

        inst = TvField(name, termVector)
        return inst


class LocationField(Field):
    def convert(self, value):
        """
        acceptable inputs:
            '43.5, 23.6'
            or (43.5, 23.6)
            or {'lat':43.5, 'lng':23.6}
        output:
            {'lat':43.5, 'lng':23.6}
        """
        if value is None:
            return None

        if isinstance(value, basestring):
            lat, lng = value.split(',')

        elif isinstance(value, (list, tuple)):
            lat, lng = value[1], value[0]
        elif isinstance(value, dict):
            lat = value.get('lat', 0)
            lng = value.get('lng', 0)

        value = {'lat': float(lat), 'lng': float(lng)}
        return value

    def to_tvrec_field(self, name, value):
        """
        input examples:
          text = 'I've come across a really weird problem.'
        """
        value = self.convert(value)
        termVector = value
        inst = TvField(name, termVector)
        return inst


class IntegerField(Field):
    def to_tvrec_field(self, name, value):
        """
        input examples:
          name: 'publisherId', value: 4501
        """
        value = self.convert(value)
        info_dict = {value: 1}
        vector = Counter(info_dict)
        termVector = dict(vector.items())
        inst = TvField(name, termVector)
        return inst


class FloatField(Field):
    def convert(self, value):
        if value is None or len(value) == 0:
            return None

        try:
            ret = float(value)
        except:
            ret = None
        return ret

    def to_tvrec_field(self, name, value):
        """
        input examples:
          name: 'score', value: 75.6
        """
        value = self.convert(value)
        if (not value):
            return None
        info_dict = {value: 1}
        vector = Counter(info_dict)
        termVector = dict(vector.items())
        inst = TvField(name, termVector)
        return inst


class BooleanField(Field):
    def prepare(self, obj):
        return self.convert(super(BooleanField, self).prepare(obj))

    def convert(self, value):
        if value is None:
            return None
        return bool(value)

    def to_tvrec_field(self, name, value):
        """
        input examples:
          name: 'score', value: 75.6
        """
        value = self.convert(value)
        info_dict = {value: 1}
        vector = Counter(info_dict)
        termVector = dict(vector.items())
        inst = TvField(name, termVector)
        return inst


class DateField(Field):
    def convert(self, value):
        if value is None:
            return None

        if isinstance(value, basestring):
            match = DATETIME_REGEX.search(value)

            if match:
                data = match.groupdict()
                return tuple_to_date(int(data['year']), int(data['month']), int(data['day']))
            else:
                raise FieldError("Date provided to '%s' field doesn't appear to be a valid date string: '%s'" % (self.instance_name, value))
        return value

    def to_tvrec_field(self, name, value):
        """
        input examples:
          name: 'score', value: 75.6
        """
        value = self.convert(value)
        info_dict = {value: 1}
        vector = Counter(info_dict)
        termVector = dict(vector.items())
        inst = TvField(name, termVector)
        return inst


class DateTimeField(Field):
    def convert(self, value):
        if value is None:
            return None

        if isinstance(value, basestring):
            match = DATETIME_REGEX.search(value)

            if match:
                data = match.groupdict()
                return tuple_to_date_time(int(data['year']),
                                          int(data['month']),
                                          int(data['day']),
                                          int(data['hour']),
                                          int(data['minute']),
                                          int(data['second']))
            else:
                raise FieldError("Datetime provided to '%s' field doesn't appear to be a valid datetime string: '%s'" % (self.instance_name, value))
        return value

    def to_tvrec_field(self, name, value):
        """
        input examples:
          name: 'score', value: 75.6
        """
        value = self.convert(value)
        info_dict = {value: 1}
        vector = Counter(info_dict)
        termVector = dict(vector.items())
        inst = TvField(name, termVector)
        return inst


class KeywordsField(Field):
    def convert(self, value):
        if value is None:
            return None
        l = value.split(',')
        ul = []
        for lword in l:
            ul.append(unicode(lword.strip()))
        return ul

    def to_tvrec_field(self, name, value):
        """
        input examples:
          name: 'score', value: 75.6
        """
        value = self.convert(value)
        cnt = Counter()
        for word in value:
            cnt[word] += 1
        termVector = dict(cnt)
        inst = TvField(name, termVector)
        return inst


def unit_test1():
    textField = TextField()
    A = textField.to_tvrec_field('fieldName', u'any text any world text')
    print A
    print A.toString()


def unit_test2():
    Field.to_tvrec_field("title", "Text", 'Harvard1 pig1 any world text')
    pass


def unit_test3():
    print Field.get_field_type(Field.FIELD_TYPE_ID)
    print Field.get_field_type(Field.FIELD_TYPE_TEXT)
    pass

if __name__ == "__main__":
    unit_test2()
