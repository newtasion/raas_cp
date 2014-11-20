"""
a python class for describing the domain meta info.

Created on Jul 12, 2013
@author: zul110
"""
import json
import os

from utils.json_utils import jsonToObj
from utils.json_utils import read_json, read_json_with_lower
from utils.text_utils import norm_dict


class DomainDescriptor:
    def __init__(self, domain_id, id_field,
                 field_types, rank_model=None, search_model=None,
                 language='en', num_candidates=20, num_matches=10,
                 data_path="", data_file="", props={}):
        self._domain_id = domain_id
        self._id_field = id_field
        self._field_types = field_types
        if rank_model:
            self._rank_model = norm_dict(rank_model)
        if search_model:
            self._search_model = norm_dict(search_model)
        self._language = language
        self._num_candidates = num_candidates
        self._num_matches = num_matches
        self._data_path = data_path
        self._data_file = data_file
        self._props = props

    def get_language(self):
        return self._language

    def get_num_candidates(self):
        return self._num_candidates

    def get_num_matches(self):
        return self._num_matches

    def get_data_path(self):
        return self._data_path

    def get_data_file(self):
        return self._data_file

    def get_id_field(self):
        return self._id_field

    def get_type(self, fieldName):
        """
        give a field name,
        it returns its type.
        """
        fieldName = fieldName.lower()
        if fieldName in self._field_types:
            return self._field_types[fieldName]
        else:
            return None

    def get_search_model(self):
        return self._search_model

    def get_search_model_features(self):
        return self._search_model.keys()

    def get_index_fields(self):
        indexFields = [self.get_id_field()]
        for featureName in self.get_search_model_features():
            source_target = featureName.split(':')
            if len(source_target) < 2:
                continue
            indexFields.append(source_target[1])
        return set(indexFields)

    def get_search_map(self):
        iMap = {}
        for featureName, featureValue in self._search_model.items():
            source_target = featureName.split(':')
            if len(source_target) < 2:
                continue
            target = source_target[1]
            source = source_target[0]
            if source not in iMap:
                iMap[source] = {target: featureValue}
            else:
                iMap[source][target] = featureValue
        return iMap

    def get_rank_model_dict(self):
        return self._rank_model

    def set_config(self, config):
        if 'fields' in config:
            self._field_types = config['fields']
        if 'language' in config:
            self._language = config['language']
        if 'num_candidates' in config:
            self._num_candidates = config['num_candidates']
        if 'num_matches' in config:
            self._num_matches = config['num_matches']

    def set_rank_model(self, rank_model):
        self._rank_model = norm_dict(rank_model)

    def set_search_model(self, search_model):
        self._search_model = norm_dict(search_model)

    @classmethod
    def from_json(cls, descJson):
        """
        parse the profileJson
        """
        if not descJson:
            return None

        descJson = descJson.lower()
        descDict = jsonToObj(descJson)
        language = descDict['language'] if 'language' in descDict else 'en'
        return DomainDescriptor(
                    domain_id=descDict['domain_id'],
                    id_field=descDict['id_field'],
                    field_types=descDict['field_types'],
                    rank_model=descDict['rank_model'],
                    search_model=descDict['search_model'],
                    language=language,
                    data_path=descDict['data_path'],
                    num_matches=descDict['num_matches'],
                    num_candidates=descDict['num_candidates'],)

    def to_json_string(self):
        dd = {
            "domain_id": self._domain_id,
            "id_field": self._id_field,
            "field_types": self._field_types,
            "rank_model": self._rank_model,
            "search_model": self._search_model,
            "data_path": self._data_path,
            "data_file": self._data_file,
            "num_candidates": self._num_candidates,
            "num_matches": self._num_matches,
            "props": self._props,
            'language': self._language
        }
        return json.dumps(dd)

    @classmethod
    def fromDbMockUp(cls, domain, dataroot):
        """domain: domain name,
           dataroot: absolute path of data
        """
        index = read_json_with_lower(os.path.join(dataroot, "index.json"))
        fields = index['fields']
        rank_model = read_json_with_lower(os.path.join(dataroot, "rankmodel.json"))
        search_model = read_json_with_lower(os.path.join(dataroot, "searchmodel.json"))
        language = index['language']
        num_candidates = index['num_candidates']
        num_matches = index['num_matches']
        return DomainDescriptor(
                domain, "id", fields,
                rank_model, search_model,
                language, num_candidates, num_matches, dataroot, {})


def unit_test():
    domainDescriptor = DomainDescriptor.from_db_mock_up('test')
    print domainDescriptor.get_type('ProductGroup')
    print domainDescriptor.get_index_fields()


if __name__ == "__main__":
    unit_test()
