"""
generate rank model and search models. Two ways to get the models:
1. bootstrap
2. train model using training data.

Created on Jul 18, 2013
@author: zul110
"""


IGNORE_MODEL_FIELD_TYPES = ['ID']
RANK_MODEL_FIELD_TYPES = ['TEXT', 'KEYWORDS', 'SINGLE']
INTERCEPT_BOOTSTRAP = 0.1
FEATURE_WEIGHT_BOOTSTRAP = 0.2

METHODS_BOOTSTRAP = {
    'ID': 'SAME',
    'TEXT': 'CONSINE',
    'SINGLE': 'SAME',
    'KEYWORDS': 'LIST_OVERLAPPING',
    'INTEGER': 'SUBSTRACT',
    'FLOAT': 'SUBSTRACT',
    'BOOLEAN': 'SAME',
    'DATETIME': 'DATE_OVERLAPPING',
    'DATE': 'DATE_DIFFERENCE',
    'LOCATION': 'GEO_DISTANCE'
}


def models_generator(fieldtype_dict, procedure='bootstrap', domain_descriptor=None):
    if procedure == 'bootstrap':
        return models_generator_bootstrap(fieldtype_dict)
    else:
        return models_generator_train(fieldtype_dict, domain_descriptor)


def models_generator_train(fieldtype_dict, domain_descriptor):
    """
    given the domain descriptor, the procedure
    1. first locate the training data
    2. call train_processor to get the model
    3. generate the two models.
    """
    pass
    return None


def models_generator_bootstrap(fieldtype_dict):
    rank_model_features = {}
    search_model = {}

    for field_name, field_type in fieldtype_dict.iteritems():
        field_type = field_type.upper()
        if field_type in IGNORE_MODEL_FIELD_TYPES:
            continue

        feature_name = field_name + ':' + field_name
        feature_weight = FEATURE_WEIGHT_BOOTSTRAP
        method = get_method_bootstrap(field_type)
        rank_model_features[feature_name] = {'weight': feature_weight, 'method': method}
        if field_type in RANK_MODEL_FIELD_TYPES:
            search_model[feature_name] = feature_weight

    rank_model = {}
    rank_model['intercept'] = INTERCEPT_BOOTSTRAP
    rank_model['features'] = rank_model_features

    return (search_model, rank_model)


def get_method_bootstrap(fieldType):
    return METHODS_BOOTSTRAP[fieldType]
