"""
fieldTypeGenerator(filePath, delimiter='\t'):

given a path to a tsv file, fieldTypeGenerator will guess the field_type for each column.

Created on Jul 18, 2013
@author: zul110
"""
import csv
import json
import os

from collections import defaultdict
from match_engine.datamodel.field_type import Field
from utils.text_utils import is_datetime
from utils.text_utils import is_empty
from utils.text_utils import getMD5hash
from utils.text_utils import is_int
from utils.text_utils import is_number


BOOL_INT = [-1, 0, 1, 2]
MAX_NUM_CATEGORIES = 10
MAX_EMPTY_COUNT = 0.4  # if 40% entries are empty, try more examples.
MAX_NUM_ROWS = 3000


def field_classifier_from_csv(filepath, has_header=False, delimiter='\t'):
    fields = defaultdict(list)
    header = {}

    with open(filepath, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=delimiter)
        cnt = 0
        for row in reader:
            cnt += 1
            if cnt == 1:
                for idx, col in enumerate(row):
                    fields[idx] = []
                    header[idx] = col if has_header else 'Column' + str(idx)
                if has_header:
                    continue
            elif cnt < MAX_NUM_ROWS:
                for idx, col in enumerate(row):
                    fields[idx].append(col)
            else:
                continue
    res = {}
    for idx, datums in fields.iteritems():
        res[header[idx]] = generate_field_type(datums)
    return res, cnt


def field_classifier_from_linejson(file_path):
    fields = defaultdict(list)
    cnt = 0
    with open(file_path) as inf:
        for line in inf:
            try:
                cols = json.loads(line.strip())
                for k, v in cols.iteritems():
                    fields[k].append(v)
                cnt += 1
                if cnt >= MAX_NUM_ROWS:
                    break
            except:
                pass
    res = {}
    for col, dat in fields.iteritems():
        try:
            res[col] = generate_field_type(dat)
        except Exception, e:
            print "col", col, e
    return res, cnt


def field_classifier(filepath, has_header=False, delimiter='\t'):
    _, ext = os.path.splitext(filepath)
    if ext == ".tsv":
        return field_classifier_from_csv(filepath, has_header, delimiter)
    elif ext == ".json":
        return field_classifier_from_linejson(filepath)
    else:
        return [], None


def numeric_type(ipt):
    t = Field.get_field_type(Field.FIELD_TYPE_TEXT)
    if is_number(ipt):
        if is_int(ipt):
            t = Field.get_field_type(Field.FIELD_TYPE_INTEGER)
        else:
            t = Field.get_field_type(Field.FIELD_TYPE_FLOAT)
    return t


def generate_field_type(datums):
    """
    variance of list will not be used in our online env.
    """
    var_set = {}

    all_float = True
    all_int = True
    all_int_boolean = True
    all_text = True

    text_total_len = 0
    text_total_splits = 0

    all_count = 0
    empty_count = 0
    text_count = 0
    # the first run.
    for datum in datums:
        if isinstance(datum, basestring):
            datum = datum.strip()
        try:
            datum = unicode(datum)
        except UnicodeDecodeError:
            return Field.get_field_type(Field.FIELD_TYPE_TEXT)

        all_count += 1
        if is_empty(datum):
            empty_count += 1
            continue

        ttype = numeric_type(datum)
        if ttype == Field.get_field_type(Field.FIELD_TYPE_TEXT):
            all_float = False
            all_int = False
            all_int_boolean = False
            text_count += 1
            text_total_len += len(datum)
            splits = datum.split(',')
            text_total_splits += len(splits)

        elif all_float and ttype == Field.get_field_type(Field.FIELD_TYPE_FLOAT):
            all_int = False
            all_int_boolean = False
            all_text = False

        elif all_int and ttype == Field.get_field_type(Field.FIELD_TYPE_INTEGER):
            all_text = False
            all_float = False
            int_value = int(datum)
            if int_value in var_set:
                var_set[int_value] += 1
            else:
                var_set[int_value] = 1
            if all_int_boolean and (int_value not in BOOL_INT):
                all_int_boolean = False

    # analyze results of the first run and start the second run.
    if float(empty_count) / all_count >= MAX_EMPTY_COUNT:
        return Field.get_field_type(Field.FIELD_TYPE_TEXT)

    if all_float:
        return Field.get_field_type(Field.FIELD_TYPE_FLOAT)

    if all_int:
        # id, integer or single (categorical)
        num_int = all_count - empty_count
        num_variants = len(var_set.keys())
        if num_int == num_variants:
            return Field.get_field_type(Field.FIELD_TYPE_ID)
        if num_variants <= MAX_NUM_CATEGORIES:
            return Field.get_field_type(Field.FIELD_TYPE_SINGLE)
        else:
            return Field.get_field_type(Field.FIELD_TYPE_INTEGER)

    if all_int_boolean:
        # single (categorical) or boolean : actually are the same.
        num_variants = len(var_set.keys())
        if num_variants == 2:
            return Field.get_field_type(Field.FIELD_TYPE_BOOLEAN)
        else:
            return Field.get_field_type(Field.FIELD_TYPE_SINGLE)

    if all_text:
        # text, single, keywords, id
        all_datetime = True
        see_comma = False
        text_avg_len = (text_total_len * 1.0) / text_count
        text_avg_splits = (text_total_splits * 1.0) / text_count
        text_min_len = 1000000
        text_max_len = 0

        len_sum_var = 0
        split_sum_var = 0

        var_set = {}

        for datum in datums:
            if is_empty(datum):
                continue

            datum = unicode(datum).strip()
            if all_datetime is True:
                all_datetime = is_datetime(datum)

            # the mean and variance of the text length.
            my_len = len(datum)
            len_sum_var += ((my_len - text_avg_len) * (my_len - text_avg_len))
            if my_len > text_max_len:
                text_max_len = my_len
            if my_len < text_min_len:
                text_min_len = my_len

            # the mean and variance of the number of splits.
            splits = datum.split(',')
            num_splits = len(splits)
            if num_splits > 1:
                see_comma = True
            split_sum_var += ((num_splits - text_avg_splits) * (num_splits - text_avg_splits))

            for split in splits:
                md5_value = getMD5hash(split)
                if md5_value in var_set:
                    var_set[md5_value] += 1
                else:
                    var_set[md5_value] = 1

        if all_datetime is True:
            return 'dateTime'

        if see_comma is False and len(var_set.keys()) == text_count and len_sum_var <= 0.00001 and text_avg_len <= 32:
            return Field.get_field_type(Field.FIELD_TYPE_ID)

        if see_comma is False and text_avg_len <= 40:  # no splits,
            return Field.get_field_type(Field.FIELD_TYPE_SINGLE)

        # not be able to detect: ['UPI Marketing, Inc.']
        if see_comma is True and len(var_set.keys()) < text_count / 3 and text_avg_len <= 100:
            return Field.get_field_type(Field.FIELD_TYPE_KEYWORDS)
        # we dont detect keywords now, since we can also use cosine similarity for keywords type.
        # we can train a model when we have enough data.
        return Field.get_field_type(Field.FIELD_TYPE_TEXT)
