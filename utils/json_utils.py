"""
utilities for processing json. 

Created on Jul 12, 2013
@author: zul110
"""
import json


def objToJson(obj):
    jsonStr = json.dumps(obj)
    return jsonStr


def jsonToObj(jsonStr):
    return json.loads(jsonStr)


def read_json(filename):
    data = None
    with open(filename) as inf:
        data = json.load(inf)
    return data

def read_json_with_lower(filename):
    data = None
    with open(filename) as inf:
        content = inf.read()
        content = content.lower()
        data = json.loads(content)
    return data

if __name__ == "__main__":
    A = {}
    A['abc'] = 3
    A['cd'] = {'c':'c', 'd':'d'}
    print A
    B = objToJson(A)
    print B
    C = jsonToObj(B)
    print C
    print C['abc']

    TE= """
    {
    	  "domain_id": "23423",
    	  "domain_name": "meedow",
    	  "lang": "en",
    	  "fieldTypes": 
    	  {
    		"title": "Text",
    		"description": "Text",
    		"artist_name": "Text",
    		"date": "Date", 
    		"price": "Numeric", 
    		"genre": "Tags"
    	  },
    	  "model":
    	  {
    		'version' : 1.0,
    		'intercept' : 0.1, 
    		'title:title':0.6,
    		'highlights:highlights':0.3,
    		'cost_avg:cost_avg':0.3,
    		'date_range:date_range':0.4,
    		'date_start:date_start':0.3,
    		'date_end:date_end':0.2,
    		'duration:duration':0.3,
    		'accept_cn_resident:accept_cn_resident':0.6,
    		'featured:featured':0.4,
    		'host_name:host_name':0.4,
    		'location:location':0.5,
    		'grade:grade':0.6,
    		'subject_minor:subject_minor':0.6,
    		'subject_major:subject_major':0.6,
    		'category:category':0.6
    	  }
    	}
    """
    print jsonToObj(TE)
   
    




