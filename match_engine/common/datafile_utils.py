"""
utilities for dealing with data files in the system. 


Created on Jul 12, 2013
@author: zul110
"""

import os

INDEX = 'domain_index'
MATCHES = 'domain_matches'
TVREC = 'domain_tvrec'
PROFILE = 'domain_profile'
CANDIDATE = "candidate"
TRAININGDIR = "training"


def getTrainningDir(domainDir):
    return os.path.join(domainDir, TRAININGDIR)


def getIndexFile(domainDir):
    return os.path.join(domainDir, INDEX)


def getMatchesFile(domainDir):
    return os.path.join(domainDir, MATCHES)


def getTvrecFile(domainDir):
    return os.path.join(domainDir, TVREC)


def getProfileFile(domainDir):
    return os.path.join(domainDir, PROFILE)


def getCandidateFile(domainDir):
    return os.path.join(domainDir, CANDIDATE)


def getTrainningDirFromDD(domainDescriptor):
    domainDir = domainDescriptor.get_data_path()
    return getTrainningDir(domainDir)


def getIndexFileFromDD(domainDescriptor):
    domainDir = domainDescriptor.get_data_path()
    return getIndexFile(domainDir)


def getMatchesFileFromDD(domainDescriptor):
    domainDir = domainDescriptor.get_data_path()
    return getMatchesFile(domainDir)


def getTvrecFileFromDD(domainDescriptor):
    domainDir = domainDescriptor.get_data_path()
    return getTvrecFile(domainDir)


def getProfileFileFromDD(domainDescriptor):
    domainDir = domainDescriptor.get_data_path()
    return getProfileFile(domainDir)


def getCandidateFileFromDD(domainDescriptor):
    domainDir = domainDescriptor.get_data_path()
    return getCandidateFile(domainDir)


def isIndexFile(fileName):
    if str(fileName).find(INDEX) > 0:
        return True
    else:
        return False


def isTvrecFile(fileName):
    if str(fileName).find(TVREC) > 0:
        return True
    else:
        return False


def isMatchesFile(fileName):
    if str(fileName).find(MATCHES) > 0:
        return True
    else:
        return False


def isProfileFile(fileName):
    if str(fileName).find(PROFILE) > 0:
        return True
    else:
        return False


def isCandidateFile(fileName):
    if str(fileName).find(CANDIDATE) > 0:
        return True
    else:
        return False


def get_base_dir(filepath):
    dirname = os.path.dirname(filepath)
    return dirname.split('/')[-1]
