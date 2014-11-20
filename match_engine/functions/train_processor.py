'''
Train the model using training set.

Here is an example to run this program:
>> python train_processor.py -i ~/inputFile -l ./logFile
-m :   the meta data file
-i :   the path of the input file
-l:    the path of the log file.

http://scikit-learn.org/stable/modules/sgd.html

Created on Jul 05, 2013
@author: zul110
"""
'''
import optparse
import logging.handlers
import sys

import numpy as np
import pylab as pl
import os
import csv
from sklearn import svm
from sklearn.svm import SVC
from sklearn.cross_validation import StratifiedKFold
from sklearn.metrics import f1_score
from sklearn.metrics import zero_one
from sklearn.grid_search import GridSearchCV
from sklearn.feature_selection import RFECV
from sklearn import datasets

iris = datasets.load_iris()

LOSS_FUNC = "log"
PENALTY = 'elasticnet'
N_ITER = 8

ROOTDIR = '/home/znli/workspace/pymh_models/samePoss/featureDumpTxtst3'
INPUT = os.path.join(ROOTDIR, 'trainingData.txt')
OUTPUT = os.path.join(ROOTDIR, 'outputFromPy')
TRUTH = os.path.join(ROOTDIR, 'outputB')
DEFAULTLOG = os.path.join(ROOTDIR, 'plog')
META = os.path.join(ROOTDIR, 'metadata.txt')


class ModelTrainer():
    def __init__(self, training_file):
        if not os.path.exists(training_file):
            _log.error('Cannot find the input %s' (input))
            print 'Error: Cannot find the input %s' % (training_file)
            sys.exit(-1)
        self.queryfile = training_file

        self.train_data = None
        self.test_data = None
        self.train_target = None
        self.test_target = None
        self.test_data_aid = []
        self.np_sample_train = 0
        self.np_sample_test = 0
        self.np_feature = 0
        self.csv_output = None
        self.scaler = None
        self.predict_model = None

        self.featuremask = None

    def load_json_data(self):
        """
        load json file as training data.
        ["1", [{"name": "title:title", "value": 0.5962847939999439},
        {"name": "image:image", "value": 0.0},
        {"name": "feature:feature", "value": 0.2857142857142857}]]
        """
        pass
        #Get number of samples
        metafile = csv.reader(open(self.metafile), delimiter)
        for i, ir in enumerate(metafile):
            if ir[0] == "numObservations":
                self.np_sample_train = int(ir[1])

        self.np_feature = 48
        maxcol = 0
        self.train_data = np.empty((self.np_sample_train, self.np_feature))
        self.train_target = np.empty((self.np_sample_train, ), dtype = np.int)

        #process input file

        datafile = csv.reader(open(self.queryfile), delimiter = ',')  
        for i, ir in enumerate(datafile):

            self.train_target[i] = ir[2]
            row = ir[3:]

            for j in range(len(row)/3):
                #i is the row index, 
                tRow = int(row[3*j+0])
                tCol = int(row[3*j+1])
                if tCol > maxcol:
                    maxcol = tCol
                tValue = float(row[3*j+2])

                self.train_data[tRow-1][tCol-1] = tValue  

        print maxcol
        _log.info("number of trainning example is: %d" %(self.np_sample_train))
        _log.info("number of dimensions is: %d" %(self.np_feature))
        
        


    def load_tsv_data(self, delimiter):
        """
        load tsv file as training data.
        assume The last row is the label.
        """

        #Get number of samples
        metafile = csv.reader(open(self.metafile), delimiter)
        for i, ir in enumerate(metafile):
            if ir[0] == "numObservations":
                self.np_sample_train = int(ir[1])

        self.np_feature = 48
        maxcol = 0
        self.train_data = np.empty((self.np_sample_train, self.np_feature))
        self.train_target = np.empty((self.np_sample_train, ), dtype = np.int)

        #process input file

        datafile = csv.reader(open(self.queryfile), delimiter = ',')  
        for i, ir in enumerate(datafile):

            self.train_target[i] = ir[2]
            row = ir[3:]

            for j in range(len(row)/3):
                #i is the row index, 
                tRow = int(row[3*j+0])
                tCol = int(row[3*j+1])
                if tCol > maxcol:
                    maxcol = tCol
                tValue = float(row[3*j+2])

                self.train_data[tRow-1][tCol-1] = tValue  

        print maxcol
        _log.info("number of trainning example is: %d" %(self.np_sample_train))
        _log.info("number of dimensions is: %d" %(self.np_feature))


    def preprocessing(self):
        """
        preprocessing steps: Standardize all features using z-score
        """
        from sklearn import preprocessing
        self.scaler = preprocessing.Scaler().fit(self.train_data)
        _log.info("scaler mean: %s" % (self.scaler.mean_))
        _log.info("scaler stand deviation: %s" % (self.scaler.std_))
        self.train_data = self.scaler.transform(self.train_data)


    def feature_analysis(self):
        """analyze features using:
            1. generate two random features and mit them with informative features.
            2. apply a linear classifier and get the weights of all features. 
        """
        methods = ['2']
        # Add one noisy feature for comparison
        E = np.random.normal(size=(len(self.train_data), 2))        
        x = np.hstack((self.train_data, E))
        y = self.train_target       
        
#       feature importance scores of a fit gradient boosting model can be accessed via the feature_importances_ property
        if '1' in methods:
            from sklearn.datasets import make_hastie_10_2
            from sklearn.ensemble import GradientBoostingClassifier
            clf = GradientBoostingClassifier(n_estimators=100, learn_rate=1.0, max_depth=1).fit(x, y)
            print clf.feature_importances_  

#        1. feature analysis using univariate feature selection
        if '2' in methods:
            print "Analyzing features using univariate feature selection"
            pl.figure(1)
            pl.clf()
            x_indices = np.arange(x.shape[-1])  
            from sklearn.feature_selection import SelectPercentile, f_classif
            selector = SelectPercentile(f_classif, percentile=10)
            selector.fit(x, y)
            print selector.scores_
            pl.bar(x_indices-.15, selector.scores_, width=.3, label='univariate score',color='b')
            pl.savefig(os.path.join(ROOTDIR, "feature_analysis_uf_ftest.png"))
        
#       2. feature analysis using SVM-RFE
        if '3' in methods:
            print "Analyzing features using SVM-RFE"
            pl.figure(1)
            pl.clf()
                  
            x_indices = np.arange(x.shape[-1])               
            clf = svm.SVC(kernel='linear')
            clf.fit(x, y)        
            svm_weights = (clf.coef_**2).sum(axis=0)
            svm_weights /= svm_weights.max()
            print svm_weights
            pl.bar(x_indices-.15, svm_weights, width=.3, label='SVM weight',color='b')
                            
            pl.title("feature weights analysis")
            pl.xlabel('Feature number')
            pl.yticks(())
            pl.axis('tight')
            pl.show()
            pl.savefig(os.path.join(ROOTDIR, "feature_analysis_svmref_ftest.png"))

    
    def feature_selection(self):
        """
        select features using SVM-RFE. see papar :
            [1] Guyon, I., Weston, J., Barnhill, S., & Vapnik, V., "Gene selection for cancer classification 
            using support vector machines", Mach. Learn., 46(1-3), 389--422, 2002.
        """
        svc = SVC(kernel="linear")
        rfecv = RFECV(estimator=svc,
        step=1,
        cv=StratifiedKFold(self.train_target, 2),
        loss_func=zero_one)
        rfecv.fit(self.train_data, self.train_target)
        
        self.featuremask = rfecv.support_
        _log.info("after svm-rfe, the feature selection mask is shown as following:")
        _log.info(self.featuremask)
        self.train_data = self.train_data[:, self.featuremask] 

    
    def model_selection(self):   
        """
        step 1: use svc with linear kernel, which tunes Penalty C using grid search. 
        """ 
        tuned_parameters = [{'kernel': ['linear'], 'C': [1, 10, 100, 1000]}]
        clf = GridSearchCV(SVC(C=1), tuned_parameters, score_func=f1_score)
        clf.fit(self.train_data, self.train_target, cv=StratifiedKFold(self.train_target, 5))
        
        _log.info('Best SVC Classifier is:')
        _log.info(clf.best_estimator_)
        
        """
        step 2: train the model using all data. 
        """
        self.predict_model = clf.best_estimator_
        self.predict_model.fit(self.train_data, self.train_target)
    
    
    def model_without_selection(self):           
        """
        to save time, skip the grid search.
        """
        self.predict_model = svm.SVC(kernel='linear', C=1)
        self.predict_model.fit(self.train_data, self.train_target)
        
        
    def model_sgd(self):           
        """
        use sgd, L1+L2.
        """
        from sklearn.linear_model import SGDClassifier
        self.predict_model = SGDClassifier(loss="log", penalty="l2", n_iter=8)
        self.predict_model.fit(self.train_data, self.train_target)
        
        #analyze the weights
        pl.figure(1)
        pl.clf()
        x_indices = np.arange(self.train_data.shape[-1])  
        coefs = self.predict_model.coef_[0]
        
        _log.info("scaler mean: %s" % (str(coefs)))
        
        coefs /= coefs.max()
        
        pl.bar(x_indices-.15, coefs, width=.3, label='SGD weight',color='b')
                        
        pl.title("feature weights analysis")
        pl.xlabel('Feature number')
        pl.yticks(())
        pl.axis('tight')
        pl.show()
        pl.savefig(os.path.join(ROOTDIR, "feature_analysis_sgd_hinge.png"))

     

    def predict_with_sgd(self, trainSet, trainTarget, testSet, testTarget):
        #train model
        from sklearn.linear_model import SGDClassifier
        self.predict_model = SGDClassifier(loss="log", penalty="elasticnet", n_iter=8)
        self.predict_model.fit(trainSet, trainTarget)
        
        #predict against testSet
        res = self.predict_model.predict(testSet)                   
        
        #if the truthfile is provided, we can estimate the accuracy.
        if testTarget != None:            
            rightcount = 0
            totalcount = 0
            for i in range(len(testSet)):
                totalcount += 1
                if int(res[i]) == testTarget[i]:
                    rightcount += 1            
            print "Total number of queries is %d, and number of right predictions is %d" %(totalcount, rightcount)
            print "The accuracy againt the truth file is: %f" % (rightcount/float(totalcount))
            print
            from sklearn.metrics import classification_report
            print classification_report(testTarget, res)
            
        
    def validate_model_cv(self):  
        from sklearn import cross_validation
        k_fold = cross_validation.StratifiedKFold(self.train_target, k=4)
        for train, test in k_fold:
            trainSet = self.train_data[train]
            trainTarget = self.train_target[train]
            testSet = self.train_data[test]
            testTarget = self.train_target[test]
            self.predict_with_sgd(trainSet, trainTarget, testSet, testTarget)
       
            
            
    def predict(self):  
        self.test_data = self.train_data
        res = self.predict_model.predict(self.test_data)                   
        for i in range(len(res)):
            if int(res[i]) == 1:
                ostr = '+1'
            else:
                ostr = '-1'
            self.csv_output.writerow([self.test_data_aid[i], ostr])
        
        #if the truthfile is provided, we can estimate the accuracy.
        if self.test_target != None:            
            rightcount = 0
            totalcount = 0
            for i in range(len(self.test_data)):
                totalcount += 1
                if int(res[i]) == self.test_target[i]:
                    rightcount += 1            
            print "Total number of queries is %d, and number of right predictions is %d" %(totalcount, rightcount)
            print "The accuracy againt the truth file is: %f" % (rightcount/float(totalcount))
            print
            from sklearn.metrics import classification_report
            print classification_report(self.test_target, res)
            _log.info("scaler mean: %s" % (str(classification_report(self.test_target, res))))


def main(options, args):
    print
    print "==> initilizing the classifier, reading data files..."
    qc = ModelTrainer(options.input, options.output, options.truthfile)
    
    qc.convert_data()
    qc.preprocessing()
    qc.feature_analysis()
    qc.feature_selection()
    qc.model_selection()
    qc.model_without_selection() #use svm
    
    print "==> start training the model..."
    qc.model_sgd()  #use sgd L2
    
    #qc.predict_train()  
    qc.validate_model_cv()  

    print "==> The job is done. Please check the output file."

if __name__ == '__main__':
    global _log
    global mailer_inst
    
    option_parser = optparse.OptionParser()

    option_parser.add_option('-v', '--verbosity', metavar='LVL',
        action='store', type='int', dest='verbosity', default=3,
        help='debug verbosity level (0-3), default is 3')    
    option_parser.add_option('-l', '--log-filename', metavar='LOG_FN',
        action='store', type='string', dest='log_filename', default=DEFAULTLOG,
        help='path to the base log filename. ')
    option_parser.add_option('-i', '--input', metavar='INPUT_FN',
        action='store', type='string', dest='input', default=INPUT,
        help='the input file of the model.')
    option_parser.add_option('-o', '--output', metavar='OUTPUT_FN',
        action='store', type='string', dest='output', default=OUTPUT,
        help='the output file of the model.')
    
    
    (options, args) = option_parser.parse_args()

    #logging
    if options.verbosity == 0:
        llevel = logging.WARN
    elif options.verbosity == 1:
        llevel = logging.INFO
    elif options.verbosity == 2:
        llevel = logging.DEBUG
    elif options.verbosity >= 3:
        llevel = logging.DEBUG
    if options.log_filename:
        log_filename = os.path.realpath(options.log_filename)
        handler = logging.handlers.RotatingFileHandler(log_filename)
        print >> sys.stderr, 'logging to %s' % (log_filename,)
    else:
        handler = logging.StreamHandler()

    handler.setFormatter(
        logging.Formatter(
        '%(pathname)s(%(lineno)d): [%(name)s] '
        '%(levelname)s %(message)s'))

    _log = logging.getLogger('')
    _log.addHandler(handler)
    _log.setLevel(level=llevel)
    _log.info('Classifier Started')

    main(options, args)
    
    




