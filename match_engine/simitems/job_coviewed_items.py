#-*-coding: utf-8 -*-

"""
python job_coviewed_items.py --output $output $input 

Created on Sep 2013
@author: zul110
"""

from mrjob.job import MRJob
from match_engine.functions.cf_similarity import correlation, cosine, regularized_correlation
from match_engine.datamodel.matches import Match, MatchList
from match_engine.datamodel.user_rate import UserRate
from mrjob.protocol import PickleProtocol
from math import sqrt
try:
    from itertools import combinations
except ImportError:
    from metrics import combinations


PRIOR_COUNT = 10
PRIOR_CORRELATION = 0
LIMIT = 10


class JobGetCoViewed(MRJob):
    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = PickleProtocol

    def steps(self):
        return [
            self.mr(mapper=self.group_by_user_mapper,
                    reducer=self.group_by_user_reducer),
            self.mr(mapper=self.pairwise_items_mapper,
                    reducer=self.pairwise_items_reducer),
            self.mr(reducer=self.group_matches_reducer)]

    def configure_options(self):
        super(JobGetCoViewed, self).configure_options()

        self.add_passthrough_option(
            '--inputformat', dest='inputformat', default='tsv',
            help='the format of the input')

        self.add_passthrough_option(
            '--alg', dest='algorithm', default='cosine',
            help='ALGORITHM: the algorithm to compute similarity.')

        self.add_passthrough_option(
            '--priorcount', dest='prior_count', default=10, type='int',
            help='PRIOR_COUNT: Parameter to regularize correlation')

        self.add_passthrough_option(
            '--priorcorrelation', dest='prior_correlation', default=0,
             type='int',
             help='PRIOR_CORRELATION: Parameter to regularize correlation')

        self.add_passthrough_option(
            '--minraters', dest='min_num_raters', default=3, type='int',
            help='the minimum number of raters')

        self.add_passthrough_option(
            '--maxraters', dest='max_num_raters', default=10000, type='int',
            help='the maximum number of raters')

        self.add_passthrough_option(
            '--minintersec', dest='min_intersection', default=0, type='int',
            help='the minimum intersection')

    def group_by_user_mapper(self, key, line):
        """
        Emit the user_id and group by their ratings (item and rating)
        17  70,3
        35  21,1
        49  19,2
        49  21,1
        49  70,4
        87  19,1
        87  21,2
        98  19,2
        """
        userRateObj = UserRate.from_line(line, self.options.inputformat)
        if (userRateObj == None):
            return
        self.increment_counter('mapper', 'profiles', 1)
        user_id = userRateObj.getUid()
        item_id = userRateObj.getIid()
        rating = userRateObj.getRate()

        yield  user_id, (item_id, float(rating))

    def group_by_user_reducer(self, user_id, values):
        """
        For each user, emit a row containing their "postings"
        (item,rating pairs)
        Also emit user rating sum and count for use later steps.

        17    1,3,(70,3)
        35    1,1,(21,1)
        49    3,7,(19,2 21,1 70,4)
        87    2,3,(19,1 21,2)
        98    1,2,(19,2)
        """
        item_count = 0
        item_sum = 0
        final = []
        for item_id, rating in values:
            item_count += 1
            item_sum += rating
            final.append((item_id, rating))

        yield user_id, (item_count, item_sum, final)

    def pairwise_items_mapper(self, user_id, values):
        '''
        The output drops the user from the key entirely, instead it emits
        the pair of items as the key:

        19,21  2,1
        19,70  2,4
        21,70  1,4
        19,21  1,2

        This mapper is the main performance bottleneck.  One improvement
        would be to create a java Combiner to aggregate the
        outputs by key before writing to hdfs, another would be to use
        a vector format and SequenceFiles instead of streaming text
        for the matrix data.
        '''
        item_count, item_sum, ratings = values
        #print item_count, item_sum, [r for r in combinations(ratings, 2)]
        #bottleneck at combinations
        for item1, item2 in combinations(ratings, 2):
            yield (item1[0], item2[0]), \
                    (item1[1], item2[1])

    def pairwise_items_reducer(self, pair_key, lines):
        '''
        Sum components of each corating pair across all users who rated both
        item x and item y, then calculate pairwise pearson similarity and
        corating counts.  The similarities are normalized to the [0,1] scale
        because we do a numerical sort.

        19,21   0.4,2
        21,19   0.4,2
        19,70   0.6,1
        70,19   0.6,1
        21,70   0.1,1
        70,21   0.1,1
        '''
        sum_xx, sum_xy, sum_yy, sum_x, sum_y, n = (0.0, 0.0, 0.0, 0.0, 0.0, 0)
        item_pair, co_ratings = pair_key, lines
        item_xname, item_yname = item_pair
        for item_x, item_y in lines:
            sum_xx += item_x * item_x
            sum_yy += item_y * item_y
            sum_xy += item_x * item_y
            sum_y += item_y
            sum_x += item_x
            n += 1

        sim_score = 0.0
        if self.options.algorithm == 'correlation':
            sim_score = correlation(n, sum_xy, sum_x, sum_y, sum_xx, sum_yy)
        elif self.options.algorithm == 'reg_correlation':
            sim_score = regularized_correlation(n, sum_xy, sum_x, sum_y, sum_xx, sum_yy, PRIOR_COUNT, PRIOR_CORRELATION)
        elif self.options.algorithm == 'cosine':
            sim_score = cosine(sum_xy, sqrt(sum_xx), sqrt(sum_yy))
        else:
            return

        if int(n) > self.options.min_intersection:
            yield item_xname, (item_yname, sim_score, n)

    def group_matches_reducer(self, key, values):
        sourceId = key
        matches = []
        for value in values:
            item_yname, sim_score, n = value
            matches.append((sim_score, item_yname))
        matches = sorted(matches, reverse=True)
        tgtList = []
        cnt = 0
        for entry in matches:
            score = entry[0]
            targetId = entry[1]
            match = Match(targetId, score)
            tgtList.append(match)
            cnt += 1
            if cnt >= LIMIT:
                break
        matchList = MatchList(sourceId, tgtList)
        yield key, matchList.toJson()

if __name__ == '__main__':
    JobGetCoViewed.run()
