"""
utilities for database access.


Created on Jul 12, 2013
@author: zul110
"""


import MySQLdb
import datetime


class DbUtils():
    """
    Basic utils of access MySQL database.
    """
    def __init__(self, host='', user='', passwd='', database_name=''):
        self.connection = MySQLdb.connect(
            host = host,
            user = user,
            passwd = passwd
        )

        self.cursor = self.connection.cursor()
        if database_name:
            self.execute("CREATE DATABASE IF NOT EXISTS %s;" % database_name)
            self.execute("USE %s;" % database_name)
            self._database_name = database_name
        else:
            raise ValueError

    def get_cursor(self):
        return self.cursor

    def execute(self, sql):
        """
        executes an sql statement, may throw MySQLError
        """
        return self.cursor.execute(sql)

    def execute_select(self, sql):
        self.execute(sql)
        return self.cursor.fetchall()

    def convert_sqldate_to_date(self, value, defaultDateStr):
        """
        input e.g., 2013-07-08
        """
        if value == None:
            valueStr = defaultDateStr
        else:
            valueStr = str(value)

        if len(valueStr) == 0:
            return defaultDateStr
        try:
            return datetime.datetime.strptime(valueStr, "%Y-%m-%d").date()
        except:
            return datetime.datetime.strptime(defaultDateStr, "%Y-%m-%d").date()
