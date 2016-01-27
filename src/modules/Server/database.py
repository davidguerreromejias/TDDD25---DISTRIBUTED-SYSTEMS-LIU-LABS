# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 24 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Implementation of a simple database class."""

import random


class Database(object):

    """Class containing a database implementation."""
    database = []
    def __init__(self, db_file):
        self.db_file = db_file
        self.rand = random.Random()
        self.rand.seed()
        #
        # Your code here.
        #
        data = open(db_file,"r") 
        res = ""
        for i in data:
            if i == "%\n": 
                self.database.append(res)
                res = ""
                i = ""
            res = res + "\n" + i
        data.close()
        pass

    def read(self):
        """Read a random location in the database."""
        #
        # Your code here.
        #
        return self.rand.choice(self.database)
        pass

    def write(self, fortune):
        """Write a new fortune to the database."""
        #
        # Your code here.
        #
        fortune = fortune + '\n' + "%\n"
        self.database.append(fortune)
        data = open(self.db_file,"a")
        data.write(fortune)
        pass
