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
        data = open(db_file,"r+") 
        #database = []
        for i in data:
            self.database.append(i)
            #print(i)
        data.close()
        self.rand.randrange(0,len(self.database)-1)
        pass

    def read(self):
        """Read a random location in the database."""
        #
        # Your code here.
        #
        print(self.rand.choice(self.database))
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
        self.rand.randrange(0,len(self.database)-1)
        pass
