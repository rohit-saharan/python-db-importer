#!/usr/bin/env python

# Run with no args for usage instructions
# Speed: ~ 1MB/s
# 

import sys
import MySQLdb
import csv
import ConfigParser, os

config_file = ""
csvfilename = ""
assoc = dict()

def main(csvfile, config):
    #set global path for config, and csvfile
    global config_file
    global csvfilename
    csvfilename = csvfile
    config_file = config

    #load configurations
    db = readconfig("Db","db")
    user = readconfig("Db","user")
    table = readconfig("Db","table")
    passwd = readconfig("Db","pass")

    try:
        conn = getconn(user, db,passwd)
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit (1)

    cursor = conn.cursor()

    makemenucols(cursor,table)
    buildInsertCmd(table)

    #loadcsv(cursor, table, csvfile)

    cursor.close()
    conn.close()

def getconn(user, db, passwd=""):
    conn = MySQLdb.connect(host = "localhost",
                           user = user,
                           passwd = passwd,
                           db = db)
    return conn

def getcsvcolumns():
    file_handle = open(csvfilename)
    csvobj = csv.reader(file_handle)
    file_handle.seek(0)
    header = csvobj.next()
    returnStr = ""
    for x in header:
        returnStr = returnStr + x + ","
    #header is here, now return the string as it is.
    returnStr = returnStr.rstrip(",")
    return returnStr

def getcsvcolnumber(number=1):
    csvcols = getcsvcolumns()
    cols = csvcols.split(",")
    number = int(number) -1
    return cols[number]

def getcolumns(cursor,table):
    """
    generate the table columns string here, excluding the ones mentioned in the config file.
    """
    query = ("select column_name from information_schema.columns where table_name = '%s' order by ordinal_position" % table)
    cursor.execute(query)
    columns = cursor.fetchall()
    col_string = ""
    exclude = readconfig("Table","exclude")
    for x in columns:
        if x[0] != exclude :
            col_string = col_string + x[0] + ","

    col_string = col_string.rstrip(",")
    return col_string

def getcolumnnumber(cursor,table,number=1):
    #get the number of column. return text.
    cols = getcolumns(cursor,table)
    ret = cols.split(",")
    number = int(number) - 1
    return ret[number]

def getselectedcolumns():
    col_list = ""
    for key,value in assoc.items():
        if value != 0:
            col_list = col_list + key + ","
    col_list = col_list[:-1]
    return col_list

def makemenucols(cursor,table):
    global assoc
    choice = 1
    while choice != 0:
        print "\n\n\n\n\n\n\n\n"
        print "---- Database Tables:"
        cols = getcolumns(cursor,table)
        ret = cols.split(",")
        i = 1
    
        for x in ret:
            print i.__str__() + ": " + x
            i += 1
        #print "Choice: "
        choice = int(raw_input("Choice:"))
        print "\n\n\n\n\n\n\n\n\n\n\n\n\n\n"
        if choice != 0:
            test = getcolumnnumber(cursor,table,choice)
            test = str(test)
            #got the table column, now call csv column generator, and get its column as well.
            csvcol = makemenucsv(test)

            assoc[test] = csvcol
            printassoc()
            #print assoc
            #print the associations now.
    #print getcsvcolnumber(choice)
    return 
    
def makemenucsv(column):
    cols = getcsvcolumns()
    ret_cols = cols.split(",")
    i = 1
    for x in ret_cols:
        print i,":",x
        i += 1
    print "Corresponding Column for ",column, ":"
    
    choice = int(raw_input())
    #get_col_name = getcsvcolnumber(choice)
    get_col_name = choice

    return get_col_name

def printassoc():
    #print the current dictionary, and return.
    for key,value in assoc.items():
        print "Db Column:", key, "\t CSV Column:", value
    return

def numberoffields():
    i = 0
    for key,value in assoc.items():
        if value != 0:
            i += 1
    return i


def readconfig(section,key):
    #now read configs
    
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_file))

    return config.get(section,key)
def nullify(L):
    """Convert empty strings in the given list to None."""

    # helper function
    def f(x):
        if(x == ""):
            return None
        else:
            return x
        
    return [f(x) for x in L]

def loadcsv(cursor, table, filename):

    """
    Open a csv file and load it into a sql table.
    Assumptions:
     - the first line in the file is a header
    """
    c = open(filename)
    f = csv.reader(c)
    c.seek(0)
    header = f.next()

    #numfields = len(header)

    query = buildInsertCmd(table)

    for line in f:
        vals = nullify(line)
        cursor.execute(query, vals)

    return

def buildInsertCmd(table):

    """
    Create a query string with the given table name and the right
    number of format placeholders.

    example:
    >>> buildInsertCmd("foo", 3)
    'insert into foo values (%s, %s, %s)' 
    """
    cols = getselectedcolumns()
    numfields = numberoffields()
    assert(numfields > 0)
    placeholders = (numfields-1) * "%s, " + "%s"
    query = ("insert into %s(%s)" % (table,cols)) + (" values (%s)" % placeholders)
    print "--------QUERY----------"
    print query
    print "-----------------------"
    return query

if __name__ == '__main__':
    # commandline execution

    args = sys.argv[1:]
    if(len(args) < 1):
        print "error: arguments: csvfile config_file"
        sys.exit(1)

    main(*args)