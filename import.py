#!/usr/bin/env python

# Run with no args for usage instructions
# Speed: ~ 1MB/s
# 

import sys
import MySQLdb
import csv
import ConfigParser, os

ConfigFile = ""
CSVFileName = ""
AllowedCSVColumns = []
ColumnAssociations = dict()

def main(config):
    #set global path for config, and csvfile
    global ConfigFile
    global CSVFileName
    ConfigFile = config
    CSVFileName = readconfig("CSV","file")

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

    MainMenu(cursor,table)
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

def GetCSVColumns():
    file_handle = open(CSVFileName)
    csvobj = csv.reader(file_handle)
    file_handle.seek(0)
    header = csvobj.next()
    returnStr = ""
    for x in header:
        returnStr = returnStr + x + ","
    #header is here, now return the string as it is.
    returnStr = returnStr.rstrip(",")
    return returnStr

def GetCSVColumnIndex(number=1):
    csvcols = GetCSVColumns()
    cols = csvcols.split(",")
    number = int(number) -1
    return cols[number]

def GetDBColumns(cursor,table):
    """
    generate the table columns string here, excluding the ones mentioned in the config file.
    """
    query = ("select column_name from information_schema.columns where table_name = '%s' order by ordinal_position" % table)
    cursor.execute(query)
    columns = cursor.fetchall()
    col_string = ""
    for x in columns:
        col_string = col_string + x[0] + ","

    col_string = col_string.rstrip(",")
    return col_string

def GetDBColumnIndex(cursor,table,number=1):
    #get the number of column. return text.
    cols = GetDBColumns(cursor,table)
    ret = cols.split(",")
    number = int(number) - 1
    return ret[number]

def GetSelectedDBColumns():
    col_list = ""
    for key,value in ColumnAssociations.items():
        if value != 0:
            col_list = col_list + key + ","
    col_list = col_list[:-1]
    return col_list

def MainMenu(cursor,table):
    #this is the main menu. Now generate the menu accordingly.
    print "\n\n\n\n\n\n\n\n\n"
    choice = 1
    while choice != 0:
        print "========================================"
        print "==================Menu=================="
        print "==============by geekrohit=============="
        print "========================================"
        print "= 1: Generate Associations"
        print "= 2: Start Importing"
        print "========================================"
        choice = int(raw_input("= Choice [1]:"))
        if choice != 0:
            if choice == 1:
                #now call MakeMenuDB.
                MakeMenuDB(cursor,table)
            elif choice == 2:
                #call the import functions
                loadcsv(cursor,table,CSVFileName)
    return

def MakeMenuDB(cursor,table):
    global ColumnAssociations
    choice = 1
    while choice != 0:
        print "\n\n\n\n\n\n\n\n"
        print "---- Database Tables:"
        cols = GetDBColumns(cursor,table)
        ret = cols.split(",")
        i = 1
    
        for x in ret:
            print i.__str__() + ": " + x
            i += 1
        #print "Choice: "
        choice = int(raw_input("Choice:"))
        print "\n\n\n\n\n\n\n\n\n\n\n\n\n\n"
        if choice != 0:
            test = GetDBColumnIndex(cursor,table,choice)
            test = str(test)
            #got the table column, now call csv column generator, and get its column as well.
            csvcol = MakeMenuCSV(test)

            ColumnAssociations[test] = csvcol
            PrintColumnsAssociation()
            #print assoc
            #print the associations now.
    #print getcsvcolnumber(choice)
    return 
    
def MakeMenuCSV(column):
    cols = GetCSVColumns()
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

def PrintColumnsAssociation():
    #print the current dictionary, and return.
    for key,value in ColumnAssociations.items():
        print "Db Column:", key, "\t CSV Column:", value
    return

def GetNumberOfFields():
    i = 0
    for key,value in ColumnAssociations.items():
        if value != 0:
            i += 1
    return i

def GenerateAllowedCSVColumns():
    #this function will generate the CSV columns once. So that they can be read easily during each line.
    #first clear the current list.
    global AllowedCSVColumns
    del AllowedCSVColumns[:]

    for key,value in ColumnAssociations.items():
        if value != 0:
            AllowedCSVColumns.append(value)
    return None

def sanitize(line):
    #first create a list of allowed
    #Accept the line, clean and reorder it as per the dictionary, and then return it to be added
    #to the sql query.
    new_list = []
    #just generate a new list containing newly ordered items
    for x in AllowedCSVColumns:
        new_list.append(line[x-1])
    
    return new_list

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
    GenerateAllowedCSVColumns()
    c = open(filename)
    f = csv.reader(c)
    c.seek(0)
    header = f.next()

    #numfields = len(header)

    query = buildInsertCmd(table)

    for line in f:
        vals = nullify(sanitize(line))
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
    cols = GetSelectedDBColumns()
    numfields = GetNumberOfFields()
    assert(numfields > 0)
    placeholders = (numfields-1) * "%s, " + "%s"
    query = ("insert into %s(%s)" % (table,cols)) + (" values (%s)" % placeholders)
    print "--------QUERY----------"
    print query
    print "-----------------------"
    return query


def readconfig(section,key):
    #now read configs

    configP = ConfigParser.ConfigParser()
    configP.readfp(open(ConfigFile))

    return configP.get(section,key)


if __name__ == '__main__':
    # commandline execution

    args = sys.argv[1:]
    if(len(args) < 1):
        print "error: arguments: config_file"
        sys.exit(1)

    main(*args)