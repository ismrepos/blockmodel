import sys
import os
import io
import csv
import mysql.connector as mariadb
from multiprocessing import Pool
from util.util import cout, with_stacktrace

#/data/WoS/17_01/parsed_WR_201[0-4]*.gz.csv

mariadb_connection = mariadb.connect(user='blockmodel', password='blockmodel', database='blockmodel')
cursor = mariadb_connection.cursor()

#
# Worker
#
@with_stacktrace
def work(path):
    counter = 0

    csv.field_size_limit(1000000000)

    cout('Reading: %s' % path)
    with open(path, 'r') as f_input:

        reader = csv.reader(f_input)
        for row in reader:
            name = str(row[0])
            num = 1

            try:

                sql = "INSERT INTO test2 (name, num) VALUES (%s, %s)"
                r = cursor.execute(sql, (name, num))

                #cursor.execute(sql)
                mariadb_connection.commit()

            except Exception as e:
                #print('Error: %s' % e)
                sql = "UPDATE test2 SET num = num + 1 WHERE name = '"+name.replace('\'' ,' ')+"'"
                r = cursor.execute(sql)
                mariadb_connection.commit()

    cursor.close()
    mariadb_connection.close()

    return counter

#
# Main function
#
def main():

    # parse args
    if len(sys.argv) < 2:
        print(USAGE)
        sys.exit(1)


    pool = Pool()
    p = pool.map_async(work, sys.argv[1:])
    p.get(86400)
    return 0

    print('hello')

    return 0

if __name__ == '__main__':
    main()
