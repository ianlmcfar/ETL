from modules import connectionclients
from modules import configparser as config
import csv
import datetime
import decimal
import sys
import os


def get_products(connection):
    content = connection.execute(config.getsettings('SQL')['skulist'])
    with open ('/var/local/dev/pythondev/data/product_csv/catalog_products.csv','w') as out:
        csv_file = csv.writer(out)
        for row in content[0]:
            csv_file.writerow([str(row)[1:-1].strip(",").strip("'")])
        print 'Product list written to CSV'
    connection.close()
