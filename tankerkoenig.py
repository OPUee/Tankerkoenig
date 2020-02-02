#!python

import sys
import sqlite3
import toml
import requests as req
from time import time

DATABASE_NAME = 'tankerkoenig.db'

# create database:
# ------------------------
# CREATE TABLE gasstation (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255), brand VARCHAR(255), address VARCHAR(512))
# CREATE TABLE price (id INTEGER PRIMARY KEY AUTOINCREMENT, gasstation_id INTEGER, value FLOAT, timestamp INTEGER)


def write_data(data):

    def read_gasstations(conn):
        c = conn.execute('SELECT id FROM gasstation')
        return [a[0] for a in c.fetchall()]

    def write_gasstation(conn, station):
        conn.execute('INSERT INTO gasstation VALUES (?,?,?,?)', station)

    def write_price(conn, price):
        conn.executemany('INSERT INTO price (gasstation_id, value, timestamp) VALUES (?,?,?)', price)


    conn = sqlite3.connect(DATABASE_NAME)

    timestamp = int(time())
    price_list = []
    station_list = read_gasstations(conn)

    for s in data['stations']:
        id = s['id']
        name = s['name']
        brand = s['brand']
        price = s['price']
        address = '{} {}, {} {}'.format(s['street'],
                                        s['houseNumber'],
                                        s['postCode'],
                                        s['place'])

        print('writing data:')
        print(id)
        print(name)
        print(brand)
        print(address)

        if id not in station_list:
            write_gasstation(conn, (id, name, brand, address))

        price_list.append((id, price, timestamp))

    write_price(conn, price_list)

    conn.commit()
    conn.close()


def get_data():
    r = req.get(URL.format(LAT, LNG, RAD, SORT, TYPE, API_KEY))
    print(r.status_code)
    return r.json()



def main():

    data = get_data()
    write_data(data)


if __name__ == '__main__':
    main()
