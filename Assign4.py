import math
import time
import decimal
import MySQLdb
import redis
from flask_table import Table, Col
from geopy.distance import vincenty
import boto.rds
import os
from flask import Flask, Response, request
app = Flask(__name__)

conn = boto.rds.connect_to_region("us-west-2", aws_access_key_id='AKIAJOODXROHPGP3TLIA', aws_secret_access_key='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')

dbinstances = conn.get_all_dbinstances()
db = dbinstances[0]
host_name = db.endpoint[0]
port = db.endpoint[1]
mydb = MySQLdb.connect(host=host_name, port=port, user="rohit_user", passwd="rohit5620", db="rohit_db")
cursor = mydb.cursor()

r = redis.StrictRedis(host='rohit-cluster.hm0ycc.0001.usw2.cache.amazonaws.com', port=6379, db=0)



def dis_to_lat_long(dis, lat):
    global change_in_latitude
    global change_in_longitude
    e_radius = 3960.0
    deg_to_rad = math.pi/180.0
    rad_to_deg = 180.0/math.pi
    change_in_latitude = (dis/e_radius)*rad_to_deg
    radius = e_radius*math.cos(lat*deg_to_rad)
    change_in_longitude = (dis/radius)*rad_to_deg

@app.route('/')
def welcome():
    return app.send_static_file('index.html')


@app.route('/find', methods=['GET', 'POST'])
def find():
    global r
    global change_in_latitude
    global change_in_longitude

    if request.method == 'POST':
        start_time = time.time()
        cityname = request.form['cityname']
        option = request.form['option']

        if option == 'bynumber':
            noc = int(request.form['noc'])
            #making table
            class ItemTable(Table):
                rank = Col('Rank')
                city = Col('City')
                distance = Col('Distance')
            class Item(object):
                def __init__(self, rank, city, distance):
                    self.rank = rank
                    self.city = city
                    self.distance = distance
            if r.exists(cityname) and r.zcard(cityname) >= noc:
                items = []
                for i in range(0, noc):
                    ncity=r.zrange(cityname, i, i)
                    doc = dict(rank=str(i+1), city=ncity[0], distance=str(r.zscore(cityname, ncity[0])))
                    items.append(doc)
                table = ItemTable(items)
                passtable = (table.__html__())
                print("--- %s seconds ---" % (time.time() - start_time))
                return passtable
            else:
                ##query and logic
                cursor.execute("Select lat, lng from cities where city = %s ", [cityname])
                for row in cursor:
                    lat = float(row[0])
                    lng = float(row[1])
                dis = 5.0
                dis_to_lat_long(dis, lat)
                while True:
                    lat_range_start = lat-change_in_latitude
                    lng_range_start = lng-change_in_longitude
                    lat_range_end = lat+change_in_latitude
                    lng_range_end = lng+change_in_longitude
                    cursor.execute("Select count(*) from cities where lat between %s and %s and lng between %s and %s", [lat_range_start,lat_range_end,lng_range_start,lng_range_end])
                    for row in cursor:
                        citicount = row[0]
                    if citicount <= noc:
                        dis = dis + 5.0
                        dis_to_lat_long(dis, lat)
                    else:
                        break
                cursor.execute("Select city, lat, lng from cities where lat between %s and %s and lng between %s and %s", [lat_range_start,lat_range_end,lng_range_start,lng_range_end])
                cities = []
                for row in cursor:
                    distance = vincenty((lat, lng), (row[1], row[2])).miles
                    cities.append((row[0], distance))
                cities = sorted(cities, key=lambda cities: cities[1])
                items = []
                for i in range(1, noc+1):
                    r.zadd(str(cityname), cities[i][1], cities[i][0])
                for i in range(1, noc+1):
                    doc = dict(rank=str(i), city=str(cities[i][0]), distance=str(cities[i][1])+' miles')
                    items.append(doc)
                table = ItemTable(items)
                passtable = (table.__html__())
                print("--- %s seconds ---" % (time.time() - start_time))
                return passtable

        elif option == 'bydistance':
            dis = float(request.form['dis'])
            class ItemTable(Table):
                rank = Col('Rank')
                city = Col('City')
                distance = Col('Distance')
            class Item(object):
                def __init__(self, rank, city, distance):
                    self.rank = rank
                    self.city = city
                    self.distance = distance
            if r.exists(cityname):
                lastcity = r.zrange(cityname, -1, -1)
                lastcitydis = float(r.zscore(cityname, lastcity[0]))
            if r.exists(cityname) and lastcitydis >= dis:
                items = []
                i = 0
                while True:
                    try:
                        ncity=r.zrange(cityname, i, i)
                        if float(r.zscore(cityname, ncity[0])) <= dis:
                            doc = dict(rank=str(i+1), city=ncity[0], distance=str(r.zscore(cityname, ncity[0])))
                            items.append(doc)
                            i = i + 1
                        else:
                            break
                    except:
                        break
                table = ItemTable(items)
                passtable = (table.__html__())
                print("--- %s seconds ---" % (time.time() - start_time))
                return passtable
            else:
                #query and logic
                cursor.execute("Select lat, lng from cities where city = %s ", [cityname])
                for row in cursor:
                    lat = float(row[0])
                    lng = float(row[1])
                dis_to_lat_long(dis, lat)
                lat_range_start = lat-change_in_latitude
                lng_range_start = lng-change_in_longitude
                lat_range_end = lat+change_in_latitude
                lng_range_end = lng+change_in_longitude
                cursor.execute("Select city, lat, lng from cities where lat between %s and %s and lng between %s and %s", [lat_range_start,lat_range_end,lng_range_start,lng_range_end])
                cities = []
                for row in cursor:
                    distance = vincenty((lat, lng), (row[1], row[2])).miles
                    cities.append((row[0], distance))
                cities = sorted(cities, key=lambda cities: cities[1])
                items = []
                k = 1
                while True:
                    try:
                        if cities[k][1] is not None:
                            k = k + 1
                    except:
                        break
                for i in range(1, k):
                    r.zadd(str(cityname), cities[i][1], cities[i][0])
                i = 1
                while True:
                    try:
                        if cities[i][1] <= dis:
                            doc = dict(rank=str(i), city=str(cities[i][0]), distance=str(cities[i][1])+' miles')
                            items.append(doc)
                            i = i + 1
                        else:
                            break
                    except:
                        break
                table = ItemTable(items)
                passtable = (table.__html__())
                print("--- %s seconds ---" % (time.time() - start_time))
                return passtable


port = os.getenv('VCAP_APP_PORT', '5002')
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(port), debug=True)