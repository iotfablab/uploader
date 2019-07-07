import sys
import json
import pprint
import time
import logging

from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError
from requests import ConnectionError
# import urllib3
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler("/var/log/uploader.log")
handler.setLevel(logging.ERROR)

formatter = logging.Formatter('%(asctime)s-%(name)s-%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


CONF_PATH = '/etc/umg/upload/conf.json'
CONF = dict()
LOCAL_DB = None
CLOUD_DB = None
BATCH = []
BATCH_SIZE = 0

def read_conf_file(path):
    with open(path) as cFile:
        _conf = json.load(cFile)
    return _conf


def upload_batch(datapoints):
    for point in datapoints:
            for field in point['fields']:
                if point['measurement'] != 'button':
                    point['fields'][field] = float(point['fields'][field])
                else:
                    point['fields'][field] = int(point['fields'][field])
            point['fields']['status'] = int(1)
    try:
        logger.info('sending data to cloud')
        if connected_to_cloud():
            if CLOUD_DB.write_points(datapoints, time_precision='ms'):
                logger.info('UPLOAD Completed')

                try:
                    logger.info('re-writing batch to Local InfluxDB')
                    if LOCAL_DB.write_points(datapoints, time_precision='ms'):
                        logger.info('LOCAL DB Updated with status field = 1')
                        global BATCH
                        BATCH = []
                except InfluxDBClientError as e:
                    logger.exception('Exception during re-writing batch to Local InfluxDB')
                    logger.exception(e)
            else:
                logger.error('UPLOAD Failed')
    except Exception as e:
        logger.exception('exception during complete upload_batch function')
        logger.exception(e)
        pass

def get_points(conf):
    QUERY = 'SELECT "{}" FROM {} WHERE "status"=0 LIMIT {}'.format(
        '","'.join(conf['fields']),
        conf['measurement'],
        conf['limit'])
    logger.debug(QUERY)
    global LOCAL_DB
    try:
        query_results = LOCAL_DB.query(QUERY, epoch='ms')
    except InfluxDBClientError as e:
        logger.exception('exception during querying of: {}'.format(conf['measurement']))
        logger.exception(e)
        pass
    if len(list(query_results)) == 0:
        logger.info('No Results for QUERY')
    else:
        global BATCH
        for points in list(query_results)[0]:
            upload_json_body = {
            'measurement': conf['measurement'],
            'fields': {}
            }
            if 'tags' in conf:
                upload_json_body['tags'] = conf['tags']
            upload_json_body['time'] = points['time']
            upload_json_body['fields'] = points
            del points['time']
            BATCH.append(upload_json_body)
        logger.debug('current batch length: {}'.format(len(BATCH)))
        global BATCH_SIZE
        if len(BATCH) >= BATCH_SIZE:
            logger.info('Uploading Batch Now since limit reached')
            upload_batch(BATCH)

def connected_to_cloud():
    cloud_connected = False
    while not cloud_connected:
        try:
            cloud_version = CLOUD_DB.ping()
            logger.debug('connected to InfluxDB v: {}'.format(cloud_version))
            cloud_connected = True
            return cloud_connected
        except ConnectionError as e:
            logger.info('cannot connect to Cloud Server')
            logger.exception(e)
            logger.info('Trying again after 1.0 Minute')
            time.sleep(60.0)

def main():
    logger.info('reading Configuration file')
    global CONF
    CONF = read_conf_file(CONF_PATH)
    logger.debug('Config File: {}'.format(CONF))
    global BATCH_SIZE
    BATCH_SIZE = CONF['batch_size']
    local_hostname = CONF['local']['host']
    local_port = CONF['local']['port']
    local_db = CONF['local']['database']
    
    cloud_hostname = CONF['cloud']['host']
    cloud_port = CONF['cloud']['port']
    cloud_db = CONF['cloud']['database']
    cloud_username = CONF['cloud']['username']
    cloud_password = CONF['cloud']['password']

    global LOCAL_DB
    LOCAL_DB = InfluxDBClient(host=local_hostname, port=local_port, database=local_db)
    
    global CLOUD_DB
    CLOUD_DB = InfluxDBClient(host=cloud_hostname,
        port=cloud_port, 
        ssl=True,
        verify_ssl=False,
        username=cloud_username,
        password=cloud_password,
        database=cloud_db)
    
    try:
        _ = LOCAL_DB.ping()
    
    except ConnectionError as e:
        logger.exception('Cannot connect to Local Server')
        raise(e)

    if connected_to_cloud():
        try:
            while True:
                for config in CONF.keys():
                    if (config != 'local') and (config != 'cloud') and (config != 'batch_size'):
                        print(config)
                        get_points(CONF[config])
                time.sleep(5.0)
        except KeyboardInterrupt:
            LOCAL_DB.close()
            CLOUD_DB.close()
            sys.exit(0)

if __name__ == "__main__":
    main()