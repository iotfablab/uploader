#!/usr/bin/env python3
import json
import time
import concurrent.futures
from influxdb import InfluxDBClient
import requests.packages.urllib3 as urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CONF_PATH = '/etc/umg/upload/conf.json'
CONF = dict()

STOP = False


with open(CONF_PATH) as cFile:
    CONF = json.load(cFile)

local_db_conf = CONF['local']
cloud_db_conf = CONF['cloud']

conf_keys = list(CONF.keys())
conf_keys.remove('local')
conf_keys.remove('cloud')
conf_keys.remove('batch_size')


LOCAL_DB = InfluxDBClient(host=local_db_conf['host'], port=local_db_conf['port'], database=local_db_conf['database'])
CLOUD_DB = InfluxDBClient(host=cloud_db_conf['host'], port=cloud_db_conf['port'], ssl=True, verify_ssl=False, username=cloud_db_conf['username'], password=cloud_db_conf['password'], database=cloud_db_conf['database'])


def connected_to_cloud():
    cloud_connected = False
    while not cloud_connected:
        try:
            cloud_version = CLOUD_DB.ping()
            print('Cloud Version %s' %cloud_version)
            cloud_connected = True
            return cloud_connected
        except Exception as e:
            print(e)
            time.sleep(60.0)

def query_and_upload(conf):
    while not STOP:
        print("For conf: {}".format(conf['measurement']))
        QUERY = 'SELECT "{}" FROM {} WHERE "status"=0 LIMIT {}'.format('","'.join(conf['fields']), conf['measurement'], conf['limit'])
        print(QUERY)
        query_results = LOCAL_DB.query(QUERY, epoch='ms')
        if len(list(query_results)) == 0:
            print('No Results')
        else:
            batch = []
            for points in list(query_results)[0]:
                
                upload_json_body = {'measurement': conf['measurement'], 'fields': {'status': 1.0}}
                if 'tags' in conf:
                    upload_json_body['tags'] = conf['tags']
                upload_json_body['time'] = points['time']
                # del points['time']
                for field in conf['fields']:
                    upload_json_body['fields'][field] = points[field]
                batch.append(upload_json_body)
                # print(batch)
            print('Length of Batch: {}'.format(len(batch)))
            for point in batch:
                for field in point['fields']:
                    if point['fields'][field] is not None:
                        if isinstance(point['fields'][field], int):
                            point['fields'][field] = float("%.3f" % point['fields'][field])
                        else:
                            point['fields'][field] = float(point['fields'][field])
                    else:
                        break
            if connected_to_cloud():
                print('Cloud Connected')
                try:
                    if CLOUD_DB.write_points(batch, time_precision='ms'):
                        print('UPLOAD DONE')
                        if LOCAL_DB.write_points(batch, time_precision='ms'):
                            print('LOCAL UPDATE DONE')
                        
                    else:
                        print('FAILED')
                        return 'Fail'
                except Exception as e:
                    print(e)
        time.sleep(1.0)

def main():
    
    try:
        _ = LOCAL_DB.ping()
    except Exception as e:
        raise(e)
    
    if connected_to_cloud():
       try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                future_to_upload = [executor.submit(query_and_upload, CONF[conf]) for conf in conf_keys]
                for future in concurrent.futures.as_completed(future_to_upload):
                    
                    try:
                        data = future.result()
                    except Exception as e:
                        print(e)
                    else:
                        print(data)
                while (not future.done() for future in future_to_upload):
                    time.sleep(1.0)
       except KeyboardInterrupt:
           global STOP
           STOP = True
           LOCAL_DB.close()
           CLOUD_DB.close()
            


if __name__ == "__main__":
    main()
