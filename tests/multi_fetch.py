#!/usr/bin/env python
import time
import requests
from pprint import pprint,pformat
from pandas import *

class tmp_node_obj():
    def __init__(self, path):
        self.path = path

class log:
    @classmethod
    def info(self,msg):
        print "%s [debug] %s\n" % (time.ctime(), msg)

def multi_fetch():
    log.info("running multi_fetch")
    start_t_g = time.time()
    start_time = 1426590825
    end_time = 1429182825
    #start_time = 1429136880
    #end_time = 1429136880+600
    data, time_step = get_multi_data()
    # fullfill data fetched from storages to fit timestamps 
    result = []
    start_t = time.time()
    time_info = start_time, end_time, time_step
    for path in data.keys():
        #start_t = time.time()
        # fill output with nans when there is no datapoints
        filled_data = get_filled_data(data[path], start_time, end_time, time_step)
        sorted_data = [ filled_data[i] for i in sorted(filled_data.keys()) ]
        #print "filled data in %.5f" % (time.time() - start_t)
        # try reindexing with pandas
        #start_t = time.time()
	#ts = Series(data[path])
        #ts = ts.reindex(index=xrange(start_time, end_time, time_step))
        #sorted_data = ts.values
	#sorted_data = ts.where((notnull(sorted_data)), None)
        #print "reindexed data in %.5f" % (time.time() - start_t)
        #result.append((tmp_node_obj(path), (time_info, sorted_data.tolist())))
        result.append((tmp_node_obj(path), (time_info, sorted_data)))
    log.info("RENDER:Timings:get_data_fill %.5f" % (time.time() - start_t))
    log.info("RENDER:Timings:get_data_all %.5f" % (time.time() - start_t_g)) 
    return result

def get_multi_data():
    time_step = 60
    num = 111
    query = "SELECT Path, intDiv(toUInt32(Time), 60) * 60, Value FROM graphite_d WHERE Path IN ( 'one_min.classic_perf_11.test71.m725', 'one_min.classic_perf_11.test71.m759', 'one_min.classic_perf_11.test71.m726', 'one_min.classic_perf_11.test71.m708', 'one_min.classic_perf_11.test71.m79', 'one_min.classic_perf_11.test71.m716', 'one_min.classic_perf_11.test71.m752', 'one_min.classic_perf_11.test71.m734', 'one_min.classic_perf_11.test71.m727', 'one_min.classic_perf_11.test71.m70', 'one_min.classic_perf_11.test71.m782', 'one_min.classic_perf_11.test71.m745', 'one_min.classic_perf_11.test71.m717', 'one_min.classic_perf_11.test71.m785', 'one_min.classic_perf_11.test71.m71', 'one_min.classic_perf_11.test71.m746', 'one_min.classic_perf_11.test71.m747', 'one_min.classic_perf_11.test71.m710', 'one_min.classic_perf_11.test71.m718', 'one_min.classic_perf_11.test71.m776', 'one_min.classic_perf_11.test71.m737', 'one_min.classic_perf_11.test71.m750', 'one_min.classic_perf_11.test71.m791', 'one_min.classic_perf_11.test71.m787', 'one_min.classic_perf_11.test71.m706', 'one_min.classic_perf_11.test71.m720', 'one_min.classic_perf_11.test71.m765', 'one_min.classic_perf_11.test71.m760', 'one_min.classic_perf_11.test71.m753', 'one_min.classic_perf_11.test71.m715', 'one_min.classic_perf_11.test71.m792', 'one_min.classic_perf_11.test71.m761', 'one_min.classic_perf_11.test71.m756', 'one_min.classic_perf_11.test71.m713', 'one_min.classic_perf_11.test71.m732', 'one_min.classic_perf_11.test71.m741', 'one_min.classic_perf_11.test71.m78', 'one_min.classic_perf_11.test71.m754', 'one_min.classic_perf_11.test71.m755', 'one_min.classic_perf_11.test71.m767', 'one_min.classic_perf_11.test71.m762', 'one_min.classic_perf_11.test71.m766', 'one_min.classic_perf_11.test71.m77', 'one_min.classic_perf_11.test71.m712', 'one_min.classic_perf_11.test71.m76', 'one_min.classic_perf_11.test71.m793', 'one_min.classic_perf_11.test71.m783', 'one_min.classic_perf_11.test71.m721', 'one_min.classic_perf_11.test71.m75', 'one_min.classic_perf_11.test71.m731', 'one_min.classic_perf_11.test71.m796', 'one_min.classic_perf_11.test71.m751', 'one_min.classic_perf_11.test71.m739', 'one_min.classic_perf_11.test71.m742', 'one_min.classic_perf_11.test71.m74', 'one_min.classic_perf_11.test71.m773', 'one_min.classic_perf_11.test71.m764', 'one_min.classic_perf_11.test71.m768', 'one_min.classic_perf_11.test71.m777', 'one_min.classic_perf_11.test71.m700', 'one_min.classic_perf_11.test71.m719', 'one_min.classic_perf_11.test71.m757', 'one_min.classic_perf_11.test71.m798', 'one_min.classic_perf_11.test71.m771', 'one_min.classic_perf_11.test71.m749', 'one_min.classic_perf_11.test71.m786', 'one_min.classic_perf_11.test71.m73', 'one_min.classic_perf_11.test71.m740', 'one_min.classic_perf_11.test71.m7', 'one_min.classic_perf_11.test71.m769', 'one_min.classic_perf_11.test71.m733', 'one_min.classic_perf_11.test71.m711', 'one_min.classic_perf_11.test71.m729', 'one_min.classic_perf_11.test71.m774', 'one_min.classic_perf_11.test71.m797', 'one_min.classic_perf_11.test71.m770', 'one_min.classic_perf_11.test71.m703', 'one_min.classic_perf_11.test71.m724', 'one_min.classic_perf_11.test71.m780', 'one_min.classic_perf_11.test71.m799', 'one_min.classic_perf_11.test71.m775', 'one_min.classic_perf_11.test71.m728', 'one_min.classic_perf_11.test71.m735', 'one_min.classic_perf_11.test71.m743', 'one_min.classic_perf_11.test71.m784', 'one_min.classic_perf_11.test71.m723', 'one_min.classic_perf_11.test71.m709', 'one_min.classic_perf_11.test71.m790', 'one_min.classic_perf_11.test71.m748', 'one_min.classic_perf_11.test71.m714', 'one_min.classic_perf_11.test71.m772', 'one_min.classic_perf_11.test71.m795', 'one_min.classic_perf_11.test71.m744', 'one_min.classic_perf_11.test71.m794', 'one_min.classic_perf_11.test71.m722', 'one_min.classic_perf_11.test71.m707', 'one_min.classic_perf_11.test71.m72', 'one_min.classic_perf_11.test71.m704', 'one_min.classic_perf_11.test71.m758', 'one_min.classic_perf_11.test71.m788', 'one_min.classic_perf_11.test71.m789', 'one_min.classic_perf_11.test71.m763', 'one_min.classic_perf_11.test71.m779', 'one_min.classic_perf_11.test71.m730', 'one_min.classic_perf_11.test71.m702', 'one_min.classic_perf_11.test71.m736', 'one_min.classic_perf_11.test71.m738', 'one_min.classic_perf_11.test71.m701', 'one_min.classic_perf_11.test71.m778', 'one_min.classic_perf_11.test71.m705', 'one_min.classic_perf_11.test71.m781' )                        AND Time > 1426590825 AND Time < 1429182825 AND Date >= toDate(toDateTime(1426590825)) AND Date <= toDate(toDateTime(1429182825)) ORDER BY Time, Timestamp"
    #query = "SELECT Path, intDiv(toUInt32(Time),60)*60, Value FROM graphite_d WHERE Path IN ( 'one_min.classic_perf_11.test71.m70')                        AND Time > 1429136880 AND Time < 1429137480 AND Date >= toDate(toDateTime(1429136880)) AND Date <= toDate(toDateTime(1429137480)) ORDER BY Time, Timestamp"
    # query_hash now have only one storage beceause clickhouse has distributed table engine
    start_t = time.time()

    url = "http://bsgraphite-load01i.yandex.net:8123"
    data = {}
    dps = requests.post(url, query).text
    log.info("RENDER:get_data_fetch %.5f" % (time.time() - start_t))
    start_t = time.time()

    if len(dps) == 0:
        log.info("WARN: empty response from db, nothing to do here")
   
    # fill values array to fit (end_time - start_time)/time_step
    for dp in dps.split("\n"):
        dp = dp.strip()
        if len(dp) == 0:
            continue
        arr = dp.split("\t")
        # and now we have 3 field insted of two, first field is path
        path = arr[0].strip()
        dp_ts = int(arr[1].strip())
        dp_val = arr[2].strip()
        data.setdefault(path, {})[dp_ts] = float(dp_val)
    log.info("RENDER:get_data_parse %.5f" % (time.time() - start_t))
    return data, time_step

def get_filled_data(data, stime, etime, step):
    # some stat about how datapoint manage to fit timestamp map 
    ts_hit = 0
    ts_miss = 0
    ts_fail = 0
    start_t = time.time() # for debugging timeouts
    stime = stime - (stime % step)
    data_ts_min = int(min(data.keys()))
    data_stime = data_ts_min - (data_ts_min % step)
    filled_data = {}

    data_keys = sorted(data.keys())
    data_index = 0
    p_start_t_g = time.time()
    search_time = 0
    for ts in xrange(stime, etime, step):
        if ts < int(data_stime):
            # we have no data for this timestamp, nothing to do here
            filled_data[ts] = None
            ts_fail += 1
            continue

#        ts = unicode(ts)
        if data.has_key(ts):
            filled_data[ts] = data[ts]
            data_index += 1
            ts_hit += 1
            continue
        else:
            p_start_t = time.time()
            for i in xrange(data_index, len(data_keys)):
                ts_tmp = int(data_keys[i])
                if ts_tmp >= int(ts) and (ts_tmp - int(ts)) < step:
                    filled_data[int(ts)] = data[data_keys[data_index]]
                    data_index += 1
                    ts_miss += 1
                    break
                elif ts_tmp < int(ts):
                    data_index += 1
                    continue
                elif ts_tmp > int(ts):
                    ts_fail += 1
                    filled_data[int(ts)] = None
                    break
            search_time += time.time() - p_start_t
        # loop didn't break on continue statements, set it default NaN value
        if not filled_data.has_key(ts):
            ts_fail += 1
            filled_data[ts] = None
#    log.info("DEBUG:OPT: loop in %.3f, search in %.3f" % ((time.time() - start_t), search_time))
#    log.info("DEBUG:OPT: filled data in %.3f" % (time.time() - start_t))
    log.info("DEBUG: hit %d, miss %d, fail %d" % (ts_hit, ts_miss, ts_fail))
    return filled_data


if __name__ == "__main__":
    data = multi_fetch()
    print "Done"
    #print "Got result, size \n%s" % pformat(data)
