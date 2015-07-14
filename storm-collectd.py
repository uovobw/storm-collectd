import collectd
import requests
import socket

HOSTNAME=socket.gethostname()
BASE_URL="http://localhost:8088"
PLUGIN_NAME = "storm"

ep = {"cluster" : BASE_URL+"/api/v1/cluster/summary",
      "supervisor" : BASE_URL+"/api/v1/supervisor/summary",
      "topology" : BASE_URL+"/api/v1/topology/summary",
      "topology_detail": BASE_URL+"/api/v1/topology/%s"}

def _parse_time(t):
    total_seconds = 0
    converter = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400
    }
    l = t.split(" ")[::-1]
    for p in l:
        value = int(p[:-1])
        kind = p[-1]
        total_seconds += value * converter[kind]
    return total_seconds

def _create_value(d, plugin_instance="", type_instance="", type="gauge", host=HOSTNAME, plugin=PLUGIN_NAME):
    v = collectd.Values(type=type, values=[int(d)], plugin_instance=plugin_instance, type_instance=type_instance, host=host, plugin=plugin)
    v.meta = {'0': True}  # HACK with this dummy dict in place JSON parsing works https://github.com/collectd/collectd/issues/716
    v.dispatch()

def _cluster_loader():
    resp = requests.get(ep["cluster"])
    if resp.status_code == 200:
        try:
            jdata = resp.json()
        except Exception as e:
            collectd.error("cannot parse storm data: %s" % e)
        _create_value(_parse_time(jdata["nimbusUptime"]), type_instance="nimbus_uptime")
        _create_value(jdata["supervisors"], type_instance="supervisors_count")
        _create_value(jdata["slotsTotal"], type_instance="slots_total")
        _create_value(jdata["slotsUsed"], type_instance="slots_used")
        _create_value(jdata["slotsFree"], type_instance="slots_free")
        _create_value(jdata["executorsTotal"], type_instance="executors_total")
        _create_value(jdata["tasksTotal"], type_instance="tasks_total")
    else:
        collectd.error("unable to fetch data for cluster")

def _supervisor_loader():
    resp = requests.get(ep["supervisor"])
    if resp.status_code == 200:
        try:
            jdata = resp.json()
        except Exception as e:
            collectd.error("cannot parse storm data: %s" % e)
            return
        for s in jdata["supervisors"]:
            plugin_instance = s["host"].split(".")[0]
            _create_value(s["slotsTotal"], type_instance="slots_total", plugin_instance=plugin_instance)
            _create_value(s["slotsUsed"], type_instance="slots_used", plugin_instance=plugin_instance)
            _create_value(_parse_time(s["uptime"]), type_instance="uptime", plugin_instance=plugin_instance)

    else:
        collectd.error("unable to fetch data for supervisors")

def _topology_loader():
    resp = requests.get(ep["topology"])
    if resp.status_code == 200:
        try:
            jdata = resp.json()
        except Exception as e:
            collectd.error("cannot parse storm data: %s" % e)
            return
        for t in jdata["topologies"]:
            plugin_instance = t["name"]
            _create_value(t["tasksTotal"], type_instance="tasks_total", plugin_instance=plugin_instance)
            _create_value(t["workersTotal"], type_instance="workers_total", plugin_instance=plugin_instance)
            _create_value(t["executorsTotal"], type_instance="executors_total", plugin_instance=plugin_instance)
            _create_value(_parse_time(t["uptime"]), type_instance="uptime", plugin_instance=plugin_instance)

    else:
        collectd.error("unable to fetch data for supervisors")

'''
def _topology_detail_loader(ids):
    for i in ids:
        t_id, plugin_instance = i
        resp = requests.get(ep["topology_detail"] % t_id)
        if resp.status_code == 200:
            try:
                jdata = json.loads(resp.text)
            except Exception as e:
                collectd.error("cannot parse storm data: %s" % e)
                return
            # bolts data
            for b in jdata["bolts"]:
                _create_value(b["executors"], type_instance="tasks_total", plugin_instance=plugin_instance)

        else:
            collectd.error("cannot get topology details for topology %s (id: %s)" % (plugin_instance, t_id))
'''

def read_callback():
    _cluster_loader()
    _supervisor_loader()
    _topology_loader()

collectd.register_read(read_callback)
