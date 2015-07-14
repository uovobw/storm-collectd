# storm-collectd

A (vey basic) python plugin for collectd to gather basic storm metrics from the UI

Requirements
============

The plugin uses the [requests](http://docs.python-requests.org/en/latest/) library to access the
ui API. You can install it on Debian with

> apt-get install python-requests

or via pip (preferred) 

> pip install requests

Installation
============

The storm-collectd.py file is to be installed under the `/usr/share/collectd/python` directory,
the storm-collectd.conf file in `/etc/collectd/collectd.conf.d` directory

TODO
====

- be able to poll a UI that is not living on the same node as the collectd daemon
- clean up the code
- add the topology detail values with bolts, spouts and general data
- add configuration callback to toggle in-depth analisys
