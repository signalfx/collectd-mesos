mesos-collectd-plugin
=====================

An [Apache Mesos](http://mesos.apache.org) plugin for [collectd](http://collectd.org) using collectd's [Python plugin](http://collectd.org/documentation/manpages/collectd-python.5.shtml).

This plugin is based heavily on the [Elasticsearch Collectd Python plugin](https://github.com/phobos182/collectd-elasticsearch) written by [phobos182](https://github.com/phobos182).

Stats:
 * Cluster status (Activated slaves, schedulers, num tasks)
 * Cluster stats (cpu, disk, memory)
 * Task stats (finished, lost, failed)
 * Many more...

Install
-------
 1. Place mesos.py in collectd'opt/collectd/lib/collectd/plugins/python (assuming you have collectd installed to /opt/collectd).
 2. Configure the plugin (see below).
 3. Restart collectd.

You can also install the plugin using this [Chef Cookbook](https://github.com/duedil-ltd/chef-collectd-mesos) if you're a Chef user.

Configuration
-------------
 * See mesos.conf

Requirements
------------
 * collectd 4.9+
 * Mesos 0.19.0 or greater
