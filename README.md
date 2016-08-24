collectd-mesos
==============

A [collectd](http://collectd.org) plugin for [Apache Mesos](http://mesos.apache.org), using collectd's [Python plugin](http://collectd.org/documentation/manpages/collectd-python.5.shtml).

This plugin is forked from [mesos-collectd-plugin](https://github.com/rayrod2030/collectd-mesos), written by [rayrod2030](https://github.com/rayrod2030).

Statistics:
 * Cluster status (activated slaves, schedulers, number of tasks)
 * Cluster statistics (CPU, disk, memory)
 * Task statistics (finished, lost, failed)
 * Many more...

Install
-------
 1. Download the plugin.
 2. Configure the plugin (see below for details).
 3. Restart collectd.

Configuration
-------------
 * In the appropriate plugin configuration file (master or slave), change ModulePath to the location where the plugin was downloaded.
 * Change Cluster to a name for the Mesos cluster.
 * Change Instance to a name that will identify the plugin instance.
 * Change Path to the location of the mesos-master or mesos-slave binary.
 * Set the Host and Port values.
 * Place the configuration file in a location that collectd is aware of.

See [10-mesos-master.conf](https://github.com/signalfx/integrations/blob/master/collectd-mesos/10-mesos-master.conf) and [10-mesos-slave.conf](https://github.com/signalfx/integrations/blob/master/collectd-mesos/10-mesos-slave.conf) as examples.

Requirements
------------
 * collectd 4.9 or greater
 * Mesos 0.19.0 or greater
