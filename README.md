# ADS-B to InfluxDB

This Python script can read data from [dump1090](https://github.com/antirez/dump1090)
in SBS1 (BaseStation) format, process it and then store them in
[InfluxDB](https://github.com/influxdata/influxdb).

Example usage:

```
$ python3 adsb2influx.py -ds 192.168.2.11 -dp 30003 -si 60 -iu http://localhost:8086 -db adsb
```

where

- `-ds 192.168.2.11`: IP address where dump1090 is running,
- `dp 30003`: TCP port of dump1090 SBS1 interface.
- `-iu http://localhost:8086`: URL of InfluxDB,
- `-si 60`: how often to write data to database (60 seconds).
- `-db adsb`: name of InfluxDB database.

## Preparing InfluxDB

1. Install and run InfluxDB.
2. Create new InfluxDB database via CLI: `CREATE DATABASE adsb WITH DURATION 7d`.

The retention policy can be set to your needs. The example uses 7 days.

## Preparing dump1090

There are no special steps. Just make sure your `dump1090` instance listens on
TCP port 30003.

Credits
-------

Big thanks to [mikenye](https://github.com/mikenye) for his
[docker-piaware-to-influx](https://github.com/mikenye/docker-piaware-to-influx)
project. This code started as minor tweaks of his work but was completely rewritten
during the process :)
