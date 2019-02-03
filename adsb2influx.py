import signal
import socket
import time
import calendar
import argparse
import re
import logging
import requests

################################################################################
# Global Variables
################################################################################

run_app = True

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger('adsb2influx')

################################################################################
# Classes
################################################################################


class AdsbError(Exception):
    pass


class AdsbProcessor(object):
    '''Parse and save ADS-B messages.

    http://woodair.net/sbs/article/barebones42_socket_data.htm

    transmission = transmision type, MSG sub types 1 to 8.
    session = session id, database Session record number.
    aircraft = aircraft id, database Aircraft record number.
    hexident = hexident, aircraft Mode S hexadecimal code.
    flight = flight id, database Flight record number.
    gen_date =  date message generated.
    gen_time = time message generated.
    log_date = date message logged.
    log_time = time message logged.

    callsign = 8 digit flight ID, can be flight number or registration (or none).
    altitude = mode C altitude, height relative to 1013.2mb (flight Level).
    speed = speed over ground (not indicated airspeed).
    track = track of aircraft (not heading), derived from the velocity E/W and N/S.
    latitude = North and East positive, South and West negative.
    longitude = North and East positive, South and West negative.
    verticalrate = 64ft resolution.
    squawk = assigned Mode A squawk code.
    alert = flag to indicate squawk has changed.
    emergency = flag to indicate emergency code has been set.
    spi = flag to indicate transponder Ident has been activated.
    onground = flag to indicate ground squat switch is active.

    All flags are -1 for true and 0 for false. Neither means it is not used.
    '''

    # Regexp pattern for MSG format.
    REGEXP_MSG = r'^MSG,' \
        r'(?P<transmission>\d),' \
        r'(?P<session>\d+),' \
        r'(?P<aircraft>\d+),' \
        r'(?P<hexident>[0-9A-F]+),' \
        r'(?P<flight>\d+),' \
        r'(?P<gen_date>[0-9/]+),' \
        r'(?P<gen_time>[0-9:\.]+),' \
        r'(?P<log_date>[0-9/]+),' \
        r'(?P<log_time>[0-9:\.]+),' \
        r'(?P<callsign>[\w\s]*),' \
        r'(?P<altitude>\d*),' \
        r'(?P<speed>\d*),' \
        r'(?P<track>[\d\-]*),' \
        r'(?P<latitude>[\d\-\.]*),' \
        r'(?P<longitude>[\d\-\.]*),' \
        r'(?P<verticalrate>[\d\-]*),' \
        r'(?P<squawk>\d*),' \
        r'(?P<alert>[\d\-]*),' \
        r'(?P<emergency>[\d\-]*),' \
        r'(?P<spi>[\d\-]*),' \
        r'(?P<onground>[\d\-]*)$'

    NORMALIZE_MSG = {
        'transmission': (lambda v: int(v)),
        'session': (lambda v: int(v)),
        'aircraft': (lambda v: int(v)),
        'flight': (lambda v: int(v)),
        'callsign': (lambda v: v.strip()),
        'altitude': (lambda v: int(v)),
        'speed': (lambda v: int(v)),
        'track': (lambda v: int(v)),
        'latitude': (lambda v: float(v)),
        'longitude': (lambda v: float(v)),
        'verticalrate': (lambda v: int(v)),
        'alert': (lambda v: True if v == '-1' else False),
        'emergency': (lambda v: True if v == '-1' else False),
        'spi': (lambda v: True if v == '-1' else False),
        'onground': (lambda v: True if v == '-1' else False),
    }

    def __init__(self):
        self.re_msg = re.compile(self.REGEXP_MSG)
        self.aircrafts = {}
        self.aircrafts_age = {}

    def __getitem__(self, key):
        return self.aircrafts[key]

    def __setitem__(self, key, value):
        self.aircrafts[key] = value

    def __delitem__(self, key):
        del self.aircrafts[key]

    def __contains__(self, key):
        return key in self.aircrafts

    def __len__(self):
        return len(self.aircrafts)

    def __repr__(self):
        return repr(self.aircrafts)

    def __cmp__(self, dict_):
        return self.__cmp__(self.aircrafts, dict_)

    def __iter__(self):
        return iter(self.aircrafts)

    def __unicode__(self):
        return unicode(repr(self.aircrafts))

    def __normalize_msg(self, msg):
        for field, fnc in self.NORMALIZE_MSG.items():
            if field in msg:
                msg[field] = fnc(msg[field])

        return msg

    def keys(self):
        return self.aircrafts.keys()

    def values(self):
        return self.aircrafts.values()

    def items(self):
        return self.aircrafts.items()

    def pop(self, *args):
        return self.aircrafts.pop(*args)

    def clear(self, age):
        '''Delete all aircrafts whose messages are older than 'age' seconds. Reset counters.'''
        for hexident in list(self.aircrafts_age.keys()):
            if hexident not in self.aircrafts:
                del self.aircrafts_age[hexident]
                continue

            if (time.time() - self.aircrafts_age[hexident]) > age:
                log.info('Hexident {} too old. Deleting.'.format(hexident))
                del self.aircrafts_age[hexident]
                del self.aircrafts[hexident]

        for hexident in self.aircrafts.keys():
            self.aircrafts[hexident]['count'] = 0

    def age(self, hexident):
        '''Return age of 'hexident' aircraft. Seconds since last seen.'''
        return (time.time() - self.aircrafts_age.get(hexident, 0))

    def msg(self, data):
        '''Parse and save new ADS-B message.'''

        m = self.re_msg.match(data)
        if not m:
            log.error('Wrong format for MSG: \'{}\'.'.format(data))
            raise AdsbError('Message has wrong format!')

        message = {k: v for k, v in m.groupdict().items() if v}
        message = self.__normalize_msg(message)

        self.aircrafts_age[message['hexident']] = time.time()

        if message['hexident'] not in self.aircrafts:
            self.aircrafts[message['hexident']] = message
            self.aircrafts[message['hexident']]['count'] = 1
        else:
            self.aircrafts[message['hexident']].update(message)
            self.aircrafts[message['hexident']]['count'] += 1


class Dump1090(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.s = None
        self.data = ''

    def connect(self):
        log.info('Connecting to dump1090 TCP on {}:{}.'.format(self.host, self.port))
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connected = False

        while not connected:
            try:
                self.s.connect((self.host, self.port))
                log.info('Connected OK, receiving data')
            except:
                connected = False
                log.warning('Could not connect, retrying')
                time.sleep(1)
            else:
                connected = True

        self.s.setblocking(False)
        self.s.settimeout(1)

    def disconnect(self):
        self.s.close()

    def receive(self):
        '''Returns one line in ADS-B MSG format.'''

        ret = None

        try:
            self.data += self.s.recv(1024).decode('UTF-8')
            self.s.send(bytes("\r\n", 'UTF-8'))

            newline = self.data.find('\r\n')
            if newline >= 0:
                ret = self.data[:newline]
                self.data = self.data[newline + 2:]
        except socket.timeout:
            pass
        except socket.error as e:
            raise AdsbError('Socket error \'{}\'.'.format(e))

        return ret


class InfluxDB(object):
    def __init__(self, url, database='dump1090', username=None, password=None):
        self.url = url
        self.params = '/write?precision=s&db={}'.format(database)

    def write(self, measurement, data, timestamp=None):
        '''Write data with tags to measurement in InfluxDB. Use line protocol.'''

        lines = []

        for d in data:
            fields = []
            for k, v in d['fields'].items():
                if v is None:
                    # Skip all data with 'None' values.
                    continue
                elif type(v) is bool:
                    # Boolean should be 't' or 'f'.
                    fields.append('{}={}'.format(k, 't' if v == -1 else 'f'))
                elif type(v) is int:
                    fields.append('{}={}i'.format(k, v))
                elif type(v) is float:
                    fields.append('{}={}'.format(k, v))
                elif type(v) is str:
                    fields.append('{}="{}"'.format(k, v))
                else:
                    log.warning('Type {} not supported by InfluxDB. {}={}.'.format(
                        type(v), k, v
                    ))

            lines.append('{measurement},{tags} {fields} {timestamp}'.format(
                measurement = measurement,
                tags = ','.join('{}={}'.format(k, v) for k, v in d['tags'].items()),
                fields = ','.join(x for x in fields),
                timestamp = d['timestamp'] if 'timestamp' in d else int(time.time()),
            ))

        resp = requests.post(self.url + self.params, data = '\n'.join(l for l in lines))
        if resp.status_code == 204:
            return True

        log.error('Writing data to InfluxDB failed. Status code {}'.format(resp.status_code))
        return False

################################################################################
# Functions
################################################################################


def exit_gracefully(signum, frame):
    global run_app
    run_app = False

################################################################################
# Main
################################################################################


def main():
    parser = argparse.ArgumentParser(
        description = 'Read dump1090 TCP BaseStation data, '
        'convert to InfluxDB line protocol, and send to InfluxDB'
    )
    parser.add_argument(
        '-ds', '--dump1090-server',
        default = "127.0.0.1",
        help = "Host/IP for dump1090 [127.0.0.1]"
    )
    parser.add_argument(
        '-dp', '--dump1090-port',
        default = "30003",
        help = "Port for dump1090 TCP BaseStation data [30003]"
    )
    parser.add_argument(
        '-iu', '--influx-url',
        default = "http://127.0.0.1:8186",
        help = "InfluxDB URL [http://127.0.0.1:8186]"
    )
    parser.add_argument(
        '-db', '--influx-database',
        default = "adsb",
        help = "InfluxDB datbase name [adsb]"
    )
    parser.add_argument(
        '-si', '--send-interval',
        default = 60,
        help = "Received data will be buffered and send to InfluxDB every X seconds."
    )

    args = parser.parse_args()
    log.info(args)

    INTERVAL = int(args.send_interval)

    ap = AdsbProcessor()

    dump1090 = Dump1090(args.dump1090_server, int(args.dump1090_port))
    dump1090.connect()

    influx = InfluxDB(args.influx_url, database=args.influx_database)

    last_print = time.time()

    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    global run_app
    while run_app:
        if (time.time() - last_print) > INTERVAL:
            last_print = time.time()

            to_send = []

            for hexident, msg in ap.items():
                if not all(k in msg for k in ['callsign', 'squawk']):
                    log.info('Missing callsign or squawk for {}'.format(hexident))
                    continue

                if ap.age(hexident) > INTERVAL:
                    log.info('Aircraft {} was not seen too long. Not sending.'.format(hexident))
                    continue

                # Create Unix timestamp from "generated date and time". The message's
                # date and time is in UTC, thus we have to use calendar.
                timestamp = int(calendar.timegm(time.strptime('{} {}'.format(
                    msg['gen_date'], msg['gen_time']
                ), '%Y/%m/%d %H:%M:%S.%f')))

                # Prepare data and tags so it can be sent to InfluxDB.
                to_send.append({
                    'tags': {
                        'hexident': hexident,
                        'callsign': msg['callsign'],
                        'squawk': msg['squawk'],
                    },
                    'fields': {
                        'generated': timestamp,
                        'altitude': msg.get('altitude'),
                        'speed': msg.get('speed'),
                        'track': msg.get('track'),
                        'latitude': msg.get('latitude'),
                        'longitude': msg.get('longitude'),
                        'verticalrate': msg.get('verticalrate'),
                        'alert': msg.get('alert'),
                        'emergency': msg.get('emergency'),
                        'spi': msg.get('spi'),
                        'onground': msg.get('onground'),
                        'count': msg.get('count', 0),
                    }
                })

            ap.clear(INTERVAL * 3)
            if len(to_send) > 0:
                if influx.write('messages', to_send):
                    log.info('Saved {} aircrafts to InfluxDB.'.format(len(to_send)))
            else:
                log.info('No aircrafts to be saved in DB.')

        try:
            msg = dump1090.receive()
        except AdsbError as e:
            print(e)
            run_app = False
        else:
            if msg is not None:
                ap.msg(msg)

    log.info("Disconnected from dump1090")
    dump1090.disconnect()

if __name__ == "__main__":
    main()
