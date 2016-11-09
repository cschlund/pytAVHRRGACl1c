
import os
import datetime
import h5py
import subs_avhrrgac as subs
import read_avhrrgac_h5 as rh5
from pycmsaf.logger import setup_root_logger
from pycmsaf.database import Database, DatabaseError

logger = setup_root_logger(name='root')


class CountError(Exception):
    pass


class DatabaseMod(Database):
    def execute(self, sql, params=None, allow_none=True):
        """
        Execute an sql command.

        @param sql: sqlite command to be executed.
        @param params: Parameters corresponding to ? placeholders in the sql
        command.
        @param allow_none: Allow the query results to be empty. If set to
        C{False} an error will be raised in case of empty results.
        @type sql: str
        @type allow_none: bool
        @rtype: list
        """
        self.logger.debug(sql)
        if params:
            if len(params) == 1:
                findings = self.curs.execute(sql, params[0]).fetchall()
            else:
                findings = self.curs.executemany(sql, params).fetchall()
        else:
            findings = self.curs.execute(sql).fetchall()

        # Raise an error, if the query should have been non-empty, but did
        # not return anything
        if len(findings) == 0 and not allow_none:
            raise DatabaseError('Database query \'{0}\' did not return any '
                                'results'.format(sql))
        return findings


class QuickDatabase(DatabaseMod):
    def __init__(self, dbfile, exclusive=False, **kwargs):
        # Call __init__ of parent class
        super(QuickDatabase, self).__init__(dbfile=dbfile, **kwargs)

        # Lock database
        if exclusive:
            self.begin_exclusive()

    def init_tables(self):
        """
        Initialize tables.
        :return:
        """
        # PyGAC Version being processed
        self.curs.execute(
            'CREATE TABLE IF NOT EXISTS pygac_versions '
            '(id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL)')

        # Satellites
        self.curs.execute(
            'CREATE TABLE IF NOT EXISTS satellites '
            '(id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL)')

        # Channels
        self.curs.execute(
            'CREATE TABLE IF NOT EXISTS channels '
            '(id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL)')

        # Orbits
        self.curs.execute(
            'CREATE TABLE IF NOT EXISTS orbits '
            '(id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL)')

        # pygac processing general information
        self.curs.execute(
            'CREATE TABLE IF NOT EXISTS procs '
            '(orbit_id INTEGER NOT NULL, '
            'satellite_id INTEGER NOT NULL, '
            'pygac_version_id INTEGER NOT NULL, '
            'start_time_l1c TIMESTAMP, '
            'end_time_l1c TIMESTAMP, '
            'pygac_runtime FLOAT, '
            'pygac_errors TEXT, '
            'pygac_warnings TEXT, '
            'FOREIGN KEY (orbit_id) REFERENCES orbits(id) '
            'FOREIGN KEY (satellite_id) REFERENCES satellites(id) '
            'FOREIGN KEY (pygac_version_id) REFERENCES pygac_versions(id) '
            'PRIMARY KEY (orbit_id, satellite_id, pygac_version_id)'
            ')'
        )

        # channel statistics based on pygac output
        self.curs.execute(
            'CREATE TABLE IF NOT EXISTS stats '
            '(orbit_id INTEGER NOT NULL, '
            'satellite_id INTEGER NOT NULL, '
            'pygac_version_id INTEGER NOT NULL, '
            'channel_id INTEGER NOT NULL, '
            'min_val FLOAT, '
            'max_val FLOAT, '
            'mean_val FLOAT, '
            'number_of_total_obs INTEGER, '
            'number_of_masked_obs INTEGER, '
            'number_of_valid_obs INTEGER, '
            'FOREIGN KEY (orbit_id) REFERENCES orbits(id) '
            'FOREIGN KEY (satellite_id) REFERENCES satellites(id) '
            'FOREIGN KEY (pygac_version_id) REFERENCES pygac_versions(id) '
            'FOREIGN KEY (channel_id) REFERENCES channels(id) '
            'PRIMARY KEY (orbit_id, satellite_id, pygac_version_id, channel_id)'
            ')'
        )

        # Create views
        self.curs.execute(
            'CREATE VIEW IF NOT EXISTS vw_procs as '
            'SELECT t.*, o.name as orbit_name, s.name as satellite_name, '
            'p.name as pygac_version_name '
            'FROM procs t, orbits o, satellites s, pygac_versions p '
            'WHERE t.satellite_id = s.id AND t.orbit_id = o.id '
            'AND t.pygac_version_id = p.id'
        )
        self.curs.execute(
            'CREATE VIEW IF NOT EXISTS vw_stats as '
            'SELECT a.*, o.name as orbit_name, s.name as satellite_name, '
            'p.name as pygac_version_name, c.name as channel_name '
            'FROM stats a, orbits o, satellites s, pygac_versions p, channels c '
            'WHERE a.satellite_id = s.id AND a.orbit_id = o.id '
            'AND a.pygac_version_id = p.id AND a.channel_id = c.id'
        )

    def insert_record(self, table, records):
        """
        Insert records into table.
        :param table: table name
        :param records: record name
        :return:
        """
        self.execute('INSERT OR IGNORE INTO {table} (name) VALUES (?)'.format(table=table), params=records)
        self.commit_changes()

    def insert_procs(self, table, orbit, l1c_file, pyg_ver, prun, perr, pwarn):
        """
        Insert processing information.
        :param table: table name
        :param orbit: full qualified L1b orbit name
        :param l1c_file: full qualified L1c orbit name
        :param pyg_ver: pygac version
        :param prun: total seconds of runtime of pygac to process orbit
        :param perr: list of pygac error messages
        :param pwarn: list of pygac warning messages
        :return:
        """
        platform = get_platform_name(l1b_filename=orbit)
        sta, end = get_l1c_timestamps(l1c_filename=l1c_file)

        sat_id = self._get_id_by_name(table='satellites', name=platform)
        orb_id = self._get_id_by_name(table='orbits', name=os.path.basename(orbit))
        pyg_id = self._get_id_by_name(table='pygac_versions', name=pyg_ver)

        if len(perr) == 0:
            err = None
        else:
            err = '|'.join(perr)

        if len(pwarn) == 0:
            warn = None
        else:
            warn = '|'.join(pwarn)

        cols = 'orbit_id, satellite_id, pygac_version_id, start_time_l1c, end_time_l1c, ' \
               'pygac_runtime, pygac_errors, pygac_warnings'
        col_lst = [orb_id, sat_id, pyg_id, sta, end, prun, err, warn]
        records = [tuple(col_lst)]
        holders = ','.join('?' * len(col_lst))
        sql_query = "INSERT OR REPLACE INTO {table} ({cols}) VALUES({holders})".format(table=table, cols=cols,
                                                                                       holders=holders)
        self.execute(sql_query, params=records)
        self.commit_changes()

        return sat_id, orb_id, pyg_id

    def insert_stats(self, table, sat_id, orb_id, pyg_id, channel, stat_list):
        """
        Insert statistics information
        :param table: table name
        :param sat_id: satellite ID
        :param orb_id: orbit ID
        :param pyg_id: pygac_version ID
        :param channel: channel name
        :param stat_list: list of orbit statistics [min, max, mean, #total_obs, #masked, #not_masked]
        :return:
        """
        cha_id = self._get_id_by_name(table='channels', name=channel)
        cols = 'orbit_id, satellite_id, pygac_version_id, channel_id, ' \
               'min_val, max_val, mean_val, number_of_total_obs, number_of_masked_obs, number_of_valid_obs'
        col_lst = [orb_id, sat_id, pyg_id, cha_id] + stat_list
        records = [tuple(col_lst)]
        holders = ','.join('?' * len(col_lst))
        sql_query = "INSERT OR REPLACE INTO {table} ({cols}) VALUES({holders})".format(table=table, cols=cols,
                                                                                       holders=holders)
        self.execute(sql_query, params=records)
        self.commit_changes()


def get_platform_name(l1b_filename):
    """
    Get satellite name from L1b filename.
    :param l1b_filename: full qualified l1b filename
    :return: name of the platform
    """
    fbase = os.path.basename(l1b_filename)
    scode = fbase.split(".")[2]
    return subs.full_sat_name(scode)[2]


def get_l1c_timestamps(l1c_filename):
    """
    Extract timestamps from filename.
    :param l1c_filename: full qualified l1c filename
    :return: start_time_l1c, end_time_l1c
    """
    if l1c_filename:
        # -- split filename
        fil_name = os.path.basename(l1c_filename)
        split_string = subs.split_filename(fil_name)
        start_date_string = split_string[5][0:-1]
        end_date_string = split_string[6][0:-1]
        # -- get timestamp of first scanline
        start_datetime_string = ''.join(start_date_string.split('T'))
        start_microseconds = int(start_datetime_string[-1]) * 1E5
        start_time_l1c_help = datetime.datetime.strptime(start_datetime_string[0:-1], '%Y%m%d%H%M%S')
        start_time_l1c = start_time_l1c_help + datetime.timedelta(microseconds=start_microseconds)
        # -- get timestamp of last scanline
        end_datetime_string = ''.join(end_date_string.split('T'))
        end_microseconds = int(end_datetime_string[-1]) * 1E5
        end_time_l1c_help = datetime.datetime.strptime(end_datetime_string[0:-1], '%Y%m%d%H%M%S')
        end_time_l1c = end_time_l1c_help + datetime.timedelta(microseconds=end_microseconds)
        return start_time_l1c, end_time_l1c
    else:
        return None, None


def collect_stats(h5file, data):
    """
    Read h5file and collect stats for specified channel.
    :param h5file: pygac output file = l1c file
    :return: min, max, mean, number of missing data
    """
    import numpy as np
    # read data
    f = h5py.File(h5file, "r+")
    var, var_name, unscaled, fillv = rh5.read_var(f, data, unscaled=True)

    # VIS reflectance between 0 and 1
    if data == 'image1' or data == 'image2' or data == 'image6':
        var[:] = var[:] / 100.

    # min/max/mean of orbit based on scaled obs: data*gain + offset
    if np.ma.count(var) > 0:
        minv = float(var.min())
        maxv = float(var.max())
        meanv = float(var.mean())
    else:
        minv = float(fillv)
        maxv = float(fillv)
        meanv = float(fillv)

    # count valid and invalid observations
    mask = np.ma.masked_equal(unscaled, fillv)
    total_obs = int(np.ma.count(unscaled))
    masked_obs = int(np.ma.count_masked(mask))
    not_masked_obs = int(np.ma.count(mask))

    if total_obs != (masked_obs + not_masked_obs):
        raise CountError(
            logger.info("Something is wrong here: "
                        "total_obs:{0} != masked_obs:{1} + not_masked_obs:{2}".
                        format(total_obs, masked_obs, not_masked_obs))
        )

    return [minv, maxv, meanv, total_obs, masked_obs, not_masked_obs]


def collect_records(l1b_file, l1c_file, sql_file, pygac_version,
                    pygac_took, pygac_errors, pygac_warnings):
    """
    Make a quick analysis of l1c output file and store it into a SQLite database.
    :param l1b_file: full qualified l1b file
    :param l1c_file: full qualified l1c file or None
    :param sql_file: SQLite database
    :param failed: if no l1c file was produced, store the traceback reason
    :param pygac_version: addresses the current pygac version used for processing
    :param pygac_took: total seconds of pygac runtime
    :param pygac_errors: list of error messages from pygac
    :param pygac_warnings: list of warning messages from pygac
    :return:
    """
    # -- open SQLite database
    db = QuickDatabase(dbfile=sql_file, timeout=3600, create=True)

    # -- prepare satellite and channel records
    satellites = subs.get_satellite_list()
    channels = subs.get_channel_list()
    images = subs.get_avhrr_h5image_list()
    sat_records = list()
    cha_records = list()
    for sat in satellites:
        sat_records.append((sat,))
    for cha in channels:
        cha_records.append((cha,))

    # -- insert records for assisting tables
    db.insert_record(table='satellites', records=sat_records)
    db.insert_record(table='channels', records=cha_records)
    db.insert_record(table='pygac_versions', records=[(pygac_version,)])
    db.insert_record(table='orbits', records=[(os.path.basename(l1b_file),)])

    # -- insert processing information
    (sat_id, orb_id, pyg_id) = db.insert_procs(table='procs', orbit=l1b_file, l1c_file=l1c_file,
                                               pyg_ver=pygac_version, prun=pygac_took,
                                               perr=pygac_errors, pwarn=pygac_warnings)

    # -- get stats for each channel of l1c orbit
    if l1c_file:
        for data, channel in zip(images, channels):
            stat_list = collect_stats(h5file=l1c_file, data=data)
            db.insert_stats(table='stats', sat_id=sat_id, orb_id=orb_id, pyg_id=pyg_id,
                            channel=channel, stat_list=stat_list)

    # db.print_schema()
    # db.printout(table='vw_procs')
    # db.printout(table='vw_stats')