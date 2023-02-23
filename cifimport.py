import argparse
from collections import Counter
from datetime import date, time
from getpass import getpass
from tqdm import tqdm
import os
import json
import sys
import psycopg

# Helper Functions
def blocks(files, size=65536):
    while True:
        b = files.read(size)
        if not b:
            break
        yield b


def coalesce(*values):
    return next((v for v in values if v is not None), None)


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def str_fmt(in_str):
    return None if in_str.isspace() or len(in_str) == 0 else in_str.strip()


def bool_fmt(in_bool):
    return True if in_bool == "Y" else False


def int_fmt(in_int):
    try:
        # time 0000 is a null in the mainframe
        return None if in_int == "0000" else int(in_int)
    except:
        return None


def date_fmt(in_date, reverse=False):
    try:
        if reverse == True:
            day = int(in_date[0:2])
            month = int(in_date[2:4])
            year = int(in_date[4:6])
        else:
            year = int(in_date[0:2])
            month = int(in_date[2:4])
            day = int(in_date[4:6])
        if year > 59:
            year += 1900
        else:
            year += 2000
        return date(year, month, day).strftime("%Y-%m-%d")
    except:
        return None


def time_fmt(in_time):
    try:
        hours = int(in_time[0:2])
        mins = int(in_time[2:4])
        seconds = 0
        if len(in_time) == 5:
            if in_time[4:] == "H":
                seconds = 30
        return time(hours, mins, seconds).strftime("%H:%M:%S")
    except:
        return None


# Schema


class Header:
    def __init__(self, raw):
        self.file_mainframe_identity = str_fmt(raw[2:22])
        self.date_of_extract = date_fmt(raw[22:28], reverse=True)
        self.time_of_extract = time_fmt(raw[28:32])
        self.current_file_reference = str_fmt(raw[32:39])
        self.last_file_reference = str_fmt(raw[39:46])
        self.update_indicator = str_fmt(raw[46:47])
        self.version = str_fmt(raw[47:48])
        self.user_start_date = date_fmt(raw[48:54], reverse=True)
        self.user_end_date = date_fmt(raw[54:60], reverse=True)


class Tiploc:
    def __init__(self, raw):
        self.tiploc_code = str_fmt(raw[2:9])
        self.nalco = int_fmt(raw[11:17])
        self.check_char = str_fmt(raw[17:18])
        self.tps_description = str_fmt(raw[18:44])
        self.stanox = int_fmt(raw[44:49])
        self.crs_code = str_fmt(raw[53:56])
        self.description = str_fmt(raw[56:72])
        self.new_tiploc = coalesce(str_fmt(raw[72:79]), self.tiploc_code)


class Association:
    def __init__(self, raw):
        self.transaction_type = str_fmt(raw[2:3])
        self.main_train_uid = str_fmt(raw[3:9])
        self.assoc_train_uid = str_fmt(raw[9:15])
        self.assoc_start_date = date_fmt(raw[15:21])
        self.assoc_end_date = date_fmt(raw[21:27])
        self.assoc_days = str_fmt(raw[27:34])
        self.category = str_fmt(raw[34:36])
        self.date_indicator = str_fmt(raw[36:37])
        self.location = str_fmt(raw[37:44])
        self.base_location_suffix = int_fmt(raw[44:45])
        self.assoc_location_suffix = int_fmt(raw[45:46])
        self.diagram_type = str_fmt(raw[46:47])
        self.association_type = str_fmt(raw[47:48])
        self.stp_indicator = str_fmt(raw[79:80])


class Schedule:
    def __init__(self, raw):
        # BS Record
        self.transaction_type = str_fmt(raw[2:3])
        self.train_uid = str_fmt(raw[3:9])
        self.schedule_start_date = date_fmt(raw[9:15])
        self.schedule_end_date = date_fmt(raw[15:21])
        self.schedule_days_runs = str_fmt(raw[21:28])
        self.train_status = str_fmt(raw[29:30])
        self.train_category = str_fmt(raw[30:32])
        self.signalling_id = str_fmt(raw[32:36])
        self.train_service_code = int_fmt(raw[41:49])
        self.power_type = str_fmt(raw[50:53])
        self.timing_load = str_fmt(raw[53:57])
        self.speed = int_fmt(raw[57:60])
        self.operating_characteristics = str_fmt(raw[60:66])
        self.train_class = str_fmt(raw[66:67])
        self.sleepers = str_fmt(raw[67:68])
        self.reservations = str_fmt(raw[68:69])
        self.catering_code = str_fmt(raw[70:74])
        self.service_branding = str_fmt(raw[74:78])
        self.stp_indicator = str_fmt(raw[79:80])
        # BX Record
        self.uic_code = None
        self.atoc_code = None
        self.applicable_timetable = None
        # Time
        self.last_modified = None
        # Child rows
        self.locations = []
        self.changes = []

    def set_bx(self, raw):
        self.uic_code = str_fmt(raw[6:11])
        self.atoc_code = str_fmt(raw[11:13])
        self.applicable_timetable = bool_fmt(raw[13:14])

    def set_time(self, time):
        self.last_modified = time

    def add_location(self, obj):
        if isinstance(obj, OriginLocation):
            self.locations.append(vars(obj))
        elif isinstance(obj, IntermediateLocation):
            self.locations.append(vars(obj))
        elif isinstance(obj, TerminatingLocation):
            self.locations.append(vars(obj))

    def add_changes(self, obj):
        if isinstance(obj, ChangesEnRoute):
            self.changes.append(vars(obj))


class OriginLocation:
    def __init__(self, raw):
        self.tiploc_code = str_fmt(raw[2:9])
        self.tiploc_instance = str_fmt(raw[9:10])
        self.departure = time_fmt(raw[10:15])
        self.public_departure = time_fmt(raw[15:19])
        self.platform = str_fmt(raw[19:22])
        self.line = str_fmt(raw[22:25])
        self.engineering_allowance = str_fmt(raw[25:27])
        self.pathing_allowance = str_fmt(raw[27:29])
        self.activity = str_fmt(raw[29:41])
        self.performance_allowance = str_fmt(raw[41:43])
        self.arrival = None
        self.public_arrival = None
        self.path = None
        self.arrival_day = None
        self.departure_day = 0


class IntermediateLocation:
    def __init__(self, raw):
        self.tiploc_code = str_fmt(raw[2:9])
        self.tiploc_instance = str_fmt(raw[9:10])
        self.arrival = time_fmt(raw[10:15])
        self.departure = coalesce(time_fmt(raw[15:20]), time_fmt(raw[20:25]))
        self.public_arrival = time_fmt(raw[25:29])
        self.public_departure = time_fmt(raw[29:33])
        self.platform = str_fmt(raw[33:36])
        self.line = str_fmt(raw[36:39])
        self.path = str_fmt(raw[39:42])
        self.activity = str_fmt(raw[42:54])
        self.engineering_allowance = str_fmt(raw[54:56])
        self.pathing_allowance = str_fmt(raw[56:58])
        self.performance_allowance = str_fmt(raw[58:60])
        self.arrival_day = 0
        self.departure_day = 0


class TerminatingLocation:
    def __init__(self, raw):
        self.tiploc_code = str_fmt(raw[2:9])
        self.tiploc_instance = str_fmt(raw[9:10])
        self.arrival = time_fmt(raw[10:15])
        self.public_arrival = time_fmt(raw[15:19])
        self.platform = str_fmt(raw[19:22])
        self.path = str_fmt(raw[22:25])
        self.activity = str_fmt(raw[25:37])
        self.departure = None
        self.public_departure = None
        self.line = None
        self.engineering_allowance = None
        self.pathing_allowance = None
        self.performance_allowance = None
        self.arrival_day = 0
        self.departure_day = None


class ChangesEnRoute:
    def __init__(self, raw):
        self.tiploc_code = str_fmt(raw[2:9])
        self.tiploc_instance = str_fmt(raw[9:10])
        self.train_category = str_fmt(raw[10:12])
        self.signalling_id = str_fmt(raw[12:16])
        self.course_indicator = str_fmt(raw[20:21])
        self.train_service_code = int_fmt(raw[21:29])
        self.power_type = str_fmt(raw[30:33])
        self.timing_load = str_fmt(raw[33:37])
        self.speed = int_fmt(raw[37:40])
        self.operating_characteristics = str_fmt(raw[40:46])
        self.train_class = str_fmt(raw[46:47])
        self.sleepers = str_fmt(raw[47:48])
        self.reservations = str_fmt(raw[48:49])
        self.catering_code = str_fmt(raw[50:54])
        self.service_branding = str_fmt(raw[54:58])
        self.uic_code = str_fmt(raw[62:67])


# Database functions


def select_last_ref(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT current_file_reference
            FROM nrod_header
            WHERE update_indicator = %s
            ORDER BY date_of_extract DESC LIMIT 1;""",
            ["U"],
        )
        one = cursor.fetchone()
        if one:
            return one[0]
    return None


def truncate_tables(connection):
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE nrod_association;")
        cursor.execute("TRUNCATE nrod_header;")
        cursor.execute("DELETE FROM nrod_schedule WHERE is_vstp = FALSE;")
        cursor.execute("TRUNCATE nrod_tiploc;")


def insert_header(connection, data):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO nrod_header VALUES (
                %(file_mainframe_identity)s,
                %(date_of_extract)s,
                %(time_of_extract)s,
                %(current_file_reference)s,
                %(last_file_reference)s,
                %(update_indicator)s,
                %(user_start_date)s,
                %(user_end_date)s,
                %(statistics)s
            );
        """,
            data,
        )


def insert_tiplocs(connection, data):
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO nrod_tiploc VALUES (
                %(new_tiploc)s,
                %(nalco)s,
                %(check_char)s,
                %(tps_description)s,
                %(stanox)s,
                %(crs_code)s,
                %(description)s
            );
        """,
            data,
        )


def delete_tiplocs(connection, data):
    with connection.cursor() as cursor:
        for d in data:
            cursor.execute("DELETE FROM nrod_tiploc WHERE tiploc_code = %(tiploc_code)s;", d)
            if cursor.rowcount == 0:
                print("Tiploc Delete ({0}) affected 0 rows".format(d["tiploc_code"]))


def insert_associations(connection, data):
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO nrod_association (
                main_train_uid,
                assoc_train_uid,
                assoc_start_date,
                assoc_end_date,
                assoc_days,
                category,
                date_indicator,
                location,
                base_location_suffix,
                assoc_location_suffix,
                diagram_type,
                association_type,
                stp_indicator
            )
            VALUES (
                %(main_train_uid)s,
                %(assoc_train_uid)s,
                %(assoc_start_date)s,
                %(assoc_end_date)s,
                %(assoc_days)s,
                %(category)s,
                %(date_indicator)s,
                %(location)s,
                %(base_location_suffix)s,
                %(assoc_location_suffix)s,
                %(diagram_type)s,
                %(association_type)s,
                %(stp_indicator)s
            );
        """,
            data,
        )


def delete_associations(connection, associations):
    with connection.cursor() as cursor:
        for a in associations:
            cursor.execute(
                """
                DELETE FROM nrod_association
                WHERE main_train_uid = %s AND assoc_train_uid = %s AND assoc_start_date = %s
                AND diagram_type = %s AND location = %s AND stp_indicator = %s;
            """,
                (
                    a["main_train_uid"],
                    a["assoc_train_uid"],
                    a["assoc_start_date"],
                    a["diagram_type"],
                    a["location"],
                    a["stp_indicator"],
                ),
            )
            if cursor.rowcount == 0:
                print(
                    "Association {0} ({1}, {2}, {3}, {4}, {5}, {6}) affected 0 rows".format(
                        a["transaction_type"],
                        a["main_train_uid"],
                        a["assoc_train_uid"],
                        a["assoc_start_date"],
                        a["diagram_type"],
                        a["location"],
                        a["stp_indicator"],
                    )
                )


def returning_id_generator(cursor):
    while True:
        yield cursor.fetchone()[0]
        if cursor.nextset() != True:
            break


def insert_schedules(connection, schedules):
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO nrod_schedule (
                is_vstp,
                train_uid,
                schedule_start_date,
                schedule_end_date,
                schedule_days_runs,
                train_status,
                train_category,
                signalling_id,
                train_service_code,
                power_type,
                timing_load,
                speed,
                operating_characteristics,
                train_class,
                sleepers,
                reservations,
                catering_code,
                service_branding,
                stp_indicator,
                uic_code,
                atoc_code,
                applicable_timetable,
                last_modified
            )
            VALUES (
                FALSE,
                %(train_uid)s,
                %(schedule_start_date)s,
                %(schedule_end_date)s,
                %(schedule_days_runs)s,
                %(train_status)s,
                %(train_category)s,
                %(signalling_id)s,
                %(train_service_code)s,
                %(power_type)s,
                %(timing_load)s,
                %(speed)s,
                %(operating_characteristics)s,
                %(train_class)s,
                %(sleepers)s,
                %(reservations)s,
                %(catering_code)s,
                %(service_branding)s,
                %(stp_indicator)s,
                %(uic_code)s,
                %(atoc_code)s,
                %(applicable_timetable)s,
                %(last_modified)s
            ) RETURNING id;
        """,
            schedules,
            returning=True,
        )

        # Collect returning id's
        returning = [id for id in returning_id_generator(cursor)]
        if len(returning) != len(schedules):
            print("Schedule ID's mismatched during insert!")
            sys.exit(1)
        # Assign schedule_id's to respective locations/changes
        locations = []
        changes = []
        for s, id in zip(schedules, returning):
            locations.extend([{**d, "schedule_id": id, "position": pos} for pos, d in enumerate(s["locations"])])
            changes.extend([{**d, "schedule_id": id} for d in s["changes"]])
        # Insert locations/changes
        insert_schedule_locations(connection, locations)
        insert_changes_en_route(connection, changes)


def delete_schedules(connection, schedules):
    with connection.cursor() as cursor:
        for s in schedules:
            cursor.execute(
                """
                DELETE FROM nrod_schedule
                WHERE is_vstp = FALSE AND train_uid = %(train_uid)s AND
                schedule_start_date = %(schedule_start_date)s AND
                stp_indicator = %(stp_indicator)s;
            """,
                s,
            )
            if cursor.rowcount == 0:
                print(
                    "Schedule {0} ({1}, {2}, {3}) affected 0 rows".format(
                        s["transaction_type"],
                        s["train_uid"],
                        s["schedule_start_date"],
                        s["stp_indicator"],
                    )
                )


def delete_old_schedules(connection, date):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            DELETE FROM nrod_schedule WHERE is_vstp IS FALSE AND
            schedule_end_date < %s::date - INTERVAL '1 day';
        """,
            (date,),
        )
        print("Deleted {0} schedules that have become historic".format(cursor.rowcount))


def insert_schedule_locations(connection, locations):
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO nrod_schedule_location (
                schedule_id,
                position,
                tiploc_code,
                tiploc_instance,
                arrival_day,
                departure_day,
                arrival,
                departure,
                public_arrival,
                public_departure,
                platform,
                line,
                path,
                activity,
                engineering_allowance,
                pathing_allowance,
                performance_allowance
            ) VALUES (
                %(schedule_id)s,
                %(position)s,
                %(tiploc_code)s,
                %(tiploc_instance)s,
                %(arrival_day)s,
                %(departure_day)s,
                %(arrival)s,
                %(departure)s,
                %(public_arrival)s,
                %(public_departure)s,
                %(platform)s,
                %(line)s,
                %(path)s,
                %(activity)s,
                %(engineering_allowance)s,
                %(pathing_allowance)s,
                %(performance_allowance)s
            );
        """,
            locations,
        )


def insert_changes_en_route(connection, changes):
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO nrod_changes_en_route (
                schedule_id,
                tiploc_code,
                tiploc_instance,
                train_category,
                signalling_id,
                train_service_code,
                power_type,
                timing_load,
                speed,
                operating_characteristics,
                train_class,
                sleepers,
                reservations,
                catering_code,
                service_branding,
                uic_code
            ) VALUES (
                %(schedule_id)s,
                %(tiploc_code)s,
                %(tiploc_instance)s,
                %(train_category)s,
                %(signalling_id)s,
                %(train_service_code)s,
                %(power_type)s,
                %(timing_load)s,
                %(speed)s,
                %(operating_characteristics)s,
                %(train_class)s,
                %(sleepers)s,
                %(reservations)s,
                %(catering_code)s,
                %(service_branding)s,
                %(uic_code)s
            );
        """,
            changes,
        )


# Parser
def parse(f, connection):
    # Count lines
    f.seek(0)
    total_lines = sum(bl.count("\n") for bl in blocks(f))
    print("Total lines: {0}".format(total_lines))

    # Header record on first line
    f.seek(0)
    hd = Header(f.readline())
    print("File mainframe id: {0}".format(hd.file_mainframe_identity))
    print("Time of extract: {0} {1}".format(hd.date_of_extract, hd.time_of_extract))
    print("Current file ref.: {0}".format(hd.current_file_reference))
    print("Last file ref.: {0}".format(hd.last_file_reference))
    print("Update indicator: {0}".format(hd.update_indicator))
    print("User time window: {0} - {1}\n".format(hd.user_start_date, hd.user_end_date))

    # Delete schedules ending before the time window
    delete_old_schedules(connection, hd.user_start_date)

    # Continuity check
    last_ref = select_last_ref(connection)
    if last_ref != None and last_ref != hd.last_file_reference:
        print("Continuity error: last file ref did not match {0} in the database".format(last_ref))
        sys.exit(1)

    # Use time the schedules were extracted on
    last_modified = f"{hd.date_of_extract}T{hd.time_of_extract}+00:00"

    # Caches for objects
    cache_tiploc_insert = []
    cache_tiploc_delete = []
    cache_assoc_insert = []
    cache_assoc_delete = []
    cache_schedule_insert = []
    cache_schedule_delete = []

    bs = None
    last_li = None

    counter = Counter()

    # Display a progress bar
    pbar = tqdm(total=total_lines, desc="Processing")

    # Iterate line-by-line
    f.seek(0)
    for line in f:
        record = line[0:2]

        if record == "HD":  # Header Record
            counter.update(HD=1)

        elif record == "TI":  # TIPOC Insert
            counter.update(TI=1)
            ti = Tiploc(line)
            cache_tiploc_insert.append(vars(ti))

        elif record == "TA":  # TIPLOC Amend
            counter.update(TA=1)
            ta = Tiploc(line)
            cache_tiploc_delete.append(vars(ta))
            cache_tiploc_insert.append(vars(ta))

        elif record == "TD":  # TIPLOC Delete
            counter.update(TD=1)
            td = Tiploc(line)
            cache_tiploc_delete.append(vars(td))

        elif record == "AA":  # Associations
            counter.update(AA=1)
            aa = Association(line)
            # Append revised/deleted association to cache for deletion
            if aa.transaction_type == "D" or aa.transaction_type == "R":
                cache_assoc_delete.append(vars(aa))
            # Append association for insertion if not a Delete
            if aa.transaction_type != "D":
                cache_assoc_insert.append(vars(aa))

        elif record == "BS":  # Basic Schedule
            counter.update(BS=1)
            bs = Schedule(line)
            bs.set_time(last_modified)
            # Append revised/deleted schedule to cache for deletion
            if bs.transaction_type == "D":
                cache_schedule_delete.append(vars(bs))
            elif bs.transaction_type == "R":
                cache_schedule_delete.append(vars(bs))
            # Append a STP cancellation for insertion (if not a delete)
            if bs.stp_indicator == "C" and bs.transaction_type != "D":
                cache_schedule_insert.append(vars(bs))
            # Clear cached schedule object after a delete
            if bs.transaction_type == "D":
                bs = None

        elif record == "BX":  # Basic Schedule Extra Details
            counter.update(BX=1)
            if bs is None:
                print("Logical error in CIF Schedule file!")
                exit(1)
            bs.set_bx(line)

        elif record == "TN":  # Train specific note (Unused)
            counter.update(TN=1)
            pass

        elif record == "LO":  # Location Origin
            counter.update(LO=1)
            if bs is None:
                print("Logical error in CIF Schedule file!")
                exit(1)
            lo = OriginLocation(line)
            # Add location
            bs.add_location(lo)
            # Set as previous location
            last_li = lo

        elif record == "LI":  # Location Intermediate
            counter.update(LI=1)
            if bs is None:
                print("Logical error in CIF Schedule file!")
                exit(1)
            li = IntermediateLocation(line)
            # Populate overnight running days
            li.arrival_day = last_li.arrival_day if last_li.arrival_day != None else 0
            li.departure_day = last_li.departure_day if last_li.departure_day != None else 0
            if last_li != None:
                if li.arrival != None and li.arrival < last_li.departure:
                    li.arrival_day += 1
                if li.departure < last_li.departure:
                    li.departure_day += 1
                    if li.arrival is None:
                        li.arrival_day += 1
            # Add location
            bs.add_location(li)
            # Set as previous location
            last_li = li

        elif record == "LT":  # Location Terminus (Closes a Schedule)
            counter.update(LT=1)
            if bs is None:
                print("Logical error in CIF Schedule file!")
                exit(1)
            lt = TerminatingLocation(line)
            # Populate overnight running days
            lt.arrival_day = last_li.arrival_day if last_li.arrival_day != None else 0
            if last_li != None:
                if lt.arrival != None and lt.arrival < last_li.departure:
                    li.arrival_day += 1
            # Add location
            bs.add_location(lt)
            # Append closed schedule to cache
            cache_schedule_insert.append(vars(bs))
            # Clear cached values
            bs = None
            last_li = None

        elif record == "CR":  # Change en route
            counter.update(CR=1)
            if bs is None:
                print("Logical error in CIF Schedule file!")
                exit(1)
            cr = ChangesEnRoute(line)
            bs.add_changes(cr)

        elif record == "LN":  # Location Note (Unused)
            counter.update(LN=1)
            pass

        elif record == "ZZ":  # Trailer Record
            counter.update(ZZ=1)
            pass

        # Save objects to the database in chunks of 2000 items and at the end of the file
        try:
            len_ti = len(cache_tiploc_delete) + len(cache_tiploc_insert)
            if (len_ti % 2000 == 0 and len_ti > 0) or (len_ti > 0 and record == "ZZ"):
                delete_tiplocs(connection, cache_tiploc_delete)
                insert_tiplocs(connection, cache_tiploc_insert)
                cache_tiploc_delete.clear()
                cache_tiploc_insert.clear()
            len_aa = len(cache_assoc_delete) + len(cache_assoc_insert)
            if (len_aa % 2000 == 0 and len_aa > 0) or (len_aa > 0 and record == "ZZ"):
                delete_associations(connection, cache_assoc_delete)
                insert_associations(connection, cache_assoc_insert)
                cache_assoc_delete.clear()
                cache_assoc_insert.clear()
            len_bs = len(cache_schedule_delete) + len(cache_schedule_insert)
            if (len_bs % 2000 == 0 and len_bs > 0) or (len_bs > 0 and record == "ZZ"):
                delete_schedules(connection, cache_schedule_delete)
                insert_schedules(connection, cache_schedule_insert)
                cache_schedule_delete.clear()
                cache_schedule_insert.clear()
        except psycopg.Error as e:
            print("SQL Error: {}".format(e.sqlstate))
            print(e.diag.message_primary)
            if e.diag.message_detail:
                print(e.diag.message_detail)
            print("Changes not committed")
            connection.rollback()
            connection.close()
            sys.exit(1)

        # Update progress
        pbar.update()

    stats = json.dumps([{"record": key, "value": value} for key, value in counter.items()])
    insert_header(connection, {**vars(hd), **{"statistics": stats}})
    pbar.close()


def main():
    ap = argparse.ArgumentParser(prog="cifimport", description="CIF Schedule Import Tool", conflict_handler="resolve")
    ap.add_argument("filename", help="read data from the file filename")

    # Postgres related arguments
    ap.add_argument("-d", "--dbname", required=True, help="specifies the name of the database to connect to")
    ap.add_argument(
        "-h",
        "--host",
        default="localhost",
        required=False,
        help="specifies the host name on which the server is runnin",
    )
    ap.add_argument(
        "-p", "--port", default=5432, required=False, help="specifies the port on which the server is listening"
    )
    ap.add_argument("-U", "--username", required=True, help="connect to the database as the username")
    ap.add_argument(
        "-W",
        "--password",
        required=False,
        action="store_true",
        help="prompt for a password before connecting to a database",
    )
    ap.add_argument(
        "--init",
        required=False,
        action="store_true",
        help="initialises the database (TRUNCATES ALL TABLES!)",
    )
    ap.add_argument("-t", required=False, action="store_true", help="test only without committing")
    args = ap.parse_args()

    # Check if the file exists
    if os.path.isfile(args.filename) != True:
        print("Error: {0} is not a file!".format(args.filename))
        sys.exit(1)

    # Postgres connection details
    dsn = f"dbname={args.dbname} user={args.username} host={args.host} port={args.port}"
    # Prompt for a password if requested
    if args.password == True:
        args.password = getpass(prompt="Password: ", stream=None)
        dsn = f"{dsn} password={args.password}"

    # Create a connection
    with psycopg.connect(dsn) as connection:
        # Truncate tables if requested
        if args.init == True and args.t != True:
            # ask for permission
            if not input("Init replaces old data. Are you sure? (y/n): ").lower().strip()[:1] == "y":
                sys.exit(1)
            print("Truncating tables...")
            truncate_tables(connection)
            connection.commit()

        # Process the file
        with open(args.filename, "r", encoding="iso-8859-1", errors="ignore") as f:
            file_size = sizeof_fmt(os.stat(args.filename).st_size)
            print("Reading data from {0}...".format(args.filename))
            print("Size on disk: {0}".format(file_size))

            first_line = f.readline()

            if first_line.startswith("HD") != True:
                print("Error: {0} is not a CIF Schedule file!".format(args.filename))
                sys.exit(1)

            parse(f, connection)
            # If a test then rollback otherwise commit
            if args.t == True:
                connection.rollback()
            else:
                connection.commit()


if __name__ == "__main__":
    main()
