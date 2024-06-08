


from __future__ import annotations

"""Support for GTFS."""
#from datetime import datetime, timezone
import datetime  as dt
import logging
from typing import Any

import os
import geopy.distance

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import slugify
import homeassistant.util.dt as dt_util
from homeassistant.helpers.event import (
    async_track_state_change_event,
    async_track_time_interval,
)
from homeassistant.const import (
    STATE_UNAVAILABLE,
    STATE_UNKNOWN,
    EVENT_HOMEASSISTANT_STARTED,
)
import functools
import cachetools.func

from .const import *
from .coordinator import *
from .common import *

_LOGGER = logging.getLogger(__name__)


####################################################################
####################################################################
####################################################################

@cachetools.func.ttl_cache(maxsize=2048, ttl=3600)  # Note: TTL value is in seconds.
def get_stop_details(gtfs_conn, feed_id, stop_id):

    sql_query = f"""
        SELECT * FROM stops
        where feed_id = :feed_id and
              stop_id = :stop_id 
    """  # noqa: S608

    sql_result = gtfs_conn.execute(
        sqlalchemy.sql.text(sql_query),
        {
            "feed_id" : feed_id,
            "stop_id":  stop_id,

        },
    )

    row= {}
    for row_cursor in sql_result:
        row = row_cursor._asdict()

    return row




####################################################################
####################################################################
####################################################################

@cachetools.func.ttl_cache(maxsize=2048, ttl=3600)  # Note: TTL value is in seconds.
def get_end_stop_of_trip(gtfs_conn, feed_id, trip_id, direction_id):

    sql_query = f"""
        SELECT * FROM stops
        where feed_id = :feed_id and
            stop_id = ( select stop_id from stop_times 
                            where   feed_id = :feed_id and
                                    trip_id = :trip_id and
                                    stop_sequence = ( select max (st2.stop_sequence)  
                                                    from stop_times st2
                                                    where feed_id = :feed_id and
                                                        trip_id = :trip_id) 

            ) 
    """  # noqa: S608

    sql_result = gtfs_conn.execute(
        sqlalchemy.sql.text(sql_query),
        {
            "feed_id" : feed_id,
            "trip_id":  trip_id,

        },
    )

    row= {}
    for row_cursor in sql_result:
        row = row_cursor._asdict()

    return row



####################################################################
####################################################################
####################################################################
def get_routes_stop_id(gtfs_conn, feed_id ,regex1, regex2 ):


    sql_query = f"""

        select distinct 
            con.route_id     as route_id , 
            con.stop_id_1    as stop_id,
            con.stop_name_1    as stop_name,

            con.stop_lat_1   as latitude,
            con.stop_lon_1   as longitude

        from "public"."V_ALL_CONNECTIONS" as con

        where stop_name_1 like  :regex1
            and stop_name_2 like :regex2	
            and feed_id = :feed_id and
            stop_1_before_2 is true

    """  # noqa: S608
    sql_result = gtfs_conn.execute(
        sqlalchemy.sql.text(sql_query),
        {
            "feed_id" : feed_id,
            "regex1"  : regex1,
            "regex2"  : regex2,
        },
    )        

    result = []

    for row_cursor in sql_result:
        row = row_cursor._asdict()
        result.append(row)


    return result



####################################################################
####################################################################
####################################################################
def next_departures_postgresql_single_day (
    gtfs_conn , 
    feed_id, 
    p_date, 
    dayname, 
    stop_regex1,
    stop_regex2,                 
    p_from_time, 
    p_to_time):


    # retreive routes excluding exception_type = 2
    sql_query = f"""
        select * from (
        (SELECT 
                stop_from.stop_id   as from_stop_id,
                stop_from.stop_name as from_stop_name,
                stop_to.stop_id   as to_stop_id,
                stop_to.stop_name as to_stop_name,                                
                trip.trip_id, 
                trip.trip_headsign, 
                trip.direction_id, 
                (:p_date)::date    as departure_date,
                st_from.departure_time  as departure_time,
                route.route_id, 
                route.route_long_name,
                route.route_short_name,
                route.route_type,
                0 as exception_type 
        FROM trips trip
        INNER JOIN calendar as calendar
                ON  calendar.feed_id    = :feed_id and
                    calendar.service_id = trip.service_id 
        INNER JOIN stop_times as st_from
                ON  st_from.feed_id = :feed_id  and 
                    st_from.trip_id = trip.trip_id
        INNER JOIN stops as stop_from
                ON  stop_from.feed_id = :feed_id  and 
                    stop_from.stop_id = st_from.stop_id 

        INNER JOIN stop_times as st_to
                ON  st_to.feed_id = :feed_id  and 
                    st_to.trip_id = trip.trip_id
        INNER JOIN stops as stop_to
                ON  stop_to.feed_id = :feed_id  and 
                    stop_to.stop_id = st_to.stop_id



        INNER JOIN routes as route
                ON  route.feed_id  = :feed_id  and
                    route.route_id = trip.route_id  
        WHERE 
            trip.feed_id = :feed_id 
            and stop_from.stop_name like  :stop_regex1
            and stop_to.stop_name like    :stop_regex2
            and st_to.stop_sequence > st_from.stop_sequence
            and ( calendar.start_date <= :p_date and calendar.end_date >= :p_date )
            and ( calendar.{dayname} = true )
            and ( st_from.departure_time  >=  interval :p_from_time ) 
            and ( st_from.departure_time  <=  interval :p_to_time   ) 
            and trip.service_id not in (select service_id 
                                        from calendar_dates as cd2 
                                        where cd2.feed_id = :feed_id and
                                            cd2.date = date(:p_date) and
                                            cd2.service_id = trip.service_id and 
                                            cd2.exception_type = 2)
        ) 
        UNION ALL
        (SELECT 
                stop_from.stop_id   as from_stop_id,
                stop_from.stop_name as from_stop_name,
                stop_to.stop_id   as to_stop_id,
                stop_to.stop_name as to_stop_name,                                
                trip.trip_id, 
                trip.trip_headsign, 
                trip.direction_id, 
                (:p_date)::date    as departure_date,
                st_from.departure_time  as departure_time,
                route.route_id, 
                route.route_long_name,
                route.route_short_name,
                route.route_type,
                1 as exception_type
        FROM trips trip
        INNER JOIN calendar_dates as calendar_dates
                ON  trip.feed_id    = calendar_dates.feed_id and
                    trip.service_id = calendar_dates.service_id

        INNER JOIN stop_times as st_from
                ON  st_from.feed_id = :feed_id  and 
                    st_from.trip_id = trip.trip_id
        INNER JOIN stops as stop_from
                ON  stop_from.feed_id = :feed_id  and 
                    stop_from.stop_id = st_from.stop_id

        INNER JOIN stop_times as st_to
                ON  st_to.feed_id = :feed_id  and 
                    st_to.trip_id = trip.trip_id
        INNER JOIN stops as stop_to
                ON  stop_to.feed_id = :feed_id  and 
                    stop_to.stop_id = st_to.stop_id 


        INNER JOIN routes as route
                ON  trip.feed_id   = route.feed_id and
                    trip.route_id  = route.route_id  
        WHERE 
            trip.feed_id = :feed_id 
            and stop_from.stop_name like  :stop_regex1
            and stop_to.stop_name like    :stop_regex2
            and st_to.stop_sequence > st_from.stop_sequence
            and  calendar_dates.exception_type = 1
            and ( calendar_dates.date = :p_date )
            and ( st_from.departure_time  >=  interval :p_from_time ) 
            and ( st_from.departure_time  <=  interval :p_to_time   ) 
        ) 
        )as MY_SELECT_UNION order by  from_stop_id asc, departure_date asc, departure_time asc 
            
        """  # noqa: S608

    result = gtfs_conn.execute(
        sqlalchemy.sql.text(sql_query),
        {
            "feed_id"     : feed_id,
            "p_date"      : p_date,
            "p_from_time" : p_from_time,
            "p_to_time"   : p_to_time,
            "stop_regex1" : stop_regex1,
            "stop_regex2" : stop_regex2 ,                 


        },
    )


    sql_rows_result = []
    max_rows_reached = False
    cpt_rows = 0
    max_rows = 50000
    
    for row_cursor in result:
        cpt_rows = cpt_rows + 1

        if max_rows_reached:
            continue
        if cpt_rows < max_rows :
            row = row_cursor._asdict()
            sql_rows_result.append(row)

    return sql_rows_result


####################################################################
####################################################################
####################################################################
def create_date_time ( p_date_str, p_interval):

    result_dt = dt_util.parse_datetime(f"{p_date_str} {'00:00'}")

    l_interval = p_interval
    
    #add number of "full days" in the interval ...
    while l_interval >= dt.timedelta(days=1): 
        l_interval = l_interval - dt.timedelta(days=1)
        result_dt = result_dt + dt.timedelta(days=1)

    # remaining  interval is ... the Time!!!
    result_dt =  result_dt + l_interval
    return result_dt


####################################################################
####################################################################
####################################################################
def time_delta_to_HH_MM ( p_time_delta):            
    minutes = p_time_delta / dt.timedelta(minutes=1)
    hour = minutes // 60
    min  = minutes %  60
    return "%02d:%02d" % (hour, min) 



####################################################################
####################################################################
####################################################################
def sensor_2_next_departures_postgresql( 
    engine      ,
    feed_id     , 
    now_date    ,
    now_time    ,
    time_zone   ,
    stop_regex1 ,
    stop_regex2 ,
    timerange   ,
    offset      ,
    caller_info ):


    _LOGGER.debug(f"sensor_2_next_departures_postgresql time_zone={time_zone} now_date={now_date}, now_time={now_time} caller_info = {caller_info}")
    start_timer = get_now_utc()


    # connect to the DB.. 
    gtfs_conn = engine.connect()



    p_now         =  dt_util.parse_datetime(f"{now_date} {now_time}")
    if p_now == None:
        _LOGGER.error(f"Error converting {now_date} {now_time} into datetime format")
        return
    p_now         =  p_now.replace(tzinfo=time_zone)

        

    from_utc      = p_now  + dt.timedelta(minutes=offset)

    from_datetime = p_now
    from_datetime = from_datetime.replace(tzinfo=None) + dt.timedelta(minutes=offset)
    from_date     = from_datetime.strftime(dt_util.DATE_STR_FORMAT)
    from_time     = from_datetime.strftime(TIME_STR_FORMAT)
    from_dayname  = from_datetime.strftime("%A").lower()

    to_utc        = p_now  + dt.timedelta(minutes=offset) + dt.timedelta(minutes= timerange)
 
    to_datetime   = from_datetime  + dt.timedelta(minutes= timerange)
    to_date       = to_datetime.strftime(dt_util.DATE_STR_FORMAT)
    to_time       = to_datetime.strftime(TIME_STR_FORMAT)
    to_dayname    = to_datetime.strftime("%A").lower()


    result = []
    
    ###################################################
    # BEGIN CASE 1 : SAME DAY ( date_from = date_to)
    if from_date == to_date:
            

        # CASE 1A
        #
        # retreive info valid yesterday, with arrival/departure time > 24h (today)
        # => retreive time range 24:00 - 48h00 ?? 
        #      yes we could, and filter result date afterward
        #      OR
        #        if we search for time range 1am-2am, we could just add 24h
        #        to search transport started yesterday, duration 25h- 26h
        #        
        
        result_tmp = []

        p_date = from_datetime - dt.timedelta(days=1)
        p_date_str = p_date.strftime(dt_util.DATE_STR_FORMAT)

        if ( False ):
            # old version    
            result_tmp = next_departures_postgresql_single_day (
                gtfs_conn  = gtfs_conn, 
                feed_id    = feed_id, 
                p_date     = p_date_str,  
                dayname    = p_date.strftime("%A").lower(),
                stop_regex1  = stop_regex1,
                stop_regex2  = stop_regex2 ,                 
                p_from_time = '24:00' , 
                p_to_time   = '48:00' )
        else:
            time_from = dt.timedelta(days=1) + dt.timedelta(hours=from_datetime.hour) +  dt.timedelta(minutes=from_datetime.minute) 
            time_to   = dt.timedelta(days=1) + dt.timedelta(hours=to_datetime.hour)   +  dt.timedelta(minutes=to_datetime.minute)             
            time_from_str = time_delta_to_HH_MM ( time_from )
            time_to_str   = time_delta_to_HH_MM ( time_to )

            result_tmp = next_departures_postgresql_single_day (
                gtfs_conn  = gtfs_conn, 
                feed_id    = feed_id, 
                p_date     = p_date_str,  
                dayname    = p_date.strftime("%A").lower(),
                stop_regex1  = stop_regex1,
                stop_regex2  = stop_regex2 ,                 
                p_from_time = time_from_str , 
                p_to_time   = time_to_str )

        _LOGGER.debug(f"CASE 1A: date: { p_date_str} {time_from_str} -> {time_to_str}:   {len (result_tmp)} rows ")

        for row_cursor in result_tmp:   
            depart_time_corrected = create_date_time( 
                p_date_str = p_date_str,
                p_interval = row_cursor["departure_time"])
            depart_time_corrected = depart_time_corrected.replace(tzinfo=time_zone)
            if depart_time_corrected >= from_utc  and depart_time_corrected <=to_utc :
                row_cursor["depart_time_corrected"] = depart_time_corrected
                result.append(row_cursor)
                _LOGGER.debug(f"CASE 1A: found entry where route started yesterday ")
            else:
                _LOGGER.debug(f"CASE 1A: depart_time_corrected {depart_time_corrected } not in {from_utc} - { to_utc} ")


        # CASE 1B: usual case, start and stop today
        #
        # 
        result_tmp = []
        p_date = from_datetime
        p_date_str = p_date.strftime(dt_util.DATE_STR_FORMAT)

        result_tmp = next_departures_postgresql_single_day (
            gtfs_conn  = gtfs_conn, 
            feed_id    = feed_id, 
            p_date     = p_date_str, 
            dayname    = p_date.strftime("%A").lower(),
            stop_regex1  = stop_regex1,
            stop_regex2  = stop_regex2 ,                 
            p_from_time = from_time , 
            p_to_time   = to_time )

        _LOGGER.debug(f"CASE 1B: date: { p_date_str} {from_time} -> {to_time}:   {len (result_tmp)} rows ")

        for row_cursor in result_tmp:   
            depart_time_corrected = create_date_time( 
                p_date_str  = p_date_str,
                p_interval = row_cursor["departure_time"])
            depart_time_corrected = depart_time_corrected.replace(tzinfo=time_zone)
            if depart_time_corrected >= from_utc  and depart_time_corrected <=to_utc :
                row_cursor["depart_time_corrected"] = depart_time_corrected
                result.append(row_cursor)
 #               _LOGGER.debug(f"CASE 1B: found entry  ")
            else:
                _LOGGER.debug(f"CASE 1B: depart_time_corrected {depart_time_corrected } not in {from_utc} - { to_utc} ")
        result_tmp = []            
    # END CASE 1

    ###################################################
    # BEGIN CASE 2 : from_date != to_date ( accross midnight) 
    if from_date != to_date:


    # Assumption: if from_date != to_date:
    #    => query is made at the end of the day ( > 20h00?)
    #    => we don't need to search for a trip started yesterday
    #    => won't work for routes during more than +/-20 hours (boat?/orient express... ??? )  


        # CASE2A: get all from totay till midnight
        result_tmp = []
        p_date = from_datetime
        p_date_str = p_date.strftime(dt_util.DATE_STR_FORMAT)
        result_tmp = next_departures_postgresql_single_day (
            gtfs_conn  = gtfs_conn, 
            feed_id    = feed_id, 
            p_date     = p_date_str, 
            dayname    = p_date.strftime("%A").lower(),
            stop_regex1  = stop_regex1,
            stop_regex2  = stop_regex2 ,                 
            p_from_time = from_time , 
            p_to_time   = "24:00" )

        _LOGGER.debug(f"CASE 2A: date: { p_date_str} { from_time} -> 24:00   { len (result_tmp) } rows ")

        for row_cursor in result_tmp:   
            depart_time_corrected = create_date_time( 
                p_date_str  = p_date_str,
                p_interval = row_cursor["departure_time"])
            depart_time_corrected = depart_time_corrected.replace(tzinfo=time_zone)
            if depart_time_corrected >= from_utc  and depart_time_corrected <=to_utc :
                row_cursor["depart_time_corrected"] = depart_time_corrected
                result.append(row_cursor)
 #               _LOGGER.debug(f"CASE 1B: found entry  ")
            else:
                _LOGGER.debug(f"CASE 2A: depart_time_corrected {depart_time_corrected } not in {from_utc} - { to_utc} ")

        result_tmp = []            



        # CASE2B: tomorrow: Transport started totay, and continues after mignight. Thus with hours >24:00)
        result_tmp = []
        p_date = from_datetime
        p_date_str = p_date.strftime(dt_util.DATE_STR_FORMAT)

        time_to   = dt.timedelta(days=1) + dt.timedelta(hours=to_datetime.hour)   +  dt.timedelta(minutes=to_datetime.minute) 
        time_to_str   = time_delta_to_HH_MM ( time_to )
        

        result_tmp = next_departures_postgresql_single_day (
            gtfs_conn  = gtfs_conn, 
            feed_id    = feed_id, 
            p_date     = p_date_str, 
            dayname    = p_date.strftime("%A").lower(),
            stop_regex1  = stop_regex1,
            stop_regex2  = stop_regex2 ,                 
            p_from_time = "24:00" , 
            p_to_time   = time_to_str )

        _LOGGER.debug(f"CASE 2B: date: { p_date_str}  24:00 -> { time_to_str}   { len (result_tmp) } rows ")


        for row_cursor in result_tmp:   
            depart_time_corrected = create_date_time( 
                p_date_str  = p_date_str,
                p_interval = row_cursor["departure_time"])
            depart_time_corrected = depart_time_corrected.replace(tzinfo=time_zone)
            if depart_time_corrected >= from_utc  and depart_time_corrected <=to_utc :
                row_cursor["depart_time_corrected"] = depart_time_corrected
                result.append(row_cursor)
 #               _LOGGER.debug(f"CASE 1B: found entry  ")
            else:
                _LOGGER.debug(f"CASE 2B: depart_time_corrected {depart_time_corrected } not in {from_utc} - { to_utc} ")
        result_tmp = []            

        # CASE2C: tomorrow: Transport started tomorrow. Thus with hours <24:00)
        result_tmp = []
        p_date = to_datetime
        p_date_str = p_date.strftime(dt_util.DATE_STR_FORMAT)
        result_tmp = next_departures_postgresql_single_day (
            gtfs_conn  = gtfs_conn, 
            feed_id    = feed_id, 
            p_date     = p_date_str , 
            dayname    = p_date.strftime("%A").lower(),
            stop_regex1  = stop_regex1,
            stop_regex2  = stop_regex2 ,                 
            p_from_time = '00:00' , 
            p_to_time   = to_time )

        _LOGGER.debug(f"CASE 2C: date: { p_date_str}  00:00 -> { to_time}   { len (result_tmp) } rows ")


        for row_cursor in result_tmp:   
            depart_time_corrected = create_date_time( 
                p_date_str  = p_date_str,
                p_interval = row_cursor["departure_time"])
            depart_time_corrected = depart_time_corrected.replace(tzinfo=time_zone)
            if depart_time_corrected >= from_utc  and depart_time_corrected <=to_utc :
                row_cursor["depart_time_corrected"] = depart_time_corrected
                result.append(row_cursor)
 #               _LOGGER.debug(f"CASE 1B: found entry  ")
            else:
                _LOGGER.debug(f"CASE 2C: depart_time_corrected {depart_time_corrected } not in {from_utc} - { to_utc} ")
        result_tmp = []            

    # END CASE 2



    ###################################################
    # Processing of entries in tables  : from_date != to_date ( accross midnight) 



    timetable = []
    local_stops_list = []
    prev_stop_id = ""
    prev_entry = entry = {}
 






    unique_stop_id = []
    for row_line in result :
        if row_line["from_stop_id"] not in unique_stop_id:
            unique_stop_id.append(row_line["from_stop_id"])

    count_row_stop = 0
    count_sql_rows_result=0

    for row in result:
        count_sql_rows_result = count_sql_rows_result + 1
    departures_returned = 0

    for row_unique_stop_id in unique_stop_id:
        timetable = []
        count_row_stop = count_row_stop + 1
        row_stop_from = get_stop_details ( gtfs_conn, feed_id ,row_unique_stop_id  )
        for row in result:
            if row['from_stop_id'] == row_unique_stop_id :
 #               _LOGGER.debug("processing stop = %S ", row['stop_id'])

                row_stop_to = get_stop_details ( gtfs_conn, feed_id ,row['to_stop_id']  )

                end_stop = {}
                end_stop["stop_id"]=""
                end_stop["stop_name"]=""

                end_stop = get_end_stop_of_trip(
                    gtfs_conn = gtfs_conn, 
                    feed_id = feed_id, 
                    trip_id = row["trip_id"], 
                    direction_id = row["direction_id"] )

                departures_returned = departures_returned + 1
                timetable.append({ 
                    "departure":     row["depart_time_corrected"], 
                    "route":         row["route_short_name"], 
                    "to_stop_id":    row_stop_to["stop_id"],
                    "to_stop_name":  row_stop_to["stop_name"],

                    "end_stop_id":   end_stop["stop_id"],
                    "end_stop_name": end_stop["stop_name"],
                    "headsign":      row["trip_headsign"], 
                    "trip_id":       row["trip_id"], 
                    "direction_id":  row["direction_id"], 
                    "icon":          ICONS.get(row['route_type'], ICON) })

        entry = {
            "stop_id":    row_stop_from['stop_id'], 
            "stop_name":  row_stop_from['stop_name'], 
            "latitude":   row_stop_from['stop_lat'], 
            "longitude":  row_stop_from['stop_lon'], 
            "departure":  timetable, 
            "offset":     offset}
            
        local_stops_list.append(entry)


    data_returned = local_stops_list   
    stop_timer = get_now_utc()
    elapsed = stop_timer - start_timer

    _LOGGER.info("next_departures_postgresql END: In %dms Processed %d stops %d departures. Returned %d departures", elapsed / dt.timedelta(milliseconds=1), count_row_stop, count_sql_rows_result, departures_returned )

    #close connection to the DB..
    gtfs_conn.close()


    return data_returned


