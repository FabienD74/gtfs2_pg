


from __future__ import annotations

"""Support for GTFS."""
#from datetime import datetime, timezone
import datetime  as dt
import logging
import math
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
@cachetools.func.ttl_cache(maxsize=2048, ttl=3600)  # Note: TTL value is in seconds.
def get_stops_arround_gps(gtfs_conn, feed_id , longitude, latitude,radius):
# return table of rows 

#    _LOGGER.info(f"get_stops_arround_gps feed_id= {feed_id} longitude={longitude} latitude={latitude} radius={radius}")

#    angle = ( 360.0 *  radius ) / ( 40000.0 * 1000.0 )
    area_min_longitude = 0
    area_min_latitude  = 0
    area_max_longitude = 0
    area_max_latitude = 0

#    area_min_longitude = longitude - angle
#    area_max_longitude = longitude + angle
#    area_min_latitude = latitude - angle
#    area_max_latitude = latitude + angle


    sizeLat = (radius * 360.0) / (40075 * 1000.0);    # degree of latitude shift
    radian= (latitude * math.pi ) / 180;             # conversion to radian
    sizeLong = sizeLat / math.cos(radian);                 # conversion to degrees of longitude shift

#longitude - sizeLong // formula for calculating longitude Min (original longitude minus shift)
#longitude + sizeLong // formula for calculating longitude Max (original longitude plus shift)
#latitude - size // formula for calculating latitude Min (initial latitude minus shift)
#latitude + size // formula for calculating latitude Max (original latitude plus shift)
 
    area_min_longitude = longitude - sizeLong
    area_max_longitude = longitude + sizeLong
    area_min_latitude = latitude - sizeLat
    area_max_latitude = latitude + sizeLat

    # north pole
    if area_max_latitude > 90:
        area_max_latitude = 90

    # south pole
    if area_min_latitude < -90:
        area_min_latitude = -90


    if area_max_longitude > 180:
        area_max_longitude = 180

    if area_min_longitude < -180:
        area_min_longitude = -180

    latitude_longitude_where_clause = f"""
        ( ( stop.stop_lat >={area_min_latitude}) and ( stop.stop_lat <={area_max_latitude})) and
        ( ( stop.stop_lon >={area_min_longitude}) and ( stop.stop_lon <={area_max_longitude}))
        """  # noqa: S608


    # get stops within the square area:

 #   gtfs_conn = engine.connect()

    sql_query = f"""
        SELECT stop.stop_id, stop.stop_name,stop.stop_lat as latitude, stop.stop_lon as longitude 
            FROM stops stop
            where
                feed_id = :feed_id and
                {latitude_longitude_where_clause}
            order by stop.stop_id asc
    """  # noqa: S608
    sql_result = gtfs_conn.execute(
        sqlalchemy.sql.text(sql_query),
        {
            "feed_id" : feed_id,
        },
    )        

    result = []

    # filter stops rows on real distance
    sql_rows_stops = []
    count_sql_rows = 0
    id_list = ""
    defined_radius = radius
    for row_cursor in sql_result:
        row = row_cursor._asdict()
        coords_1 = (latitude ,longitude)
        coords_2 = (row['latitude'], row['longitude'])
        distance_meters = geopy.distance.geodesic(coords_1, coords_2).meters 
        if distance_meters <= radius :
            #_LOGGER.info("Row added ( %d <  %d meters): %s",distance_meters, defined_radius, row)
            row["distance"] = distance_meters 
            result.append(row)
            count_sql_rows = count_sql_rows + 1    

    return result



####################################################################
####################################################################
####################################################################
def next_departures_postgresql_single_day (gtfs_conn , feed_id, p_date, dayname, sql_stop_id_list , p_from_time, p_to_time):


    # retreive routes excluding exception_type = 2
    sql_query = f"""
        select * from (
        (SELECT st.stop_id as stop_id, trip.trip_id, trip.trip_headsign, trip.direction_id, 
            (:p_date)::date    as departure_date,
            st.departure_time  as departure_time,
            route.route_id, 
            route.route_long_name,route.route_short_name,route.route_type,
            0 as exception_type 
        FROM trips trip
        INNER JOIN calendar as calendar
                ON  calendar.feed_id    = :feed_id and
                    calendar.service_id = trip.service_id 
        INNER JOIN stop_times as st
                ON  st.feed_id = :feed_id  and 
                    st.trip_id = trip.trip_id 
        INNER JOIN routes as route
                ON  route.feed_id  = :feed_id  and
                    route.route_id = trip.route_id  
        WHERE 
            trip.feed_id = :feed_id and
            st.stop_id in {sql_stop_id_list} 
            and ( calendar.start_date <= :p_date and calendar.end_date >= :p_date )
            and ( calendar.{dayname} = true )
            and ( st.departure_time  >=  interval :p_from_time ) 
            and ( st.departure_time  <=  interval :p_to_time   ) 
            and trip.service_id not in (select service_id 
                                        from calendar_dates as cd2 
                                        where cd2.feed_id = :feed_id and
                                            cd2.date = date(:p_date) and
                                            cd2.service_id = trip.service_id and 
                                            cd2.exception_type = 2)
        ) 
        UNION ALL
        (SELECT st.stop_id as stop_id, trip.trip_id, trip.trip_headsign, trip.direction_id, 
            (:p_date)::date    as departure_date,
            st.departure_time  as departure_time,
            route.route_id, 
            route.route_long_name,route.route_short_name,route.route_type,
            1 as exception_type
        FROM trips trip
        INNER JOIN calendar_dates as calendar_dates
                ON  trip.feed_id    = calendar_dates.feed_id and
                    trip.service_id = calendar_dates.service_id
        INNER JOIN stop_times as st
                ON  trip.feed_id = st.feed_id and
                    trip.trip_id = st.trip_id
        INNER JOIN routes as route
                ON  trip.feed_id   = route.feed_id and
                    trip.route_id  = route.route_id  
        WHERE trip.feed_id = :feed_id 
            and st.stop_id in {sql_stop_id_list}
            and  calendar_dates.exception_type = 1
            and ( calendar_dates.date = :p_date )
            and ( st.departure_time  >=  interval :p_from_time ) 
            and ( st.departure_time  <=  interval :p_to_time   ) 
        ) 
        )as MY_SELECT_UNION order by stop_id asc, departure_date asc, departure_time asc 
            
        """  # noqa: S608

    result = gtfs_conn.execute(
        sqlalchemy.sql.text(sql_query),
        {
            "feed_id"    : feed_id,
            "p_date": p_date,
            "p_from_time": p_from_time,
            "p_to_time"  : p_to_time,

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
def next_departures_postgresql(engine,
    feed_id, 
    now_date, 
    now_time, 
    time_zone,
    longitude, 
    latitude, 
    radius, 
    timerange,
    offset,
    caller_info):

    _LOGGER.debug(f"next_departures_postgresql time_zone={time_zone} now_date={now_date}, now_time={now_time} caller_info = {caller_info}")


    start_timer = get_now_utc()


    _LOGGER.debug("next_departures_postgresql BEGIN:")


    if not latitude or not longitude:
        _LOGGER.error("No latitude and/or longitude for : %s", self._data['device_tracker_id'])
        return []
    
    # connect to the DB.. 
    gtfs_conn = engine.connect()



    lt_stops = get_stops_arround_gps(
        gtfs_conn = gtfs_conn, 
        feed_id = feed_id,
        longitude = longitude ,
        latitude  = latitude,
        radius    = radius)

    id_list = ""
    for row in lt_stops:
        if id_list == "":
            id_list =  "'"+ row["stop_id"] + "'"
        else:
            id_list = id_list + ", '"+ row["stop_id"] + "'"

    if id_list == "":
        id_list = "('')" 
    else:
        id_list = "(" + id_list + ")"
#    _LOGGER.info("stop_id within %d metres = : %s", radius,  id_list)





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
                sql_stop_id_list = id_list, 
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
                sql_stop_id_list = id_list, 
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
            sql_stop_id_list = id_list, 
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
            sql_stop_id_list = id_list, 
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
            sql_stop_id_list = id_list, 
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
            sql_stop_id_list = id_list, 
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
 



    count_row_stop=0
    for row in lt_stops:
        count_row_stop = count_row_stop + 1

    count_sql_rows_result=0
    for row in result:
        count_sql_rows_result = count_sql_rows_result + 1

    departures_returned = 0

    for row_stop in lt_stops:
        timetable = []
        for row in result:
            if row['stop_id'] == row_stop["stop_id"]:
 #               _LOGGER.debug("processing stop = %S ", row['stop_id'])

                end_stop = {}
                end_stop["stop_id"]=""
                end_stop["stop_name"]=""

                end_stop = get_end_stop_of_trip(
                    gtfs_conn = gtfs_conn, 
                    feed_id = feed_id, 
                    trip_id = row["trip_id"], 
                    direction_id = row["direction_id"] )

                if row_stop['stop_id'] != end_stop['stop_id'] : 
                    # we search for departures only, if current stop is the terminus, we skip! ;-)
                    departures_returned = departures_returned + 1
                    timetable.append({ 
    #                            "date":          row["departure_date"], 
    #                    "route_long":    row["route_long_name"], 
    #                            "departure_realtime": departure_rt, 
    #                            "delay_realtime": delay_rt,  
                        "departure":     row["depart_time_corrected"], 
                        "route":         row["route_short_name"], 
                        "end_stop_id":   end_stop["stop_id"],
                        "end_stop_name": end_stop["stop_name"],
                        "headsign":      row["trip_headsign"], 
                        "trip_id":       row["trip_id"], 
                        "direction_id":  row["direction_id"], 
                        "icon":          ICONS.get(row['route_type'], ICON) })

        entry = {"stop_id": row_stop['stop_id'], 
            "distance": row_stop["distance"], 
            "stop_name": row_stop['stop_name'], 
            "latitude": row_stop['latitude'], 
            "longitude": row_stop['longitude'], 
            "departure": timetable, 
            "offset": offset}
            
        local_stops_list.append(entry)


    data_returned = local_stops_list   
    stop_timer = get_now_utc()
    elapsed = stop_timer - start_timer

    _LOGGER.info("next_departures_postgresql END: In %dms Processed %d stops %d departures. Returned %d departures", elapsed / dt.timedelta(milliseconds=1), count_row_stop, count_sql_rows_result, departures_returned )

    #close connection to the DB..
    gtfs_conn.close()


    return data_returned


