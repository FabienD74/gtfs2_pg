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
def next_departures_sqlite(self,engine):

    start_timer = get_now_utc()
    _LOGGER.debug("next_departures_sqlite: %s", self._data)

    if self.hass.config.time_zone is None:
        _LOGGER.error("Timezone is not set in Home Assistant configuration")
        timezone = "UTC"
    else:
        timezone=dt_util.get_time_zone(self.hass.config.time_zone)
#    if check_extracting(self.hass, self._data['gtfs_dir'],self._data['file']):
#        _LOGGER.warning("Cannot get next depurtures on this datasource as still unpacking: %s", self._data["file"])
#        return {}
    """Get next departures from data."""
#    schedule = self._data["schedule"]
    offset = self._data["offset"]

    now = dt_util.now().replace(tzinfo=None) + dt.timedelta(minutes=offset)

    now_hist_corrected = dt_util.now().replace(tzinfo=None) + dt.timedelta(minutes=offset) - dt.timedelta(minutes=DEFAULT_LOCAL_STOP_TIMERANGE)
    now_date = now.strftime(dt_util.DATE_STR_FORMAT)
    now_time = now.strftime(TIME_STR_FORMAT)
    now_time_hist_corrected = now_hist_corrected.strftime(TIME_STR_FORMAT)
    tomorrow = now + dt.timedelta(days=1)
    tomorrow_date = tomorrow.strftime(dt_util.DATE_STR_FORMAT)    
    device_tracker = self.hass.states.get(self._data['device_tracker_id']) 
    latitude = device_tracker.attributes.get("latitude", None)
    longitude = device_tracker.attributes.get("longitude", None)
    include_tomorrow = self._data.get("include_tomorrow", True)   
    tomorrow_select = tomorrow_select2 = tomorrow_where = tomorrow_order = ""
    tomorrow_calendar_date_where = f"AND (calendar_date_today.date = date(:now_offset))"

    timerange = self._data.get("timerange", DEFAULT_LOCAL_STOP_TIMERANGE)
    timerange = 120

    time_range = str('+' + str( timerange) + ' minute')
    time_range_history = str('-' + str(self._data.get("timerange_history", DEFAULT_LOCAL_STOP_TIMERANGE_HISTORY)) + ' minute')
#    radius = self._data.get(CONF_RADIUS, DEFAULT_LOCAL_STOP_RADIUS) / 1111111

    radius = ( 360 * self._data.get(CONF_RADIUS, DEFAULT_LOCAL_STOP_RADIUS) ) / ( 40000 * 1000 )


    if not latitude or not longitude:
        _LOGGER.error("No latitude and/or longitude for : %s", self._data['device_tracker_id'])
        return []
    if include_tomorrow:
        _LOGGER.debug("Includes Tomorrow")
        tomorrow_name = tomorrow.strftime("%A").lower()
        tomorrow_select = f"calendar.{tomorrow_name} AS tomorrow,"
        tomorrow_calendar_date_where = f"AND (calendar_date_today.date = date(:now_offset) or calendar_date_today.date = date(:now_offset,'+1 day'))"
        tomorrow_select2 = f"CASE WHEN date(:now_offset) < calendar_date_today.date THEN '1' else '0' END as tomorrow,"        

    sql_query = f"""
        SELECT * FROM (
        SELECT stop.stop_id, stop.stop_name,stop.stop_lat as latitude, stop.stop_lon as longitude, trip.trip_id, trip.trip_headsign, trip.direction_id, time(st.departure_time) as departure_time,
               route.route_long_name,route.route_short_name,route.route_type,
               calendar.{now.strftime("%A").lower()} AS today,
               {tomorrow_select}
               calendar.start_date AS start_date,
               calendar.end_date AS end_date,
               date(:now_offset) as calendar_date,
               0 as today_cd, 
               route.route_id
        FROM trips trip
        INNER JOIN calendar calendar
                   ON trip.service_id = calendar.service_id
        INNER JOIN stop_times st
                   ON trip.trip_id = st.trip_id
        INNER JOIN stops stop
                   on stop.stop_id = st.stop_id and abs(stop.stop_lat - :latitude) < :radius and abs(stop.stop_lon - :longitude) < :radius
        INNER JOIN routes route
                   ON route.route_id = trip.route_id 
		WHERE 
        trip.service_id not in (select service_id from calendar_dates where date = date(:now_offset) and exception_type = 2)
        and  datetime(date(:now_offset) || ' ' || time(st.departure_time) ) between  datetime(:now_offset,:timerange_history) and  datetime(:now_offset,:timerange) 
        AND calendar.start_date <= date(:now_offset) 
        AND calendar.end_date >= date(:now_offset) 
        )
		UNION ALL
        SELECT * FROM (
	    SELECT stop.stop_id, stop.stop_name,stop.stop_lat as latitude, stop.stop_lon as longitude, trip.trip_id, trip.trip_headsign, trip.direction_id, time(st.departure_time) as departure_time,
               route.route_long_name,route.route_short_name,route.route_type,
               '0' AS today,
               {tomorrow_select2}
               date(:now_offset) AS start_date,
               date(:now_offset) AS end_date,
               calendar_date_today.date as calendar_date,
               calendar_date_today.exception_type as today_cd,
               route.route_id
        FROM trips trip
        INNER JOIN stop_times st
                   ON trip.trip_id = st.trip_id
        INNER JOIN stops stop
                   on stop.stop_id = st.stop_id and abs(stop.stop_lat - :latitude) < :radius and abs(stop.stop_lon - :longitude) < :radius
        INNER JOIN routes route
                   ON route.route_id = trip.route_id 
        INNER JOIN calendar_dates calendar_date_today
				   ON trip.service_id = calendar_date_today.service_id
		WHERE 
        today_cd = 1
        and  datetime(date(:now_offset) || ' ' || time(st.departure_time) ) between  datetime(:now_offset,:timerange_history) and  datetime(:now_offset,:timerange) 
		{tomorrow_calendar_date_where}
        )
        order by stop_id, tomorrow, departure_time
        """  # noqa: S608
    db_connection = engine.connect()
    result = db_connection.execute(
        text(sql_query),
        {
            "latitude": latitude,
            "longitude": longitude,
            "timerange": time_range,
            "timerange_history": time_range_history,
            "radius": radius,
            "now_offset": now
        },
    )        
    timetable = []
    local_stops_list = []
    prev_stop_id = ""
    prev_entry = entry = {}
    
    
    self._realtime = False;  #<<<<<<< FDES FORCED

    # Set elements for realtime retrieval via local file.
    if self._realtime:
        self._rt_group = "trip"
        self._rt_data = {
            "url": self._trip_update_url,
            "headers": self._headers,
            "file": self._data["name"] + "_localstop",
            }
        check = get_gtfs_rt(self.hass,DEFAULT_PATH_RT,self._rt_data)
        # check if local file created
        if check != "ok":
            _LOGGER.error("Could not download RT data from: %s", self._trip_update_url)
            return False
        else:
            # use local file created as new url
            self._trip_update_url = "file://" + DEFAULT_PATH_RT + "/" + self._data["name"] + "_localstop.rt"

    for row_cursor in result:
        row = row_cursor._asdict()
#        _LOGGER.debug("Row from query: %s", row)
        if row["stop_id"] != prev_stop_id and prev_stop_id != "": 
            local_stops_list.append(prev_entry)
            timetable = []
        entry = {"stop_id": row['stop_id'], "stop_name": row['stop_name'], "latitude": row['latitude'], "longitude": row['longitude'], "departure": timetable, "offset": offset}
        self._icon = ICONS.get(row['route_type'], ICON)
        if row["today"] == 1 or (row["today_cd"] == 1 and row["start_date"] == row["calendar_date"]):
            self._trip_id = row["trip_id"]
            self._direction = str(row["direction_id"])
            self._route = row['route_id']   
            self._route_id = row['route_id'] 
            self._stop_id = row['stop_id']
            departure_rt = "-"
            delay_rt = "-"
            # Find RT if configured
            if self._realtime:
                self._get_next_service = {}
                _LOGGER.debug("Find rt for local stop route: %s - direction: %s - stop: %s", self._route , self._direction, self._stop_id)
                next_service = get_rt_route_trip_statuses(self)
                
                if next_service:                       
                    delays = next_service.get(self._route, {}).get(self._direction, {}).get(self._stop_id, []).get("delays", [])
                    departures = next_service.get(self._route, {}).get(self._direction, {}).get(self._stop_id, []).get("departures", [])
                    delay_rt = delays[0] if delays else "-"
                    departure_rt = departures[0] if departures else "-"
                    
            if departure_rt != '-':
                depart_time_corrected = departures[0]
#                _LOGGER.debug("Departure time: %s, corrected with delay timestamp: %s", dt_util.parse_datetime(f"{now_date} {row["departure_time"]}").replace(tzinfo=timezone), depart_time_corrected)
            else: 
                depart_time_corrected = dt_util.parse_datetime(f"{now_date} {row["departure_time"]}").replace(tzinfo=timezone)
            if delay_rt != '-':
                depart_time_corrected = dt_util.parse_datetime(f"{now_date} {row["departure_time"]}").replace(tzinfo=timezone) + datetime.timedelta(seconds=delay_rt)
#                _LOGGER.debug("Departure time: %s, corrected with delay: %s", dt_util.parse_datetime(f"{now_date} {row["departure_time"]}").replace(tzinfo=timezone), depart_time_corrected)
            else:
                depart_time_corrected = dt_util.parse_datetime(f"{now_date} {row["departure_time"]}").replace(tzinfo=timezone)                

#            _LOGGER.debug("Departure time: %s", depart_time_corrected)   
            if depart_time_corrected > now.replace(tzinfo=timezone): 
#                _LOGGER.debug("Departure time corrected: %s, after now: %s", depart_time_corrected, now.replace(tzinfo=timezone))
                timetable.append({"departure": row["departure_time"], "departure_realtime": departure_rt, "delay_realtime": delay_rt, "date": now_date, "stop_name": row['stop_name'], "route": row["route_short_name"], "route_long": row["route_long_name"], "headsign": row["trip_headsign"], "trip_id": row["trip_id"], "direction_id": row["direction_id"], "icon": self._icon})
        
        if (
            "tomorrow" in row
            and row["tomorrow"] == 1
            and row["today"] == 0
            and now_time_hist_corrected > row["departure_time"]            
        ):
#            _LOGGER.debug("Tomorrow adding row_tomorrow: %s, row_today: %s, now_time: %s, departure_time: %s", row["tomorrow"],row["today"],now_time_hist_corrected,row["departure_time"])
            timetable.append({"departure": row["departure_time"], "departure_realtime": "tomorrow", "delay_realtime": "tomorrow", "date": tomorrow_date, "stop_name": row['stop_name'], "route": row["route_short_name"], "route_long": row["route_long_name"], "headsign": row["trip_headsign"], "trip_id": row["trip_id"], "direction_id": row["direction_id"], "icon": self._icon})
        
        prev_entry = entry.copy()
        prev_stop_id = str(row["stop_id"])
        entry["departure"] = timetable          

    if entry:      

        local_stops_list.append(entry)
    data_returned = local_stops_list   


    stop_timer = get_now_utc()
    elapsed = stop_timer - start_timer
    _LOGGER.info("get_local_stops_next_departures_old: In %dms Processed ", elapsed / dt.timedelta(milliseconds=1))

    db_connection.close()
    return data_returned




####################################################################
####################################################################
####################################################################
def get_feed_id(gtfs_conn):
    sql_query = f"""
        SELECT * FROM _feed
    """  # noqa: S608
    sql_result = gtfs_conn.execute(
        text(sql_query),
        {

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

    if True:
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
    else:
        sql_query = f"""
            SELECT * FROM stops
            where feed_id = :feed_id and
                stop_id = ( select stop_id from stop_times 
                                where   feed_id = :feed_id and
                                        trip_id = :trip_id and
                                        stop_sequence = ( select min (st2.stop_sequence)  
                                                        from stop_times st2
                                                        where feed_id = :feed_id and
                                                            trip_id = :trip_id) 

                ) 
        """  # noqa: S608


    sql_result = gtfs_conn.execute(
        text(sql_query),
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
def get_stops_arround_gps(gtfs_conn, feed_id , longitude, latitude,radius):
# return table of rows 

#    _LOGGER.info(f"get_stops_arround_gps feed_id= {feed_id} longitude={longitude} latitude={latitude} radius={radius}")

    angle = ( 360.0 *  radius ) / ( 40000.0 * 1000.0 )

    area_min_longitude = 0
    area_min_latitude  = 0
    area_max_longitude = 0
    area_max_latitude = 0

    area_min_longitude = longitude - angle
    area_max_longitude = longitude + angle
    area_min_latitude = latitude - angle
    area_max_latitude = latitude + angle

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
        text(sql_query),
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
        text(sql_query),
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
    size_returned = sys.getsizeof(data_returned, -1)
    stop_timer = get_now_utc()
    elapsed = stop_timer - start_timer

    _LOGGER.info("next_departures_postgresql END: In %dms Processed %d stops %d departures. Returned %d departures:, Returned %d bytes", elapsed / dt.timedelta(milliseconds=1), count_row_stop, count_sql_rows_result, departures_returned, size_returned )

    #close connection to the DB..
    gtfs_conn.close()


    return data_returned


####################################################################
####################################################################
####################################################################
def gtfs2_pg_sensor_1_get_data(pself, engine_type, engine,feed_id, now_date, now_time, time_zone, longitude, latitude, radius, timerange,offset, caller_info = ""):
    
    returned = None

    if engine_type == 'sqlite':
        returned =  next_departures_sqlite(pself, engine = engine)
        return returned

    if engine_type == 'postgresql':
        returned = next_departures_postgresql( 
            engine    = engine, 
            feed_id   = feed_id, 
            now_date  = now_date, 
            now_time  = now_time,
            time_zone = time_zone, 
            longitude = longitude , 
            latitude  = latitude, 
            radius    = radius, 
            timerange = timerange,
            offset    = offset,
            caller_info = caller_info)
            
        return returned


####################################################################
####################################################################
####################################################################
class gtfs2_pg_sensor_1(CoordinatorEntity, SensorEntity):

    """Implementation of a GTFS local stops departures sensor."""

###########################
### __INIT__
    def __init__( self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        coordinator ) -> None:

        _LOGGER.debug(f"gtfs2_pg_sensor_1.__init__: BEGIN")
        _LOGGER.debug(f"config_entry={config_entry}")
        _LOGGER.debug(f"hass={hass}")
        _LOGGER.debug(f"coordinator={coordinator}")

        """Initialize the GTFSsensor."""
        self.hass = hass
        self.config_entry = config_entry
        self.coordinator = coordinator

        super().__init__(coordinator)

        self._data= coordinator.data

        provided_name = coordinator.data.get(CONF_NAME, "No Name")
        _LOGGER.debug(f"provided_name={provided_name}")

        db_id = coordinator.entry.data.get("db_id", "N/A")
        _LOGGER.debug(f"db_id={db_id}")

        self._previous_longitude = -1
        self._previous_latitude = -1 
        self._longitude = -1
        self._latitude = -1 
        self._distance = -1 

        self._name =  provided_name + "_local_stoplist"
        self._attributes: dict[str, Any] = {}

        self._attr_unique_id = f"sensor.{DOMAIN}_{self._name}"
        self._attr_unique_id = self._attr_unique_id.lower()
        self._attr_unique_id = self._attr_unique_id.replace(" ", "_")
        self.entity_id = self._attr_unique_id

        self._attr_device_info = DeviceInfo(
            name=f"GTFS - {provided_name}",
            entry_type=DeviceEntryType.SERVICE,
            identifiers={(DOMAIN, f"GTFS - {provided_name}")},
            manufacturer="GTFS",
            model=provided_name,
        )
        self._state: str | None = None
        self._state = "Initialized"
            

        self._attributes["gtfs_updated_at"] = get_now_utc_iso_to_str()
        self._attributes[CONF_DEVICE_TRACKER_ID] = self._data.get(CONF_DEVICE_TRACKER_ID,None)
        self._attributes["offset"] = self._data.get('offset',None)
        self._attributes["latitude"] = self._latitude
        self._attributes["longitude"] = self._longitude
        self._attributes["movement_meters"] = self._distance
        self._attributes["stops"] = []

        self._attr_native_value = self._state

        async_track_state_change_event(
            self.hass, self.config_entry.data.get(CONF_DEVICE_TRACKER_ID,None), self.device_tracker_state_listener
        )
        self.hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STARTED, self.home_assistant_started
        )

        engine  = self.coordinator.gtfs_engine
        gtfs_conn = engine.connect()
        row_feed = get_feed_id(gtfs_conn)
        gtfs_conn.close()

        self.feed_id =  row_feed["feed_id"]

    ###########################
    def get_yaml_parameter (self, param_name):
        config_yaml = self.hass.data[DOMAIN].get("config_yaml")
 #       _LOGGER.debug(f"config_yaml = {config_yaml}")
        if config_yaml == None:
            return None

        param_value = config_yaml.get(param_name)  
#        _LOGGER.debug(f"param_value = {param_value}")

        return param_value

    ###########################
    async def home_assistant_started(self, event):
        _LOGGER.debug(f"gtfs2_pg_sensor_1.home_assistant_started: BEGIN")

        self._update_attrs(p_called_by = "home_assistant_started" )


    ###########################
    async def device_tracker_state_listener(self, event):
        _LOGGER.debug(f"gtfs2_pg_sensor_1.device_tracker_state_listener: BEGIN")

        """Handle  device state changes."""
        await self._new_device_tracker_state(state = event.data.get("new_state"))

    ###########################
    async def _new_device_tracker_state(self, state):
        _LOGGER.debug(f"gtfs2_pg_sensor_1._new_device_tracker_state: BEGIN")

        if state is not None and state not in (STATE_UNKNOWN, STATE_UNAVAILABLE) :
 #               _LOGGER.info("device_tracer has state: %s" ,state)
 #               _LOGGER.info("device_tracer has state.state: %s " ,state.state )
 #               _LOGGER.info("device_tracer has state.attributes: %s " ,state.attributes )
 #               _LOGGER.info("device_tracer has attribute latitude= %s " ,state.attributes["latitude"] )
 #               _LOGGER.info("device_tracer has attribute longitude= %s " ,state.attributes["longitude"] )

                self._update_attrs( p_force_update =False , p_called_by = "event/listner", p_longitude = state.attributes["longitude"] , p_latitude = state.attributes["latitude"])
        else:
            _LOGGER.info("device_tracer has an invalid value: %s. " ,state)



    ###########################
    @property
    def name(self) -> str:
        """Return the name of the sensor."""
        return self._name

    ###########################
    @callback
    def _handle_coordinator_update(self) -> None:
#        _LOGGER.debug(f"gtfs2_pg_sensor_1._handle_coordinator_update: BEGIN")
        self._update_attrs(p_called_by = "_handle_coordinator_update")
        super()._handle_coordinator_update()
        
    ###########################
    def _update_attrs(self, p_force_update = False , p_called_by = "Unknown", p_longitude = -1, p_latitude = -1):  # noqa: C901 PLR0911
#        _LOGGER.debug("gtfs2_pg_sensor_1._update_attrs BEGIN : %s", self._name)
#        _LOGGER.debug("gtfs2_pg_sensor_1._update_attrs .config_entry= %s", self.config_entry)
#        _LOGGER.debug("gtfs2_pg_sensor_1._update_attrs .hass= %s", self.hass )

        delta = get_now_utc() - get_utc_iso_str_to_datetime( self._attributes["gtfs_updated_at"] )

        if p_longitude == -1 and p_latitude == - 1 :
            # coordinates not received in parameters
            device_tracker = self.hass.states.get( self.config_entry.data.get(CONF_DEVICE_TRACKER_ID,None))
            self._latitude = device_tracker.attributes.get("latitude", None)
            self._longitude = device_tracker.attributes.get("longitude", None)

        else:
            self._latitude  = p_latitude
            self._longitude = p_longitude

        distance_meters = -1
        if ((self._previous_latitude  != -1) and (self._previous_longitude  != -1 )) : 
            coords_1 = (self._latitude ,self._longitude)
            coords_2 = (self._previous_latitude, self._previous_longitude)
            distance_meters = geopy.distance.geodesic(coords_1, coords_2).meters 


        update = p_force_update
        if update:
                _LOGGER.info("gtfs2_pg_sensor_1._update_attrs(caller=%s): %s, Update Forced", 
                    p_called_by,
                    self._name)

        if not update:
            if  ( self._state == "Initialized" ):
                _LOGGER.info("gtfs2_pg_sensor_1._update_attrs(caller=%s): %s, State 'Init' => update",p_called_by, self._name)
                update = True

        if not update:
            if  ( self._previous_longitude == -1 ) or ( self._previous_latitude == -1):
                _LOGGER.info("gtfs2_pg_sensor_1._update_attrs(caller=%s): %s, Logic Error => update", 
                    p_called_by,self._name)
                # this case should never happens
                # was never updated ???? (= State <> Initialized  and no GSP coordinates ?????)
                update = True

        if not update:
            if delta.total_seconds()  > self._data.get('refresh_max_seconds',0): 
                _LOGGER.info("gtfs2_pg_sensor_1._update_attrs(caller=%s): %s, data too old => update",
                    p_called_by, self._name)                
                # existing data are outdated
                update = True

        if not update:
            if distance_meters > self._data.get('refresh_min_distance',0):
                _LOGGER.info("gtfs2_pg_sensor_1._update_attrs(caller=%s): %s, GPS moved %d meters (%2.8f %2.8f) => (%2.8f %2.8f) => update", 
                    p_called_by,
                    self._name, 
                    distance_meters,
                    self._previous_latitude, 
                    self._previous_longitude, 
                    self._latitude, 
                    self._longitude)

                # GPS coordinates are changing
                update = True

        if not update:
            reason = f"p_force_update= {p_force_update}, distance {distance_meters} < {self._data.get('refresh_min_distance',0)},  oudated {delta.total_seconds()} < {self._data.get('refresh_max_seconds',0)}"
#            _LOGGER.debug("gtfs2_pg_sensor_1._update_attrs(caller=%s): %s, Update skipped, (reason= %s)", p_called_by, self._name, reason)
        else:
            
            if ( ( self._state != "Initialized" ) and  (delta.total_seconds() > 5 ) and (delta.total_seconds() < 30)):
                _LOGGER.warning("gtfs2_pg_sensor_1._update_attrs(caller=%s): %s, HIGH REFRESH RATE",
                    p_called_by, self._name)
            if  ( self._state != "Initialized" ) and  ( delta.total_seconds() < 5 ):
                _LOGGER.error("gtfs2_pg_sensor_1._update_attrs(caller=%s): %s, refresh rate TO HIGH. Max 1 every 5 seconds",
                    p_called_by, self._name)
            else:
                self._state = "Updated"
                self._attributes["latitude"] = self._latitude
                self._attributes["longitude"] = self._longitude
                self._attributes["movement_meters"] = distance_meters
                self._attributes["gtfs_updated_at"] = get_now_utc_iso_to_str()

                engine_type = self.coordinator.db_type

                if self.hass.config.time_zone is None:
                    _LOGGER.error("Timezone is not set in Home Assistant configuration")
                    time_zone = "UTC"
                else:
                    time_zone=dt_util.get_time_zone(self.hass.config.time_zone)

                force_now_date = self.get_yaml_parameter ("force_now_date")
                if force_now_date == None:
                    force_now_date = get_now_local_timezone(time_zone).strftime(dt_util.DATE_STR_FORMAT)

                force_now_time = self.get_yaml_parameter ("force_now_time")
                if force_now_time == None:
                    force_now_time = get_now_local_timezone(time_zone).strftime(TIME_STR_FORMAT)

                self._departure = gtfs2_pg_sensor_1_get_data(
                    pself       = self, 
                    engine_type = engine_type , 
                    engine      = self.coordinator.gtfs_engine,
                    feed_id     = self.feed_id, 
                    now_date    = force_now_date,
                    now_time    = force_now_time,
                    time_zone   = time_zone,
                    longitude   = self._longitude, 
                    latitude    = self._latitude, 
                    radius      = self._data.get(CONF_RADIUS, 1500), 
                    timerange   = self._data.get(CONF_TIMERANGE, 30),
                    offset      = self._data.get(CONF_OFFSET,0),
                    caller_info = f"sensor = {self._name}")

#                self._departure = gtfs2_pg_sensor_1_get_data(pself = self, engine_type = engine_type, engine = self.coordinator.gtfs_engine)
                if self._departure == None:
                    self._attributes["stops"] = []
                else:
                    self._attributes["stops"] = self._departure

                if self._longitude != -1 :
                    self._previous_longitude = self._longitude

                if self._latitude != -1 :
                    self._previous_latitude = self._latitude

                self._attr_native_value = self._state        
                self._attr_extra_state_attributes = self._attributes
                super()._handle_coordinator_update()

