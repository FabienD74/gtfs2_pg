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

    time_range = str('+' + str( timerange) + ' minute')
    time_range_history = str('-' + str(self._data.get("timerange_history", DEFAULT_LOCAL_STOP_TIMERANGE_HISTORY)) + ' minute')

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
        and ((datetime(date(:now_offset) || ' ' || time(st.departure_time) ) between  datetime(:now_offset,:timerange_history) and  datetime(:now_offset,:timerange))
        or (datetime(date(:now_offset,'+1 day') || ' ' || time(st.departure_time) ) between  datetime(:now_offset,:timerange_history) and  datetime(:now_offset,:timerange)))
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
        and ((datetime(date(:now_offset) || ' ' || time(st.departure_time) ) between  datetime(:now_offset,:timerange_history) and  datetime(:now_offset,:timerange))
        or (datetime(date(:now_offset,'+1 day') || ' ' || time(st.departure_time) ) between  datetime(:now_offset,:timerange_history) and  datetime(:now_offset,:timerange)))
        {tomorrow_calendar_date_where}
        )
        order by stop_id, tomorrow, departure_time
        """  # noqa: S608



    db_connection = engine.connect()
    result = db_connection.execute(
        sqlalchemy.sql.text(sql_query),
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


