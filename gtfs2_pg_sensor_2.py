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


#from .gtfs2_pg_sensor_2_sqlite import *
from .gtfs2_pg_sensor_2_postgres import *



####################################################################
####################################################################
####################################################################

def gtfs2_pg_sensor_2_get_data(
    engine_type , 
    engine      ,
    feed_id     , 
    now_date    ,
    now_time    ,
    time_zone   ,
    stop_regex1 ,
    stop_regex2 ,
    timerange   ,
    offset      ,
    caller_info = ""):

    
    returned = None

    if engine_type == 'sqlite':
 #       returned =  next_departures_sqlite(pself, engine = engine)
        return returned

    if engine_type == 'postgresql':
        returned = sensor_2_next_departures_postgresql( 
            engine      = engine,
            feed_id     = feed_id, 
            now_date    = now_date,
            now_time    = now_time,
            time_zone   = time_zone,
            stop_regex1 = stop_regex1,
            stop_regex2 = stop_regex2,
            timerange   = timerange,
            offset      = offset,
            caller_info = caller_info)

        return returned


####################################################################
####################################################################
####################################################################
class gtfs2_pg_sensor_2(CoordinatorEntity, SensorEntity):

    """Implementation of a GTFS local stops departures sensor."""

###########################
### __INIT__
    def __init__( self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        coordinator ) -> None:

        _LOGGER.debug(f"gtfs2_pg_sensor_1.__init__: BEGIN")
#        _LOGGER.debug(f"config_entry={config_entry}")
#        _LOGGER.debug(f"hass={hass}")
#        _LOGGER.debug(f"coordinator={coordinator}")

        """Initialize the GTFSsensor."""
        self.hass = hass
        self.config_entry = config_entry
        self.coordinator = coordinator
        super().__init__(coordinator)



        self._data= coordinator.data

        provided_name = coordinator.data.get(CONF_NAME, "No Name")
        _LOGGER.debug(f"provided_name={provided_name}")

        db_id = coordinator.entry.data.get("db_id", None)
        _LOGGER.debug(f"db_id={db_id}")

        feed_id = coordinator.entry.data.get("feed_id", None)
        _LOGGER.debug(f"feed_id={feed_id}")


        self._db_id = db_id
        self._feed_id = feed_id


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
            
        self._attributes["db_id"]         =  self._db_id
        self._attributes["feed_id"]       =  self._feed_id
        self._attributes["gtfs_updated_at"] = get_now_utc_iso_to_str()
        self._attributes["stop_regex1"]   = coordinator.entry.data.get("stop_regex1", "Unknown")
        self._attributes["stop_regex2"]   = coordinator.entry.data.get("stop_regex2", "Unknown")
        self._attributes["offset"]        = self._data.get('offset',None)
        self._attributes[CONF_TIMERANGE] = coordinator.data.get(CONF_TIMERANGE, 60)

        self._attributes["stops"]         = []

        self._attr_native_value = self._state


        self.hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STARTED, self.home_assistant_started
        )


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
            if delta.total_seconds()  > self._data.get('refresh_max_seconds',0): 
                _LOGGER.info("gtfs2_pg_sensor_1._update_attrs(caller=%s): %s, data too old => update",
                    p_called_by, self._name)                
                # existing data are outdated
                update = True


        if not update:
            reason = f"p_force_update= {p_force_update}, oudated {delta.total_seconds()} < {self._data.get('refresh_max_seconds',0)}"
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

                self._departure = None

                self._departure = gtfs2_pg_sensor_2_get_data(
                    engine_type = engine_type , 
                    engine      = self.coordinator.gtfs_engine,
                    feed_id     = self._feed_id, 
                    now_date    = force_now_date,
                    now_time    = force_now_time,
                    time_zone   = time_zone,
                    stop_regex1 = self._data["stop_regex1"],
                    stop_regex2 = self._data["stop_regex2"],
                    timerange   = self._data.get(CONF_TIMERANGE, 30),
                    offset      = self._data.get(CONF_OFFSET,0),
                    caller_info = f"sensor = {self._name}")

#                self._departure = gtfs2_pg_sensor_1_get_data(pself = self, engine_type = engine_type, engine = self.coordinator.gtfs_engine)
                if self._departure == None:
                    self._attributes["stops"] = []
                else:
                    self._attributes["stops"] = self._departure

                self._attr_native_value = self._state        
                self._attr_extra_state_attributes = self._attributes
                super()._handle_coordinator_update()

