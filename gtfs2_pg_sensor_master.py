"""Support for GTFS."""
#from datetime import datetime, timezone

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

from .const import *
from .coordinator import *
from .common import *

_LOGGER = logging.getLogger(__name__)




########################################################
class gtfs2_pg_sensor_master(CoordinatorEntity, SensorEntity):
########################################################
    
### __INIT__
    def __init__( self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        coordinator ) -> None:

        _LOGGER.debug(f"gtfs2_pg_sensor_master.__init__: BEGIN")
        _LOGGER.debug(f"config_entry={config_entry}")
        _LOGGER.debug(f"hass={hass}")
        _LOGGER.debug(f"coordinator={coordinator}")

        """Initialize the GTFSsensor."""
        self.hass = hass
        self.config_entry = config_entry
        self.coordinator = coordinator

        super().__init__(coordinator)
        provided_name = coordinator.data.get("name", "No Name")

        self._name =  provided_name
        self._attributes: dict[str, Any] = {}

        self._attr_unique_id = "sensor.gtfs2_pg_" + self._name
        self._attr_unique_id = self._attr_unique_id.lower()
        self._attr_unique_id = self._attr_unique_id.replace(" ", "_")
        self.entity_id = self._attr_unique_id

        self._attr_device_info = DeviceInfo(
            name=f"GTFS2 PG - {provided_name}",
            entry_type=DeviceEntryType.SERVICE,
            identifiers={(DOMAIN, f"GTFS2 PG - {provided_name}")},
            manufacturer="GTFS2 PG",
            model=provided_name,
        )
        self._state: str | None = None
        self._state = "Initialized"

        db_res = None
        try:
            db_conn = self.coordinator.master_engine.connect()
            db_sql = "select count (*) from db_config"
            db_res = db_conn.execute(sqlalchemy.sql.text(db_sql))
        except SQLAlchemyError as e:
            self._state = "Error with select count *"

        self._attr_native_value = self._state
        self._attributes["updated_at"] = get_now_utc_iso_to_str()
        self.hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STARTED, self.home_assistant_started
        )


    async def home_assistant_started(self, event):
        _LOGGER.debug(f"gtfs2_pg_sensor_master.home_assistant_started: BEGIN")
        self._update_attrs(p_called_by = "home_assistant_started" )

    @property
    def name(self) -> str:
        """Return the name of the sensor."""
        return self._name

    @callback
    def _handle_coordinator_update(self) -> None:
#        _LOGGER.debug(f"gtfs2_pg_sensor_master._handle_coordinator_update: BEGIN")
        self._update_attrs(p_called_by = "_handle_coordinator_update")
        super()._handle_coordinator_update()

    def _update_attrs(self, p_force_update = False , p_called_by = "Unknown", p_longitude = -1, p_latitude = -1):  # noqa: C901 PLR0911
#        _LOGGER.debug("gtfs2_pg_sensor_master._update_attrs (called by= %s) ", p_called_by)

        if self._state == "Initialized":
            delta = get_now_utc()- get_utc_iso_str_to_datetime( self._attributes["updated_at"] )

            self._attributes["updated_at"] = get_now_utc_iso_to_str()
            self._state = "Updated"
            self._attr_native_value = self._state        
            self._attr_extra_state_attributes = self._attributes

            super()._handle_coordinator_update()

