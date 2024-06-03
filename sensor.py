"""Support for GTFS."""
from datetime import datetime, timezone

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

from .const import (
    ATTR_ARRIVAL,
    ATTR_BICYCLE,
    ATTR_DAY,
    ATTR_DUE_IN,
    ATTR_NEXT_RT,
    ATTR_DROP_OFF_DESTINATION,
    ATTR_DROP_OFF_ORIGIN,
    ATTR_FIRST,
    ATTR_RT_UPDATED_AT,
    ATTR_INFO,
    ATTR_INFO_RT,
    ATTR_LAST,
    ATTR_LOCATION_DESTINATION,
    ATTR_LOCATION_ORIGIN,
    ATTR_OFFSET,
    ATTR_PICKUP_DESTINATION,
    ATTR_PICKUP_ORIGIN,
    ATTR_ROUTE_TYPE,
    ATTR_TIMEPOINT_DESTINATION,
    ATTR_TIMEPOINT_ORIGIN,
    ATTR_WHEELCHAIR,
    ATTR_WHEELCHAIR_DESTINATION,
    ATTR_WHEELCHAIR_ORIGIN,
    BICYCLE_ALLOWED_DEFAULT,
    BICYCLE_ALLOWED_OPTIONS,
    DEFAULT_NAME,
    DOMAIN,
    DROP_OFF_TYPE_DEFAULT,
    DROP_OFF_TYPE_OPTIONS,
    ICON,
    ICONS,
    LOCATION_TYPE_DEFAULT,
    LOCATION_TYPE_OPTIONS,
    PICKUP_TYPE_DEFAULT,
    PICKUP_TYPE_OPTIONS,
    ROUTE_TYPE_OPTIONS,
    TIMEPOINT_DEFAULT,
    TIMEPOINT_OPTIONS,
    WHEELCHAIR_ACCESS_DEFAULT,
    WHEELCHAIR_ACCESS_OPTIONS,
    WHEELCHAIR_BOARDING_DEFAULT,
    WHEELCHAIR_BOARDING_OPTIONS,
)

#from .common import *
from .coordinator import *
from .gtfs2_pg_sensor_master import *
from .gtfs2_pg_sensor_1 import *

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
    ) -> None:

        _LOGGER.debug(f"sensor.py async_setup_entry: config_entry={config_entry}")
        _LOGGER.debug(f"sensor.py async_setup_entry: hass={hass}")

        sensor_type = config_entry.data.get('sensor_type',None)

        match sensor_type:
            case "master":
                sensors = []
                coordinator: gtfs2_pg_Coordinator_Master = hass.data[DOMAIN][config_entry.entry_id][
                "coordinator"
                ]
                await coordinator.async_config_entry_first_refresh()
                
                sensors.append(
                    gtfs2_pg_sensor_master(
                        hass = hass,
                        config_entry = config_entry,
                        coordinator = coordinator)
                )
                async_add_entities(sensors, False)

            case "sensor_1":
                sensors = []
                coordinator: gtfs2_pg_Coordinator_Sensor = hass.data[DOMAIN][config_entry.entry_id][
                "coordinator"
                ]
                await coordinator.async_config_entry_first_refresh()

                sensors.append(
                    gtfs2_pg_sensor_1(
                        hass = hass,
                        config_entry = config_entry,
                        coordinator = coordinator)
                )
                async_add_entities(sensors, False)

            case _:
                _LOGGER.error("async_setup_entry: UNKNOWN SENSOR TYPE!!!!")

            