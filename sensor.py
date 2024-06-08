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

from .const import *


from .coordinator import *
from .gtfs2_pg_sensor_master import *
from .gtfs2_pg_sensor_1 import *
from .gtfs2_pg_sensor_2 import *


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


            case "sensor_2":
                sensors = []
                coordinator: gtfs2_pg_Coordinator_Sensor = hass.data[DOMAIN][config_entry.entry_id][
                "coordinator"
                ]
                await coordinator.async_config_entry_first_refresh()

                sensors.append(
                    gtfs2_pg_sensor_2(
                        hass = hass,
                        config_entry = config_entry,
                        coordinator = coordinator)
                )
                async_add_entities(sensors, False)

            case _:
                _LOGGER.error("async_setup_entry: UNKNOWN SENSOR TYPE!!!!")

            