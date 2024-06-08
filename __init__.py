"""The GTFS integration."""
from __future__ import annotations

import logging
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall

from datetime import timedelta

from .const import DOMAIN, PLATFORMS, DEFAULT_PATH, DEFAULT_PATH_RT, DEFAULT_REFRESH_INTERVAL
from homeassistant.const import CONF_HOST

import voluptuous as vol
#from .gtfs2_pg_helper import *
from .coordinator import * 

_LOGGER = logging.getLogger(__name__)


##################################################################"
##################################################################"
##################################################################"
async def async_migrate_entry(hass, config_entry: ConfigEntry) -> bool:
    """Migrate old entry."""
    _LOGGER.warning("__init__.py  Migrating from version %s", config_entry.version)

    if config_entry.version == 4:

        new_options = {**config_entry.options}
        new_data = {**config_entry.data}
        new_data['route_type'] = '99'
        new_options['offset'] = 0
        new_data.pop('offset')
        new_data['agency'] = '0: ALL'

        config_entry.version = 9
        hass.config_entries.async_update_entry(config_entry, data=new_data)
        hass.config_entries.async_update_entry(config_entry, options=new_options)

    if config_entry.version == 5:

        new_data = {**config_entry.data}
        new_data['route_type'] = '99'
        new_data['agency'] = '0: ALL'

        config_entry.version = 9
        hass.config_entries.async_update_entry(config_entry, data=new_data)

    if config_entry.version == 6:

        new_data = {**config_entry.data}
        new_data['agency'] = '0: ALL'

        config_entry.version = 9
        hass.config_entries.async_update_entry(config_entry, data=new_data)


    if config_entry.version == 7 or config_entry.version == 8:

        new_data = {**config_entry.data}
        new_options = {**config_entry.options}

        config_entry.version = 9

        hass.config_entries.async_update_entry(config_entry, data=new_data)
        hass.config_entries.async_update_entry(config_entry, options=new_options)

    _LOGGER.warning("Migration to version %s successful", config_entry.version)

    return True

##################################################################"
##################################################################"
##################################################################"
async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    _LOGGER.debug(f"__init__.py  async_setup_entry: entry={entry}")


####   où : entry.async_on_unload(entry.add_update_listener(update_listener))


    """Set up GTFS from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    sensor_type = entry.data.get('sensor_type',None)


    match sensor_type:
        case "master":
            _LOGGER.debug("async_setup_entry: Create Master sensor")
            coordinator = gtfs2_pg_Coordinator_Master(hass, entry)
            if not coordinator.last_update_success:
                raise ConfigEntryNotReady

            hass.data[DOMAIN][entry.entry_id] = {
                "coordinator": coordinator
            }

            entry.async_on_unload(entry.add_update_listener(update_listener))
            await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
            return True

        case "sensor_1":
            _LOGGER.debug("async_setup_entry: => Create sensor_1")
            coordinator = gtfs2_pg_Coordinator_Sensor(hass, entry)
            if not coordinator.last_update_success:
                raise ConfigEntryNotReady

            hass.data[DOMAIN][entry.entry_id] = {
                "coordinator": coordinator
            }

            entry.async_on_unload(entry.add_update_listener(update_listener))
            await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
            return True


        case "sensor_2":
            _LOGGER.debug("async_setup_entry: => Create sensor_2")
            coordinator = gtfs2_pg_Coordinator_Sensor(hass, entry)
            if not coordinator.last_update_success:
                raise ConfigEntryNotReady

            hass.data[DOMAIN][entry.entry_id] = {
                "coordinator": coordinator
            }

            entry.async_on_unload(entry.add_update_listener(update_listener))
            await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
            return True

        case _:
            _LOGGER.error("async_setup_entry: UNKNOWN SENSOR TYPE!!!!")

            return False

    # defaut return false. this statement will never be reached 
    return False









##################################################################"
##################################################################"
##################################################################"
async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok


##################################################################"
##################################################################"
##################################################################"
def setup(hass, config):
    """Setup the service component."""

    def update_gtfs2_pg(call):
        """My GTFS service."""
        _LOGGER.debug("Updating GTFS with: %s", call.data)
        get_gtfs2_pg(hass, DEFAULT_PATH, call.data, True)
        return True

    def update_gtfs_rt_local(call):
        """My GTFS RT service."""
        _LOGGER.debug("Updating GTFS RT with: %s", call.data)
        get_gtfs_rt(hass, DEFAULT_PATH_RT, call.data)
        return True

    async def update_local_stops(call):
        """My GTFS RT service."""
        _LOGGER.debug("Updating GTFS Local Stops with: %s", call.data)
        await update_gtfs_local_stops(hass, call.data)
        return True

    hass.services.register(
        DOMAIN, "update_gtfs2_pg",update_gtfs2_pg)
#    hass.services.register(
#        DOMAIN, "update_gtfs_rt_local", update_gtfs_rt_local)
#    hass.services.register(
#        DOMAIN, "update_gtfs_local_stops", update_local_stops)

    return True

##################################################################"
##################################################################"
##################################################################"
async def update_listener(hass: HomeAssistant, entry: ConfigEntry):
 
 #### FDES: why do we maintain the .update_interval  ( and only .update_interval ) here ???
 ####  => commented out to see if needed.
 #   """Handle options update."""
 #   hass.data[DOMAIN][entry.entry_id]['coordinator'].update_interval = timedelta(minutes=1)

    # FDES: we are here because we have finished the "optionflow", and entry most be reloaded
    await hass.config_entries.async_reload(entry.entry_id)

    return True


##################################################################"
##################################################################"
##################################################################"
async def async_setup(hass: HomeAssistant, config: dict):
#    _LOGGER.debug(f"__init__.py async_setup: hass={hass} entry={config}")
    _LOGGER.debug(f"__init__.py async_setup:")
    hass.data.setdefault(DOMAIN,{})
    hass.data[DOMAIN]["config_yaml"] = config.get(DOMAIN) or {}
    return True

