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


####################################################################
def create_db_master(db_conn):
    # i don't think we need to do anysthing, SQLITE will create the 
    # DB by itselt...
    _LOGGER.debug(f"create_db_master")

    return
####################################################################
def create_table_db_config(db_conn):
    _LOGGER.debug(f"create_table_db_config")

# examples of connection string:
#    postgresql://gtfs:gtfs@vm-hadb-tst.home/gtfs
#    sqlite:///gtfs2/TEC-GTFS.sqlite

    sql_query = f"""
        CREATE TABLE IF NOT EXISTS db_config (
            db_id        INTEGER   PRIMARY KEY,
            db_conn_str  TEXT      NOT NULL,
            db_status    TEXT      NOT NULL,
            db_message   TEXT      NOT NULL
        ) WITHOUT ROWID;
            
        """  # noqa: S608

    result = db_conn.execute(sqlalchemy.sql.text(sql_query))
    db_conn.commit()
    return

####################################################################
def create_table_feed(db_conn):
    _LOGGER.debug(f"create_table_feed")

    # this table will contains all tables "feed" from all db
    sql_query = f"""
        CREATE TABLE IF NOT EXISTS feed (
            db_id        INTEGER,
            feed_id      INTEGER,
            feed_name    TEXT,
            feed_append_date  date,
            
            PRIMARY KEY (db_id, feed_id)
        ) WITHOUT ROWID;
            
        """  # noqa: S608

    result = None
    try:
        result = db_conn.execute(sqlalchemy.sql.text(sql_query))
        db_conn.commit()
    except SQLAlchemyError as e:
        _LOGGER.debug(f"create_table_feed SQL ERROR: {e}")

    return

####################################################################
def update_table_feed(db_conn, feeds):

 #   _LOGGER.debug(f" update_table_feed feeds = {feeds}")


    sql_query = f"""
        delete from feed;
        """  # noqa: S608

    result = db_conn.execute(sqlalchemy.sql.text(sql_query))
    
    sql_query = f"""
        insert or replace into feed (db_id, feed_id, feed_name, feed_append_date) values
        (:db_id, :feed_id, :feed_name, :feed_append_date);

        """  # noqa: S608
    try:
        for row_feed in feeds:
            _LOGGER.debug(f" => found feed: {row_feed} )")

            result = db_conn.execute(
                sqlalchemy.sql.text(sql_query),
                {
                    "db_id"       : row_feed["db_id"],
                    "feed_id"     : row_feed["feed_id"],
                    "feed_name"   : row_feed["feed_name"],
                    "feed_append_date" : row_feed["feed_append_date"],
                },
            )

        db_conn.commit()

    except SQLAlchemyError as e:
        _LOGGER.debug(f"create_table_feed SQL ERROR: {e}")

    return    



####################################################################
def get_all_feed_data (db_conn):
    # this table will contains all tables "feed" from all db
    sql_query = f"""
        select * from _feed 
            
        """  # noqa: S608

    result = db_conn.execute(sqlalchemy.sql.text(sql_query))
    
    result_data = []
    for row_cursor in result:
        row = row_cursor._asdict()
        result_data.append (row)
    return result_data

####################################################################
def get_all_feeds_from_all_db (db_conn): 
    sql_query = f"""
        select * from db_config   

            
        """  # noqa: S608

    result = db_conn.execute(sqlalchemy.sql.text(sql_query))

    result_data = []
    for row_cursor in result:
        row = row_cursor._asdict()
        result_data.append (row)


    all_feeds = []

    for row_data in result_data:
#        _LOGGER.debug(f"get_all_feeds_from_all_db db_id:{row_data["db_id"]} ({row_data["db_conn_str"]})")

        gtfs_engine = sqlalchemy.create_engine(row_data["db_conn_str"], echo=False)
        db_conn_gtfs = gtfs_engine.connect()
        feeds= get_all_feed_data (db_conn_gtfs)
        db_conn_gtfs.close()
        for row_feed in feeds:
            _LOGGER.debug(f" => found feed: {row_feed["feed_id"]} = {row_feed["feed_name"]} ({row_feed["feed_append_date"]} )")
            row_all_feeds ={}
            row_all_feeds["db_id"]            = row_data["db_id"]
            row_all_feeds["db_conn_str"]      = row_data["db_conn_str"]
            row_all_feeds["feed_id"]          = row_feed["feed_id"]
            row_all_feeds["feed_name"]        = row_feed["feed_name"]
            row_all_feeds["feed_append_date"] = row_feed["feed_append_date"]

#            _LOGGER.debug(f"all_feeds.append ( row_all_feeds)=  {row_all_feeds}")

            all_feeds.append ( row_all_feeds) 


    return all_feeds


########################################################
class gtfs2_pg_sensor_master(CoordinatorEntity, SensorEntity):
########################################################
    
### __INIT__
    def __init__( self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        coordinator ) -> None:

        _LOGGER.debug(f"gtfs2_pg_sensor_master.__init__: BEGIN")
#        _LOGGER.debug(f"config_entry={config_entry}")
#        _LOGGER.debug(f"hass={hass}")
#        _LOGGER.debug(f"coordinator={coordinator}")

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
            create_db_master(db_conn)
            create_table_db_config(db_conn)
            create_table_feed(db_conn)
            db_conn.close()
        except SQLAlchemyError as e:
            self._state = "SQL ERROR in gtfs2_pg_sensor_master.__init__"
            _LOGGER.debug(f"gtfs2_pg_sensor_master.__init__ error={e}")

        

        self._attr_native_value = self._state
        self._attributes["updated_at"] = get_now_utc_iso_to_str()
        self.hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STARTED, self.home_assistant_started
        )


    async def home_assistant_started(self, event):
#        _LOGGER.debug(f"gtfs2_pg_sensor_master.home_assistant_started: BEGIN")
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

        db_conn = self.coordinator.master_engine.connect()

        all_feeds = []

        try:
            all_feeds = get_all_feeds_from_all_db (db_conn)

#            create_db_master(db_conn)
#            create_table_db_config(db_conn)
#            create_table_feed(db_conn)
            update_table_feed(db_conn,all_feeds  )  
            db_conn.close()
        except SQLAlchemyError as e:
            self._state = "SQL ERROR in gtfs2_pg_sensor_master.__init__"
            _LOGGER.debug(f"gtfs2_pg_sensor_master._update_attrs error={e}")

        self._attributes["updated_at"] = get_now_utc_iso_to_str()
        self._attributes["all_feeds"] = all_feeds

        self._state = "Updated"
        self._attr_native_value = self._state        
        self._attr_extra_state_attributes = self._attributes

        super()._handle_coordinator_update()

