"""Data Update coordinator for the GTFS integration."""
from __future__ import annotations

import datetime
from datetime import timedelta
import logging

import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError


from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
import homeassistant.util.dt as dt_util

from .const import *

from .gtfs2_pg_helper import *


_LOGGER = logging.getLogger(__name__)


####################################################################
####################################################################
####################################################################
class gtfs2_pg_Coordinator_Master(DataUpdateCoordinator):
    """Data update coordinator for the GTFS integration."""


    ################################################################
    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass=hass,
            logger=_LOGGER,
            name=entry.entry_id,
            update_interval=timedelta(seconds=30),
        )
        _LOGGER.debug("gtfs2_pg_Coordinator_Master.__init__  BEGIN")          
        self.entry = entry
        self.hass = hass
        self.data: dict[str, str] = {}
        self.data["name"] = "master"
        self.master_engine = sqlalchemy.create_engine(MASTER_CONN_STR, echo=False)

#        self.engine = sqlalchemy.create_engine(MASTER_CONN_STR, echo=True)
        conn = self.master_engine.connect()

        res = conn.execute(text("SELECT name FROM sqlite_master WHERE name='db_config'"))
        if res.fetchone() is None:
            _LOGGER.debug("gtfs2_pg_Coordinator_Master.__init__  create table ")          

            sql = text("CREATE TABLE db_config (db_id INTEGER PRIMARY KEY, db_conn_str TEXT NOT NULL, db_status TEXT NOT NULL,db_message TEXT NOT NULL)")     
            res = conn.execute(sql)
            conn.commit()
            
        _LOGGER.debug("gtfs2_pg_Coordinator_Master;__init__  END")  

    ################################################################
    async def _async_update_data(self) -> dict[str, str]:
        """Get the latest data from GTFS and GTFS relatime, depending refresh interval"""
 
        return self.data


####################################################################
####################################################################
####################################################################
class gtfs2_pg_Coordinator_Sensor(DataUpdateCoordinator):
    """Data update coordinator for the GTFS integration."""

    ################################################################
    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        _LOGGER.debug("gtfs2_pg_Coordinator_Sensor.__init__  BEGIN")
        _LOGGER.debug(f"entry ={entry}")

        super().__init__(
            hass=hass,
            logger=_LOGGER,
            name=entry.data.get('name',0),
            update_interval=timedelta(seconds=60),
        )
    
        self.entry = entry
        self.hass = hass
        self.data: dict[str, str] = {}
    #    self.data["name"] = entry.data.get('name',None)
        self.data = entry.data
        self.master_engine = sqlalchemy.create_engine(MASTER_CONN_STR)
        self.gtfs_engine = None

        db_conn_master = self.master_engine.connect()
 
        # retreive gtfs connection string from db_id
        db_conn_str = ""

        db_id = entry.data.get('db_id',0)
        _LOGGER.debug(f"using db_id ={db_id}")

        try:
            db_sql = f"select * from db_config where db_id = {entry.data.get('db_id',0)}"
            db_res = db_conn_master.execute(text(db_sql))
            for row_cursor in db_res:
                row = row_cursor._asdict()
                db_conn_str = row.get('db_conn_str', '') 
                        
            self.db_type = "unknown"
            if db_conn_str.startswith('sqlite'):
                self.db_type = 'sqlite'
            if db_conn_str.startswith('postgresql'):
                self.db_type = 'postgresql'
        except SQLAlchemyError as e:
            errors["base"] = f"SQL Error: {e}"

        _LOGGER.debug(f"found db_conn_str ={db_conn_str}")
        self.gtfs_engine = sqlalchemy.create_engine(db_conn_str)
        db_conn_master.close()
        _LOGGER.debug("gtfs2_pg_Coordinator_Sensor.__init__  END")

    ################################################################
    async def _async_update_data(self) -> dict[str, str]:
        """Get the latest data from GTFS and GTFS relatime, depending refresh interval"""
#        _LOGGER.debug("gtfs2_pg_Coordinator_Sensor._async_update_data  BEGIN")  



#        _LOGGER.debug("gtfs2_pg_Coordinator_Sensor._async_update_data  END")  
        return self.data

