
########################################################
########################################################
#     C O M M O N   F U N C T I O N S 
 ########################################################
########################################################
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
import logging
import geopy.distance
from .const import *
import homeassistant.util.dt as dt_util
#from .coordinator import * 


_LOGGER = logging.getLogger(__name__)




########################################################
def get_now_local_timezone(timezone) -> datetime:
    return  datetime.now(timezone)

########################################################
def get_now_utc() -> datetime:
    return dt_util.utcnow()

########################################################
def get_now_utc_iso_to_str () -> str:
    return get_now_utc().isoformat()

########################################################
def get_utc_iso_str_to_datetime ( input_str) -> datetime:
    l_datetime = datetime.strptime( input_str ,"%Y-%m-%dT%H:%M:%S.%f%z" )
    return l_datetime

########################################################
def get_configured_db_connections(db_engine) -> dict[str]:
    _LOGGER.debug(f"get_configured_db_connection")

    # retreive list of configured DB connections from "master.db"
    db_list = []

    db_conn = None
    try:
        db_conn = db_engine.connect()
        db_sql = f"select * from db_config "     
        db_res = db_conn.execute(text(db_sql))
        for row_cursor in db_res:
            row = row_cursor._asdict()
            db_list.append(row)

    except SQLAlchemyError as e:
        _LOGGER.debug(f"get_configured_db_connections(): {e}") 
        
    db_conn.close()
    return db_list

