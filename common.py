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
        db_res = db_conn.execute(sqlalchemy.sql.text(db_sql))
        for row_cursor in db_res:
            row = row_cursor._asdict()
            db_list.append(row)

    except SQLAlchemyError as e:
        _LOGGER.debug(f"get_configured_db_connections(): {e}") 
        
    db_conn.close()
    return db_list


########################################################
def get_all_feeds_from_all_db(db_conn) -> dict[str]:
    _LOGGER.debug(f"get_configured_db_connection")

    # retreive list of configured DB connections from "master.db"
    db_list = []
    try:

        sql_query = f"""
            select db_config.db_id,  
                db_config.db_conn_str ,
                feed.feed_id, 
                feed.feed_name,
                feed_append_date 
            from db_config 
            INNER JOIN feed
                    ON  feed.db_id = db_config.db_id                
            """  # noqa: S608

        db_res = db_conn.execute(sqlalchemy.sql.text(sql_query))
        for row_cursor in db_res:
            row = row_cursor._asdict()
            _LOGGER.debug(f"get_all_feeds_from_all_db {row}")

            db_list.append(row)

    except SQLAlchemyError as e:
        _LOGGER.debug(f"get_configured_db_connections(): {e}") 
        
    return db_list



##########################################################
def check_db_conn(conn_str):

    import sqlalchemy_utils
#        assert not sqlalchemy_utils.database_exists(db_url)


    _LOGGER.debug("check_db_conn: %s", conn_str)

    db_exist = sqlalchemy_utils.database_exists (conn_str)
    if not db_exist:
        return "Database Not Found or Connection Error"
    else:
        return ""
    return None         
