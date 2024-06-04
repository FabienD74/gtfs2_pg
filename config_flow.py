"""ConfigFlow for GTFS integration."""
from __future__ import annotations

import logging
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult
import homeassistant.helpers.config_validation as cv
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import selector

from .const import *

import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError

from .common import *

#from .config_flow_helper import *        




_LOGGER = logging.getLogger(__name__)

@config_entries.HANDLERS.register(DOMAIN)


##########################################################
##########################################################
##########################################################
class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for GTFS."""


    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Create the options flow."""
        return GTFS_pg_OptionsFlowHandler(config_entry)



    VERSION = 1

    def __init__(self) -> None:
        """Init ConfigFlow."""
#        self._pygtfs = ""
        self._data: dict[str, str] = {}
        self.engine = sqlalchemy.create_engine(MASTER_CONN_STR, echo=True)

        self._user_inputs: dict = {}

    ##########################################################
    async def async_step_user(self, user_input: dict | None = None) -> FlowResult:
        """Handle the source."""
        errors: dict[str, str] = {}

        db_conn = self.engine.connect()
        db_sql = "SELECT name FROM sqlite_master WHERE name='db_config'"
        db_res = db_conn.execute(sqlalchemy.sql.text(db_sql))

        db_conn.commit()
        db_conn.rollback()
        db_conn.close()

        if db_res.fetchone() is None:
            # create main sensor
            user_input = {}
            user_input["sensor_type"] = "master"
            self._user_inputs.update(user_input)
            _LOGGER.debug(f"master_db empty => create main sensor") 
            return self.async_create_entry( title="Monitor Sensor", data=self._user_inputs )                

        return self.async_show_menu(
            step_id="user",
            menu_options=["add_db_conn", "del_db_conn","list_db_conn", "sensor_1" ,"db_upload"],
            description_placeholders={
                "model": "Example model",
            }
        )

    ########################################################
    async def async_step_add_db_conn(self, user_input: dict | None = None) -> FlowResult:
        errors: Dict[str, str] = {}
        if user_input is not None:
            
            # Validate input.            
            msg= check_db_conn(user_input[CONF_DB_CONN_STR])
            if msg != "":
                errors["base"] = "Database Not Found"
#                return self.async_abort(reason=check_data)
            else:
                db_res = None
                try:
                    db_conn = self.engine.connect()
                    db_sql = f"select max(db_id ) as db_id from db_config "     
                    db_res = db_conn.execute(sqlalchemy.sql.text(db_sql))
                    if ( not db_res) or (db_res == None ):
                        _LOGGER.debug(f"CASE 1")


                        new_db_id = 1
                    else:
                        for row_cursor in db_res:
                            row = row_cursor._asdict()
#                            new_db_id = row["db_id"] + 1
#                            new_db_id = row.get("db_id",0) + 1
                            new_db_id = row["db_id"]
                            if new_db_id == None:
                                _LOGGER.debug(f"CASE 2")
                                new_db_id = 1
                            else:
                                new_db_id = new_db_id + 1
                                
                    db_sql = f"INSERT INTO db_config  (db_id,db_conn_str,db_status,db_message) VALUES( {new_db_id}, '{user_input[CONF_DB_CONN_STR]}', 'init', 'init')"
                    db_res = db_conn.execute(sqlalchemy.sql.text(db_sql))
                except SQLAlchemyError as e:
                    errors["base"] = f"SQL Error: {e}"
                    
                if ( not db_res):
                    db_conn.rollback()
#                    errors["base"] = "db_conn.error"
                else:
                    db_conn.commit()

                db_conn.close()
                return self.async_abort(reason="Entry saved")

        ret_form = self.async_show_form(
            step_id="add_db_conn",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_DB_CONN_STR, default="postgresql://gtfs:gtfs@vm-hadb-tst.home/gtfs"): str,
                },
            ),
            errors=errors,
        ) 
        return ret_form 




    ########################################################
    async def async_step_del_db_conn(self, user_input: dict | None = None) -> FlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            # Validation and additional processing logic omitted for brevity.
            # ...
            
            if not errors:
                _LOGGER.debug(f"async_step_del_db_conn db_selected : {user_input['db_selected']}")
                db_conn = self.engine.connect()

                for line in user_input['db_selected']:
                    db_id = line.split('[', 1)[1].split(']')[0]
                    _LOGGER.debug(f"selected : {db_id}")

                    db_res = None
                    try:
                        db_sql = f"delete from db_config where  db_id = {db_id} "     
                        db_res = db_conn.execute(sqlalchemy.sql.text(db_sql))
                    except SQLAlchemyError as e:
                        errors["base"] = f"SQL Error: {e}"
                        
                    if ( not db_res):
                        db_conn.rollback()
                    else:
                        db_conn.commit()
                db_conn.close()
                return self.async_abort(reason="DONE")


        datasources = get_configured_db_connections(self.engine) 

        keys=[]
        descriptions=[]
        # split datasources in 2 tables : keys and description
        for line in datasources:
            keys.append ( line['db_id'] )  
            descriptions.append ( f"[{line['db_id']}] - {line['db_conn_str']} ({line['db_status']})" )

        return self.async_show_form(
            step_id="del_db_conn",
            data_schema=vol.Schema(
                {
                    vol.Optional("db_selected"): cv.multi_select(descriptions),
                },
            ),
            errors=errors,
        )

        return self.async_abort(reason="end of async_step_del_db_conn")




    ########################################################
    async def async_step_sensor_1(self, user_input: dict | None = None) -> FlowResult:
        errors: dict[str, str] = {}
        l_step_id = "sensor_1"

        if user_input is not None:
            # Validation and additional processing logic omitted for brevity.
            # ...
            
            if not errors:
                _LOGGER.debug(f"async_step_sensor_1 db_selected : {user_input['db_selected']}")

                line = user_input['db_selected']
                db_id = line.split('[', 1)[1].split(']')[0]
                _LOGGER.debug(f"selected : {db_id}")

                # create  sensor
                param_user_input = {}
                param_user_input["sensor_type"] = "sensor_1"
                param_user_input["db_id"] = db_id
                param_user_input[CONF_NAME] = user_input[CONF_NAME]
                param_user_input[CONF_DEVICE_TRACKER_ID] = user_input[CONF_DEVICE_TRACKER_ID]
                param_user_input[CONF_RADIUS] = user_input[CONF_RADIUS]
                param_user_input["refresh_min_distance"] = user_input["refresh_min_distance"]
                param_user_input["refresh_min_seconds"] = user_input["refresh_min_seconds"]
                param_user_input["refresh_max_seconds"] = user_input["refresh_max_seconds"]
                param_user_input["offset"] = user_input["offset"]


                _LOGGER.debug(f"async_step_sensor_1 : create sensor with  data={param_user_input}") 
                return self.async_create_entry( title=param_user_input[CONF_NAME], data=param_user_input )                



        datasources = get_configured_db_connections(self.engine) 

        keys=[]
        descriptions=[]
        # split datasources in 2 tables : keys and description
        for line in datasources:
            keys.append ( line['db_id'] )  
            descriptions.append ( f"[{line['db_id']}] - {line['db_conn_str']} ({line['db_status']})" )

        return self.async_show_form(
            step_id=l_step_id,
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_NAME): str, 
                    vol.Required("db_selected", default=""): vol.In(descriptions),
                    vol.Required(CONF_DEVICE_TRACKER_ID): selector.EntitySelector(
                        selector.EntitySelectorConfig(domain=["person","zone"]),                          
                    ),
                    vol.Optional(CONF_RADIUS, default=250): vol.All(vol.Coerce(int), vol.Range(min=50, max=5000)),

                    vol.Optional("refresh_min_distance", default=30): vol.All(vol.Coerce(int), vol.Range(min=10, max=1000)),
                    vol.Optional("refresh_min_seconds", default=30): vol.All(vol.Coerce(int), vol.Range(min=20, max=300)),
                    vol.Optional("refresh_max_seconds", default=300): vol.All(vol.Coerce(int), vol.Range(min=120, max=3600)),
                    vol.Optional(CONF_OFFSET, default=0): int,


                },
            ),
            errors=errors,
        )
  

    ########################################################
    async def async_step_sensor_2(self, user_input: dict | None = None) -> FlowResult:
        """Handle a flow initialized by the user."""
        errors: dict[str, str] = {}
        if user_input is None:
            datasources = get_datasources(self.hass, DEFAULT_PATH)
            return self.async_show_form(
                step_id="remove",
                data_schema=vol.Schema(
                    {
                        vol.Required(CONF_FILE, default=""): vol.In(datasources),
                    },
                ),
                errors=errors,
            )
        try:
            removed = remove_datasource(self.hass, DEFAULT_PATH, user_input[CONF_FILE])
            _LOGGER.debug(f"Removed gtfs data source: {removed}")
        except Exception as ex:
            _LOGGER.error("Error while deleting : %s", {ex})
            return "generic_failure"
        return self.async_abort(reason="files_deleted")


    ########################################################
    async def async_step_sensor_3(self, user_input: dict | None = None) -> FlowResult:


        """Handle a flow initialized by the user."""
        errors: dict[str, str] = {}
        if user_input is None:
            datasources = get_datasources(self.hass, DEFAULT_PATH)
            return self.async_show_form(
                step_id="remove",
                data_schema=vol.Schema(
                    {
                        vol.Required(CONF_FILE, default=""): vol.In(datasources),
                    },
                ),
                errors=errors,
            )
        try:
            removed = remove_datasource(self.hass, DEFAULT_PATH, user_input[CONF_FILE])
            _LOGGER.debug(f"Removed gtfs data source: {removed}")
        except Exception as ex:
            _LOGGER.error("Error while deleting : %s", {ex})
            return "generic_failure"
        return self.async_abort(reason="files_deleted")

    ########################################################
    async def async_step_db_upload(self, user_input: dict | None = None) -> FlowResult:
        import pygtfs

        if os.fork() != 0:
            return    

        sched = pygtfs.Schedule("postgresql://gtfs:gtfs@vm-hadb-tst.home/gtfs")
        return pygtfs.append_feed(sched, "/config/gtfs2_pg_import/TEC-GTFS.zip", chunk_size=500)
        


    async def async_step_finish(self, user_input: dict | None = None) -> FlowResult:
        return 


##########################################################
##########################################################
##########################################################
class GTFS_pg_OptionsFlowHandler(config_entries.OptionsFlow):

    ##########################################################
    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        """Initialize options flow."""
        self.config_entry = config_entry
        self._data: dict[str, str] = {}
        self._user_inputs: dict = {}
        self.engine = sqlalchemy.create_engine(MASTER_CONN_STR, echo=True)

        super().__init__()



    ##########################################################
    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Manage the options."""
        errors: dict[str, str] = {}

        ############################
        ############################
        if user_input is not None:

            match self.config_entry.data.get("sensor_type", None):
                case None:
                    _LOGGER.debug(f"Sensor None ?????")
                case "master":
                    _LOGGER.debug(f"Sensor master")

                case "sensor_1":
                    _LOGGER.debug(f"Save values for Sensor1")

                    line = user_input['db_selected']
                    db_id = line.split('[', 1)[1].split(']')[0]
                    _LOGGER.debug(f"selected : {db_id}")


                    # update  sensor
                    param_user_input = {}
                    param_user_input["sensor_type"] = "sensor_1"
                    param_user_input["db_id"] = db_id
                    param_user_input[CONF_NAME] = user_input[CONF_NAME]
                    param_user_input[CONF_DEVICE_TRACKER_ID] = user_input[CONF_DEVICE_TRACKER_ID]
                    param_user_input[CONF_RADIUS] = user_input[CONF_RADIUS]
                    param_user_input["refresh_min_distance"] = user_input["refresh_min_distance"]
                    param_user_input["refresh_min_seconds"] = user_input["refresh_min_seconds"]
                    param_user_input["refresh_max_seconds"] = user_input["refresh_max_seconds"]
                    param_user_input["offset"] = user_input["offset"]
                    param_user_input[CONF_TIMERANGE] = user_input[CONF_TIMERANGE]
                    self.hass.config_entries.async_update_entry(
                        self.config_entry, data=param_user_input, options={}
                    )
                    return self.async_abort(reason="Data saved")


                case _:
                    return self.async_abort(reason=f"Sensor type {self.config_entry.data.get("sensor_type", None)} UNKNOWN")


        # end if user_input is not None:
        ############################

        _LOGGER.debug(f"Sensor type:  {self.config_entry.data.get("sensor_type", None)}")

        match self.config_entry.data.get("sensor_type", None):
            case None:
                _LOGGER.debug(f"Sensor None ?????")
            case "master":
                _LOGGER.debug(f"Sensor master")

            case "sensor_1":
                _LOGGER.debug(f"Sensor1")
                _LOGGER.debug(f"data = {self.config_entry.data}")

                datasources = get_configured_db_connections(self.engine) 

                current_db_id = self.config_entry.data.get("db_id","-1")
                descriptions=[]
                default_db_selected = ""
                # Create all entries for Radiobuttons, and catch the current db_id ( used as default)
                for line in datasources:
                    option_line = f"[{line['db_id']}] - {line['db_conn_str']} ({line['db_status']})" 
                    descriptions.append (option_line )
                    if ( int(line["db_id"]) ==  int (current_db_id) ): 
                        default_db_selected = option_line

                _LOGGER.debug(f"current_db_id = {current_db_id}  default radio set to={default_db_selected}")


                opt1_schema = (
                    {
                        vol.Required(CONF_NAME, default=self.config_entry.data.get("name","")): str, 
                        vol.Required("db_selected", default=default_db_selected): vol.In(descriptions),
                        vol.Required(CONF_DEVICE_TRACKER_ID, default=self.config_entry.data.get("device_tracker_id","")): selector.EntitySelector(
                            selector.EntitySelectorConfig(domain=["person","zone"]),                          
                        ),
                        vol.Required(CONF_RADIUS, default=self.config_entry.data.get("radius",99)): vol.All(vol.Coerce(int), vol.Range(min=50, max=5000)),

                        vol.Required("refresh_min_distance", default=self.config_entry.data.get("refresh_min_distance",20)):            vol.All(vol.Coerce(int), vol.Range(min=10, max=1000)),
                        vol.Required("refresh_min_seconds", default=self.config_entry.data.get("refresh_min_seconds",30)):              vol.All(vol.Coerce(int), vol.Range(min=20, max=300)),
                        vol.Required("refresh_max_seconds", default=self.config_entry.data.get("refresh_max_seconds",300)):             vol.All(vol.Coerce(int), vol.Range(min=120, max=1200)),
                        vol.Required(CONF_OFFSET, default=self.config_entry.data.get(CONF_OFFSET,0)): int,
                        vol.Required(CONF_TIMERANGE, default=self.config_entry.data.get(CONF_TIMERANGE, DEFAULT_LOCAL_STOP_TIMERANGE)): vol.All(vol.Coerce(int), vol.Range(min=5, max=600)),
                    }
                )
                return self.async_show_form(
                    step_id="init",
                    data_schema=vol.Schema(opt1_schema),
                    errors = errors
                )                
            case _:
                _LOGGER.debug(f"Sensor unknown")










