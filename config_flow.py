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
from .gtfs_helper import *

#from .common.py import *        

import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError




_LOGGER = logging.getLogger(__name__)

@config_entries.HANDLERS.register(DOMAIN)

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


##########################################################
##########################################################
##########################################################
class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for GTFS."""

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
        db_sql = text("SELECT name FROM sqlite_master WHERE name='db_config'")
        db_res = db_conn.execute(db_sql)

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
            menu_options=["add_db_conn", "del_db_conn","list_db_conn", "sensor_1" ,"start_end", "local_stops", "source","remove","db_upload"],
            description_placeholders={
                "model": "Example model",
            }
        )


    ########################################################
    async def async_step_db_upload(self, user_input: dict | None = None) -> FlowResult:
        import pygtfs

        if os.fork() != 0:
            return    

        sched = pygtfs.Schedule("postgresql://gtfs:gtfs@vm-hadb-tst.home/gtfs")
        return pygtfs.append_feed(sched, "/config/gtfs2_pg_import/TEC-GTFS.zip")
        


    async def async_step_finish(self, user_input: dict | None = None) -> FlowResult:
        return 

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
                    db_res = db_conn.execute(text(db_sql))
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
                    db_res = db_conn.execute(text(db_sql))
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
                        db_res = db_conn.execute(text(db_sql))
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


    #######################################################
    async def async_step_start_end(self, user_input: dict | None = None) -> FlowResult:
        """Handle the source."""
        errors: dict[str, str] = {}      
        if user_input is None:
            datasources = get_datasources(self.hass, DEFAULT_PATH)
            return self.async_show_form(
                step_id="start_end",
                data_schema=vol.Schema(
                    {
                        vol.Required(CONF_FILE, default=""): vol.In(datasources),
                    },
                ),
            )

        user_input[CONF_URL] = "na"
        user_input[CONF_EXTRACT_FROM] = "zip"
        self._user_inputs.update(user_input)
        _LOGGER.debug(f"UserInputs Start End: {self._user_inputs}")
        return await self.async_step_agency()            
            
    ##########################################################
    async def async_step_local_stops(self, user_input: dict | None = None) -> FlowResult:
        """Handle the source."""
        errors: dict[str, str] = {}       
        if user_input is None:
            datasources = get_datasources(self.hass, DEFAULT_PATH)
            return self.async_show_form(
                step_id="local_stops",
                data_schema=vol.Schema(
                    {
                        vol.Required(CONF_FILE, default=""): vol.In(datasources),
                        vol.Required(CONF_DEVICE_TRACKER_ID): selector.EntitySelector(
                            selector.EntitySelectorConfig(domain=["person","zone"]),                          
                        ),
                        vol.Required(CONF_NAME): str, 
                    },
                ),
            ) 
        user_input[CONF_URL] = "na"
        user_input[CONF_EXTRACT_FROM] = "zip"    
        self._user_inputs.update(user_input)
        _LOGGER.debug(f"UserInputs Local Stops: {self._user_inputs}") 
        check_data = await self._check_data(self._user_inputs)
        if check_data :
            errors["base"] = check_data
            return self.async_abort(reason=check_data)
        else:
            return self.async_create_entry(
                title=user_input[CONF_NAME], data=self._user_inputs
                )

    ##########################################################
    async def async_step_source(self, user_input: dict | None = None) -> FlowResult:
        """Handle a flow initialized by the user."""
        errors: dict[str, str] = {}
        if user_input is None:
            return self.async_show_form(
                step_id="source",
                data_schema=vol.Schema(
                    {
                        vol.Required(CONF_EXTRACT_FROM): selector.SelectSelector(selector.SelectSelectorConfig(options=["url", "zip"], translation_key="extract_from")),
                        vol.Required(CONF_FILE): str,
                        vol.Required(CONF_URL, default="na"): str,
                    },
                ),
                errors=errors,
            )    
        check_data = await self._check_data(user_input)
        if check_data :
            errors["base"] = check_data
            return self.async_abort(reason=check_data)
        else:
            self._user_inputs.update(user_input)
            _LOGGER.debug(f"UserInputs Source: {self._user_inputs}")
            return await self.async_step_agency()            

    ##########################################################
    async def async_step_remove(self, user_input: dict | None = None) -> FlowResult:
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

    ##########################################################        
    async def async_step_agency(self, user_input: dict | None = None) -> FlowResult:
        """Handle the agency."""
        errors: dict[str, str] = {}
        self._pygtfs = get_gtfs(
            self.hass,
            DEFAULT_PATH,
            self._user_inputs,
            False,
        )
        check_data = await self._check_data(self._user_inputs)
        if check_data :
            errors["base"] = check_data
            return self.async_abort(reason=check_data)
        agencies = get_agency_list(self._pygtfs, self._user_inputs)
        if len(agencies) > 1:
            agencies[:0] = ["0: ALL"]
            errors: dict[str, str] = {}
            if user_input is None:
                return self.async_show_form(
                    step_id="agency",
                    data_schema=vol.Schema(
                        {
                            vol.Required(CONF_AGENCY): vol.In(agencies),
                        },
                    ),
                    errors=errors,
                ) 
        else:
            user_input = {}
            user_input[CONF_AGENCY] = "0: ALL"
        self._user_inputs.update(user_input)
        _LOGGER.debug(f"UserInputs Agency: {self._user_inputs}")
        return await self.async_step_route_type()          

    ##########################################################        
    async def async_step_route_type(self, user_input: dict | None = None) -> FlowResult:
        """Handle the route_type."""
        errors: dict[str, str] = {}
        if user_input is None:
            return self.async_show_form(
                step_id="route_type",
                data_schema=vol.Schema(
                    {
                        vol.Required(CONF_ROUTE_TYPE): selector.SelectSelector(selector.SelectSelectorConfig(options=["99", "2"], translation_key="route_type")),
                    },
                ),
                errors=errors,
            )                
        self._user_inputs.update(user_input)
        _LOGGER.debug(f"UserInputs Route Type: {self._user_inputs}")
        if user_input[CONF_ROUTE_TYPE] == "2":
            return await self.async_step_stops_train()
        else:
            return await self.async_step_route()          

    ##########################################################
    async def async_step_route(self, user_input: dict | None = None) -> FlowResult:
        """Handle the route."""
        errors: dict[str, str] = {}
        check_data = await self._check_data(self._user_inputs)
        _LOGGER.debug("Source check data: %s", check_data)
        if check_data :
            errors["base"] = check_data
            return self.async_abort(reason=check_data)
        self._pygtfs = get_gtfs(
            self.hass,
            DEFAULT_PATH,
            self._user_inputs,
            False,
        )

        if user_input is None:
            return self.async_show_form(
                step_id="route",
                data_schema=vol.Schema(
                    {
                        vol.Required(CONF_ROUTE, default = ""): selector.SelectSelector(selector.SelectSelectorConfig(options = get_route_list(self._pygtfs, self._user_inputs), translation_key="route_type",custom_value=True)),
                        vol.Required(CONF_DIRECTION): selector.SelectSelector(selector.SelectSelectorConfig(options=["0", "1"], translation_key="direction")),
                    },
                ),
                errors=errors,
            )
        self._user_inputs.update(user_input)
        _LOGGER.debug(f"UserInputs Route: {self._user_inputs}")
        return await self.async_step_stops()

    ##########################################################
    async def async_step_stops(self, user_input: dict | None = None) -> FlowResult:
        """Handle the route step."""
        errors: dict[str, str] = {}
        if user_input is None:
            try:
                stops = get_stop_list(
                    self._pygtfs,
                    self._user_inputs[CONF_ROUTE].split(": ")[0],
                    self._user_inputs[CONF_DIRECTION],
                )
                last_stop = stops[-1:][0]
                return self.async_show_form(
                    step_id="stops",
                    data_schema=vol.Schema(
                        {
                            vol.Required(CONF_ORIGIN): vol.In(stops),
                            vol.Required(CONF_DESTINATION, default=last_stop): vol.In(stops),
                            vol.Required(CONF_NAME): str,
                            vol.Optional(CONF_INCLUDE_TOMORROW, default = False): selector.BooleanSelector(),
                        },
                    ),
                    errors=errors,
                )
            except:
                _LOGGER.debug(f"Likely no stops for this route: {[CONF_ROUTE]}")
                return self.async_abort(reason="no_stops")
        self._user_inputs.update(user_input)
        _LOGGER.debug(f"UserInputs Stops: {self._user_inputs}")
        check_config = await self._check_config(self._user_inputs)
        if check_config:
            errors["base"] = check_config
            return self.async_abort(reason=check_config)
        else:
            return self.async_create_entry(
                title=user_input[CONF_NAME], data=self._user_inputs
            )

    ##########################################################
    async def async_step_stops_train(self, user_input: dict | None = None) -> FlowResult:
        """Handle the stops when train, as often impossible to select ID"""
        errors: dict[str, str] = {}
        if user_input is None:
            return self.async_show_form(
                step_id="stops_train",
                data_schema=vol.Schema(
                    {
                        vol.Required(CONF_ORIGIN): str,
                        vol.Required(CONF_DESTINATION): str,
                        vol.Required(CONF_NAME): str,
                        vol.Optional(CONF_INCLUDE_TOMORROW, default = False): selector.BooleanSelector(),
                    },
                ),
                errors=errors,
            )
        self._user_inputs.update(user_input)
        self._user_inputs[CONF_DIRECTION] = 0
        self._user_inputs[CONF_ROUTE] = "train"
        _LOGGER.debug(f"UserInputs Stops Train: {self._user_inputs}")
        check_config = await self._check_config(self._user_inputs)
        if check_config:
            _LOGGER.debug(f"CheckConfig: {check_config}")
            errors["base"] = check_config
            return self.async_abort(reason=check_config)
        else:
            return self.async_create_entry(
                title=user_input[CONF_NAME], data=self._user_inputs
            )            
    ##########################################################
    async def _check_data(self, data):
        self._pygtfs = await self.hass.async_add_executor_job(
            get_gtfs, self.hass, DEFAULT_PATH, data, False
        )
        _LOGGER.debug("Checkdata pygtfs: %s with data: %s", self._pygtfs, data)
        if self._pygtfs in ['no_data_file', 'no_zip_file', 'extracting'] :
            return self._pygtfs
        check_index = await self.hass.async_add_executor_job(
                    check_datasource_index, self.hass, self._pygtfs, DEFAULT_PATH, data["file"]
                )            
        return None
    ##########################################################
    async def _check_config(self, data):
        self._pygtfs = await self.hass.async_add_executor_job(
            get_gtfs, self.hass, DEFAULT_PATH, data, False
        )
        if self._pygtfs == "no_data_file":
            return "no_data_file"
        self._data = {
            "schedule": self._pygtfs,
            "origin": data["origin"],
            "destination": data["destination"],
            "offset": 0,
            "include_tomorrow": True,
            "gtfs_dir": DEFAULT_PATH,
            "name": data["name"],
            "next_departure": None,
            "file": data["file"],
            "route_type": data["route_type"]
        }
        # check and/or add indexes
        check_index = await self.hass.async_add_executor_job(
                    check_datasource_index, self.hass, self._pygtfs, DEFAULT_PATH, data["file"]
                )
        try:
            self._data["next_departure"] = await self.hass.async_add_executor_job(
                get_next_departure, self
            )
        except Exception as ex:  # pylint: disable=broad-except
            _LOGGER.error(
                "Config: error getting gtfs data from generic helper: %s",
                {ex},
                exc_info=1,
            )
            return "generic_failure"
        if self._data["next_departure"]:
            return None
        return "stop_incorrect"

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Create the options flow."""
        return GTFS_pg_OptionsFlowHandler(config_entry)

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
#                    _LOGGER.debug(f"Call to async_create_entry :data ={param_user_input}")
#                    is_ok = self.async_create_entry(title="", data=param_user_input)
#                    _LOGGER.debug(f"is_ok ={is_ok}")

                    self.hass.config_entries.async_update_entry(
                        self.config_entry, data=param_user_input, options={}
                    )
                    return self.async_abort(reason="Data saved")


                case _:
                    _LOGGER.debug(f"Sensor unknown")





#                param_user_input["sensor_type"] = "sensor_1"
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

                keys=[]
                descriptions=[]
                # split datasources in 2 tables : keys and description
                for line in datasources:
                    keys.append ( line['db_id'] )  
                    descriptions.append ( f"[{line['db_id']}] - {line['db_conn_str']} ({line['db_status']})" )

                tmp_radius = self.config_entry.data.get("radius",99)

                _LOGGER.debug(f"radius =  {tmp_radius}")


                opt1_schema = (
                    {
                        vol.Required(CONF_NAME, default=self.config_entry.data.get("name","")): str, 
                        vol.Required("db_selected", default=""): vol.In(descriptions),
                        vol.Required(CONF_DEVICE_TRACKER_ID, default=self.config_entry.data.get("device_tracker_id","")): selector.EntitySelector(
                            selector.EntitySelectorConfig(domain=["person","zone"]),                          
                        ),
                        vol.Required(CONF_RADIUS, default=self.config_entry.data.get("radius",99)): vol.All(vol.Coerce(int), vol.Range(min=50, max=5000)),

                        vol.Required("refresh_min_distance", default=self.config_entry.data.get("refresh_min_distance",20)): vol.All(vol.Coerce(int), vol.Range(min=10, max=1000)),
                        vol.Required("refresh_min_seconds", default=self.config_entry.data.get("refresh_min_seconds",30)): vol.All(vol.Coerce(int), vol.Range(min=20, max=300)),
                        vol.Required("refresh_max_seconds", default=self.config_entry.data.get("refresh_max_seconds",300)): vol.All(vol.Coerce(int), vol.Range(min=120, max=3600)),
                        vol.Required(CONF_OFFSET, default=self.config_entry.data.get(CONF_OFFSET,0)): int,
                        vol.Required(CONF_TIMERANGE, default=self.config_entry.data.get(CONF_TIMERANGE, DEFAULT_LOCAL_STOP_TIMERANGE)): vol.All(vol.Coerce(int), vol.Range(min=5, max=240)),

                    }
                )
                return self.async_show_form(
                    step_id="init",
                    data_schema=vol.Schema(opt1_schema),
                    errors = errors
                )                
            case _:
                _LOGGER.debug(f"Sensor unknown")



        
    async def async_step_real_time(
           self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle a realtime initialized by the user."""
        errors: dict[str, str] = {}
        if user_input is not None:
            self._user_inputs.update(user_input)
            _LOGGER.debug(f"UserInput Realtime: {self._user_inputs}")
            return self.async_create_entry(title="", data=self._user_inputs)

        if self.config_entry.data.get(CONF_DEVICE_TRACKER_ID, None):
            return self.async_show_form(
                step_id="real_time",
                data_schema=vol.Schema(
                    {
                        vol.Required(CONF_TRIP_UPDATE_URL, default=self.config_entry.options.get(CONF_TRIP_UPDATE_URL)): str,
                        vol.Optional(CONF_API_KEY, default=self.config_entry.options.get(CONF_API_KEY,"")): str,
                        vol.Optional(CONF_X_API_KEY,default=self.config_entry.options.get(CONF_X_API_KEY,"")): str,
                        vol.Optional(CONF_OCP_APIM_KEY,default=self.config_entry.options.get(CONF_OCP_APIM_KEY,"")): str,
                        vol.Required(CONF_API_KEY_LOCATION, default=self.config_entry.options.get(CONF_API_KEY_LOCATION,DEFAULT_API_KEY_LOCATION)) : selector.SelectSelector(selector.SelectSelectorConfig(options=ATTR_API_KEY_LOCATIONS, translation_key="api_key_location")),
                        vol.Optional(CONF_ACCEPT_HEADER_PB, default = False): selector.BooleanSelector(),
                    },
                ),
                errors=errors,
            )  
        else:
            return self.async_show_form(
                step_id="real_time",
                data_schema=vol.Schema(
                    {
                        vol.Required(CONF_TRIP_UPDATE_URL, default=self.config_entry.options.get(CONF_TRIP_UPDATE_URL)): str,
                        vol.Optional(CONF_VEHICLE_POSITION_URL, default=self.config_entry.options.get(CONF_VEHICLE_POSITION_URL,"")): str,
                        vol.Optional(CONF_ALERTS_URL, default=self.config_entry.options.get(CONF_ALERTS_URL,"")): str,
                        vol.Optional(CONF_API_KEY, default=self.config_entry.options.get(CONF_API_KEY,"")): str,
                        vol.Optional(CONF_X_API_KEY,default=self.config_entry.options.get(CONF_X_API_KEY,"")): str,
                        vol.Optional(CONF_OCP_APIM_KEY,default=self.config_entry.options.get(CONF_OCP_APIM_KEY,"")): str,
                        vol.Required(CONF_API_KEY_LOCATION, default=self.config_entry.options.get(CONF_API_KEY_LOCATION,DEFAULT_API_KEY_LOCATION)) : selector.SelectSelector(selector.SelectSelectorConfig(options=ATTR_API_KEY_LOCATIONS, translation_key="api_key_location")),
                        vol.Optional(CONF_ACCEPT_HEADER_PB, default = False): selector.BooleanSelector(),
                    },
                ),
                errors=errors,
            )      
##########################################################    
async def _check_stop_list(self, data):
    _LOGGER.debug("Checkstops option with data: %s", data)
    self._pygtfs = await self.hass.async_add_executor_job(
        get_gtfs, self.hass, DEFAULT_PATH, data, False
    )
    count_stops = await self.hass.async_add_executor_job(
                get_local_stop_list, self.hass, self._pygtfs, data
            )  
    if count_stops > DEFAULT_MAX_LOCAL_STOPS:
        _LOGGER.debug("Checkstops limit reached with: %s", count_stops)
        return "stop_limit_reached"
    return None         


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







