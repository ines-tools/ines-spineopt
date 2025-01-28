import spinedb_api as api
from spinedb_api import DatabaseMapping, DateTime, Map, to_database
from spinedb_api.parameter_value import convert_map_to_table, IndexedValue
from sqlalchemy.exc import DBAPIError
import yaml
import sys
from ines_tools import ines_transform
import pandas as pd
import json

def nested_index_names(value, names = None, depth = 0):
    if names is None:
        names = []
    if depth == len(names):
        names.append(value.index_name)
    elif value.index_name != names[-1]:
        raise RuntimeError(f"Index names at depth {depth} do no match: {value.index_name} vs. {names[-1]}")
    for y in value.values:
        if isinstance(y, IndexedValue):
            nested_index_names(y, names, depth + 1)
    return names

operations = {
    "multiply": lambda x, y: x * y,
    "add": lambda x, y: x + y,
    "subtract": lambda x, y: x - y,
    "divide": lambda x, y: x / y,
    "constant": lambda x, y: y
}

if len(sys.argv) > 1:
    url_db_in = sys.argv[1]
else:
    exit("Please provide input database url and output database url as arguments. They should be of the form ""sqlite:///path/db_file.sqlite""")
if len(sys.argv) > 2:
    url_db_out = sys.argv[2]
else:
    exit("Please provide input database url and output database url as arguments. They should be of the form ""sqlite:///path/db_file.sqlite""")

with open('ines_to_spineopt_entities.yaml', 'r') as file:
    entities_to_copy = yaml.load(file, yaml.BaseLoader)
with open('ines_to_spineopt_parameters.yaml', 'r') as file:
    parameter_transforms = yaml.load(file, yaml.BaseLoader)
with open('ines_to_spineopt_methods.yaml', 'r') as file:
    parameter_methods = yaml.load(file, yaml.BaseLoader)
with open('ines_to_spineopt_entities_to_parameters.yaml', 'r') as file:
    entities_to_parameters = yaml.load(file, yaml.BaseLoader)
with open('settings.yaml', 'r') as file:
    settings = yaml.load(file, yaml.BaseLoader)

def add_entity(db_map : DatabaseMapping, class_name : str, name : tuple, ent_description = None) -> None:
    _, error = db_map.add_entity_item(entity_byname=name, entity_class_name=class_name, description = ent_description)
    if error is not None:
        raise RuntimeError(error)

def add_parameter_value(db_map : DatabaseMapping,class_name : str,parameter : str,alternative : str,elements : tuple,value : any) -> None:
    db_value, value_type = api.to_database(value)
    _, error = db_map.add_parameter_value_item(entity_class_name=class_name,entity_byname=elements,parameter_definition_name=parameter,alternative_name=alternative,value=db_value,type=value_type)
    if error:
        raise RuntimeError(error)

def add_alternative(db_map : DatabaseMapping,name_alternative : str) -> None:
    _, error = db_map.add_alternative_item(name=name_alternative)
    if error is not None:
        raise RuntimeError(error)

def main():
    with DatabaseMapping(url_db_in) as source_db:
        with DatabaseMapping(url_db_out) as target_db:
            ## Empty the database
            target_db.purge_items('parameter_value')
            target_db.purge_items('entity')
            target_db.purge_items('alternative')
            target_db.refresh_session()
            target_db.commit_session("Purged stuff")

            ## Copy alternatives
            for alternative in source_db.get_alternative_items():
                target_db.add_alternative_item(name=alternative["name"])
            for scenario in source_db.get_scenario_items():
                target_db.add_scenario_item(name=scenario["name"])
            for scenario_alternative in source_db.get_scenario_alternative_items():
                target_db.add_scenario_alternative_item(alternative_name=scenario_alternative["alternative_name"],
                                                        scenario_name=scenario_alternative["scenario_name"],
                                                        rank=scenario_alternative["rank"])
            
            
            ## Copy entites
            target_db = ines_transform.copy_entities(source_db, target_db, entities_to_copy)
            ## Copy numeric parameters(source_db, target_db, copy_entities)
            target_db = ines_transform.transform_parameters(source_db, target_db, parameter_transforms)
            ## Copy methods(source_db, target_db, copy_entities)
            target_db = ines_transform.process_methods(source_db, target_db, parameter_methods)
            ## Copy entities to parameters
            # target_db = ines_transform.copy_entities_to_parameters(source_db, target_db, entities_to_parameters)

            # Process emisssions balance equations
            process_emissions(source_db,target_db)

            # Manual functions
            map_of_ts_conversion_ts_alternatives(source_db,target_db,settings["map_of_historical_ts_to_scenario_ts"])
            map_of_periods_to_ts(source_db,target_db,settings["map_of_periods_to_ts"])
            timeline_setup(source_db,target_db)

def process_emissions(source_db, target_db):

    # unit flow coming from fossil nodes
    co2_params = source_db.get_parameter_value_items(entity_class_name="node",parameter_definition_name="co2_content",alternative_name="Base")
    co2_value  = {co2_param["entity_name"]:co2_param["parsed_value"] for co2_param in co2_params}

    for entity_items in [element for element in target_db.get_entity_items(entity_class_name="unit__from_node") if element["entity_byname"][1] in co2_value]:
        entity_byname = entity_items["entity_byname"]
        unit_name, node_in = entity_byname
        # Connect the unit to the atmosphere
        add_entity(target_db,"unit__to_node",(unit_name,"atmosphere"))

        # Check carbon capture technology coupled
        cc_capability = [element for element in target_db.get_entity_items(entity_class_name="unit__to_node") if "CO2" in element["entity_byname"][1] and unit_name == element["entity_byname"][0]]
        if not cc_capability:
            add_entity(target_db,"unit__node__node",(unit_name,"atmosphere",node_in))
            add_parameter_value(target_db,"unit__node__node","fix_ratio_out_in_unit_flow","Base",(unit_name,"atmosphere",node_in),co2_value[node_in])
        else:
            # Build the equations for balancing emissions
            add_entity(target_db,"user_constraint",(f"emissions_{unit_name}",))
            # Connect the user_constraints with the unit_flows
            add_entity(target_db,"unit__to_node__user_constraint",(unit_name,"atmosphere",f"emissions_{unit_name}"))
            add_entity(target_db,"unit__from_node__user_constraint",(unit_name,node_in,f"emissions_{unit_name}"))
            # Add the unit_flow coefficients in this expression
            add_parameter_value(target_db,"unit__to_node__user_constraint","unit_flow_coefficient","Base",(unit_name,"atmosphere",f"emissions_{unit_name}"),1.0)
            add_parameter_value(target_db,"unit__from_node__user_constraint","unit_flow_coefficient","Base",(unit_name,node_in,f"emissions_{unit_name}"),-co2_value[node_in])
        
            unit_name, node_out = cc_capability[0]["entity_byname"]
            # Connect the user_constraints with the unit_flows
            add_entity(target_db,"unit__to_node__user_constraint",(unit_name,node_out,f"emissions_{unit_name}"))
            # Add the unit_flow coefficients in this expression
            add_parameter_value(target_db,"unit__to_node__user_constraint","unit_flow_coefficient","Base",(unit_name,node_out,f"emissions_{unit_name}"),1.0)

    try:
        target_db.commit_session("Added process capacities")
    except DBAPIError as e:
        print("commit process capacities error")

def map_of_ts_conversion_ts_alternatives(source_db,target_db,settings):
    
    duration   = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "duration")[0]["value"])["data"]
    starttime  = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "start_time")[0]["value"])["data"]
    resolution = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "time_resolution")[0]["value"])["data"]
                
    for source_param in settings:
        for source_entity_class in settings[source_param]:
            for param_map in [i for i in source_db.get_parameter_value_items(entity_class_name = source_entity_class, parameter_definition_name = source_param) if i["type"] == "map"]:

                index_names = nested_index_names(param_map["parsed_value"])

                map_table = convert_map_to_table(param_map["parsed_value"])
                index_names = nested_index_names(param_map["parsed_value"])
                data = pd.DataFrame(map_table, columns=index_names + ["value"]).set_index(index_names[0])
                data.index = data.index.astype("string")

                if any(i in data.index for i in starttime):
                    map_export = {"type": "map","index_type": "str", "data":{}}
                    for index, element in enumerate(starttime):
                        try:
                            alternative_name = f"wy{str(pd.Timestamp(element).year)}"
                            add_alternative(target_db,alternative_name)
                            #add_entity(target_db,"stochastic_scenario",(f"wy{str(pd.Timestamp(element).year)}",))
                            #add_entity(target_db,"stochastic_structure__stochastic_scenario",("deterministic",f"wy{str(pd.Timestamp(element).year)}"))
                        except:
                            pass
                        param_list = settings[source_param][source_entity_class]
                        target_entity_class = param_list[0]
                        steps = pd.to_timedelta(duration) / pd.to_timedelta(resolution)
                        df_data = (float(param_list[2])*data.iloc[data.index.tolist().index(element):data.index.tolist().index(element)+int(steps),data.columns.tolist().index("value")]).tolist()
                        # map_export["data"][f"realization_{index+1}"] = {"type": "time_series","data": df_data,"index": {"start": f"2018{element[4:]}","resolution": resolution,"ignore_year": True}}
                        ts_export = {"type": "time_series","data": df_data,"index": {"start": f"2018{element[4:]}","resolution": resolution,"ignore_year": True}}
                        target_names = tuple(["__".join([param_map["entity_byname"][int(i)-1] for i in k]) for k in param_list[3]])
                        add_parameter_value(target_db,target_entity_class,param_list[1],alternative_name,target_names,ts_export)

    try:
        target_db.commit_session("Added historical timeseries error")
    except DBAPIError as e:
        print("commit process historical timeseries error")

def map_of_periods_to_ts(source_db,target_db,settings):
    
    starttime = {} 
    year_repr = {} 
    for period in json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "period")[0]["value"])["data"]:
        starttime[period] = json.loads(source_db.get_parameter_value_item(entity_class_name = "period", entity_byname = (period,), alternative_name = "Base", parameter_definition_name = "start_time")["value"])["data"]
        year_repr[period] = source_db.get_parameter_value_item(entity_class_name = "period", entity_byname = (period,), alternative_name = "Base", parameter_definition_name = "years_represented")["parsed_value"]
                    
    for source_entity_class in settings:
        for target_entity_class in settings[source_entity_class]:
            for source_param in settings[source_entity_class][target_entity_class]:
                print(source_entity_class,target_entity_class,source_param)
                param_elements = settings[source_entity_class][target_entity_class][source_param]

                if isinstance(param_elements,list):
                    target_param = param_elements[0]
                    multiplier = float(param_elements[1])
                    target_order = param_elements[2]
                elif isinstance(param_elements,dict):
                    target_param = param_elements["target"][0]
                    target_order = param_elements["target"][2]
                    op = operations[param_elements["operation"]]
                    multiplier = op(float(param_elements["target"][1]),float(param_elements["with"]))

                for param_map in source_db.get_parameter_value_items(entity_class_name = source_entity_class, parameter_definition_name = source_param):

                    if param_map["type"] == "map":

                        map_table = convert_map_to_table(param_map["parsed_value"])
                        index_names = nested_index_names(param_map["parsed_value"])
                        data = pd.DataFrame(map_table, columns=index_names + ["value"]).set_index(index_names[0])
                        data.index = data.index.astype("string")

                        indexes_ = []
                        values_ = []
                        for period_, ts_index_ in starttime.items():
                            values_.append(multiplier*(float(data.at[period_,"value"]) if period_ in data.index else 0.0))
                            values_.append(values_[-1])
                            # this should be removed once the fixed resolution is repaired
                            indexes_.append(ts_index_)
                            indexes_.append((pd.Timestamp(ts_index_).replace(year=int(pd.Timestamp(ts_index_).year+year_repr[period_]))-pd.Timedelta("1h")).isoformat())

                        ts_to_export = {"type": "time_series","data": dict(zip(indexes_,values_)),}
                        target_names = tuple(["__".join([param_map["entity_byname"][int(i)-1] for i in k]) for k in target_order])
                        add_parameter_value(target_db,target_entity_class,target_param,"Base",target_names,ts_to_export)
                    
                    elif param_map["type"] == "float":
                        target_names = tuple(["__".join([param_map["entity_byname"][int(i)-1] for i in k]) for k in target_order])
                        add_parameter_value(target_db,target_entity_class,target_param,"Base",target_names,multiplier*param_map["parsed_value"])
          
    try:
        target_db.commit_session("Added map of periods to timeseries")
    except DBAPIError as e:
        print("commit map of periods to timeseries error")

def timeline_setup(source_db,target_db):

    # model_data
    model_name = source_db.get_entity_items(entity_class_name = "solve_pattern")[0]["name"]
    # Process scenario realizations
    sto_structure = "deterministic"
    sto_scenario  = "realization"
    add_entity(target_db,"stochastic_structure",(sto_structure,))
    add_entity(target_db,"model__default_stochastic_structure",(model_name,sto_structure))
    add_entity(target_db,"model__default_investment_stochastic_structure",(model_name,sto_structure))
    add_entity(target_db,"stochastic_scenario",(sto_scenario,))
    add_entity(target_db,"stochastic_structure__stochastic_scenario",(sto_structure,sto_scenario))
    
    periods = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "period")[0]["value"])["data"]
    resolution = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "time_resolution")[0]["value"])["data"]
    
    # historical data
    duration = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "duration")[0]["value"])["data"]

    # if not multiyear
    if len(periods) == 1:
        print("it is not a multiyear invesment problem")
        # model horizon
        for period in periods:
            py_start = json.loads(source_db.get_parameter_value_item(entity_class_name = "period", parameter_definition_name = "start_time", alternative_name = "Base", entity_byname = (period,))["value"])["data"]
            print("Leap Year: ", bool(pd.Timestamp(py_start).year % 4 == 0))
            extra_duration = pd.Timedelta("1D") if pd.Timestamp(py_start).year % 4 == 0 else pd.Timedelta("0h")
            py_end = (pd.Timestamp(py_start) + pd.Timedelta(duration) + extra_duration).isoformat()
            add_parameter_value(target_db,"model","model_start","Base",(model_name,),{"type":"date_time","data":py_start})
            add_parameter_value(target_db,"model","model_end","Base",(model_name,),{"type":"date_time","data":py_end})

        # operational_resolution
        temporal_block_name = "operations"
        add_entity(target_db,"temporal_block",(temporal_block_name,))
        add_entity(target_db,"model__default_temporal_block",(model_name,temporal_block_name))
        add_parameter_value(target_db,"temporal_block","resolution","Base",(temporal_block_name,),{"type":"duration","data":resolution})

        # investment_resolution
        temporal_block_name = "planning"
        add_entity(target_db,"temporal_block",(temporal_block_name,))
        add_entity(target_db,"model__default_investment_temporal_block",(model_name,temporal_block_name))
        add_parameter_value(target_db,"temporal_block","resolution","Base",(temporal_block_name,),{"type":"duration","data":duration})
        
    try:
        target_db.commit_session("Added timeline")
    except DBAPIError as e:
        print("commit timeline error")

# investment_costs_annualized
# default_parameters_model     
        
if __name__ == "__main__":
    main()