import spinedb_api as api
from spinedb_api import DatabaseMapping, DateTime, Map, to_database
from spinedb_api.parameter_value import convert_map_to_table, IndexedValue
from sqlalchemy.exc import DBAPIError
import yaml
import sys
from ines_tools import ines_transform
import pandas as pd
import json
import numpy as np

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
    parameter_methods = yaml.safe_load(file)
with open('ines_to_spineopt_entities_to_parameters.yaml', 'r') as file:
    entities_to_parameters = yaml.load(file, yaml.BaseLoader)
with open('settings.yaml', 'r') as file:
    settings = yaml.safe_load(file)

def add_entity_group(db_map : DatabaseMapping, class_name : str, group : str, member : str) -> None:
    _, error = db_map.add_entity_group_item(group_name = group, member_name = member, entity_class_name=class_name)
    if error is not None:
        raise RuntimeError(error)
    
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
    
def add_scenario(db_map : DatabaseMapping,name_scenario : str) -> None:
    _, error = db_map.add_scenario_item(name=name_scenario)
    if error is not None:
        raise RuntimeError(error)

def add_scenario_alternative(db_map : DatabaseMapping,name_scenario : str, name_alternative : str, rank_int = None) -> None:
    _, error = db_map.add_scenario_alternative_item(scenario_name = name_scenario, alternative_name = name_alternative, rank = rank_int)
    if error is not None:
        raise RuntimeError(error)

def parameter_features(param_elements,source_db,source_entity_class,source_entity_names,source_alternative):

    if isinstance(param_elements,list):
        target_param = param_elements[0]
        multiplier = float(param_elements[1])
        target_order = param_elements[2]
    elif isinstance(param_elements,dict):
        target_param = param_elements["target"][0]
        conver_factor = float(param_elements["target"][1])
        target_order = param_elements["target"][2]
        op = operations[param_elements["operation"]]
        try:
            with_value = float(param_elements["with"])
        except:
            print("operating with ",param_elements["with"])
            value_ = source_db.get_parameter_value_item(entity_class_name = source_entity_class, parameter_definition_name = param_elements["with"], entity_byname = source_entity_names, alternative_name = source_alternative)
            if value_:
                with_value = value_["parsed_value"]
            else:
                raise ValueError(f"{param_elements['with']} does not exist for {source_entity_class} {source_entity_names}")
        multiplier = conver_factor*op(float(param_elements["target"][1]),with_value)

    return target_param, target_order, multiplier

def main():
    with DatabaseMapping(url_db_in) as source_db:
        with DatabaseMapping(url_db_out) as target_db:
            ## Empty the database
            target_db.purge_items('parameter_value')
            target_db.purge_items('entity')
            target_db.purge_items('alternative')
            target_db.purge_items('scenario')
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

            # Manual functions
            # timeline configuration for spineopt model
            timeline_setup(source_db,target_db)

            ## historical and future time series
            map_of_periods_or_historical_to_ts(source_db,target_db,settings["map_of_periods_or_historical_to_ts"])

            ## flow profiles addition
            flow_profile_method(source_db,target_db)

            ## investments not allowed
            limiting_investments_notallowed(source_db,target_db)

            # Process emisssions balance equations
            process_emissions(source_db,target_db)

            # Fix boundary condition for storages
            storage_state_fix_method(source_db,target_db)
            storage_state_binding_method(source_db,target_db)

            # Set to group constraints
            set_to_entities_and_parameters(source_db,target_db)

            # Default parameters
            default_parameters(target_db,settings["default_parameters"])
            candidates_to_number_of(target_db)

            # existing capacity 
            existing_capacity(source_db,target_db)

            # lifetime to duration
            lifetime_to_duration(source_db,target_db,settings["lifetime_to_duration"])

             # unit flow transformation
            unit_flow_variants(source_db,target_db,settings)
        

def process_emissions(source_db, target_db):

    for param_map in source_db.get_parameter_value_items(entity_class_name="set", parameter_definition_name = "co2_max_cumulative"):
        if param_map:
            add_entity(target_db,"node",("atmosphere",))
            add_parameter_value(target_db,"node","has_state","Base",("atmosphere",),True)
            if param_map["type"] == "map":
                # getting periods info
                starttime = {} 
                year_repr = {} 
                for period in json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "period")[0]["value"])["data"]:
                    starttime[period] = json.loads(source_db.get_parameter_value_item(entity_class_name = "period", entity_byname = (period,), alternative_name = "Base", parameter_definition_name = "start_time")["value"])["data"]
                    year_repr[period] = source_db.get_parameter_value_item(entity_class_name = "period", entity_byname = (period,), alternative_name = "Base", parameter_definition_name = "years_represented")["parsed_value"]

                map_table = convert_map_to_table(param_map["parsed_value"])
                index_names = nested_index_names(param_map["parsed_value"])
                data = pd.DataFrame(map_table, columns=index_names + ["value"]).set_index(index_names[0])
                data.index = data.index.astype("string")

                if any(i in data.index for i in starttime):
                    indexes_ = []
                    values_ = []
                    for period_, ts_index_ in starttime.items():
                        values_.append((float(data.at[period_,"value"]) if period_ in data.index else 0.0))
                        
                        # this should be removed once the fixed resolution is repaired
                        indexes_.append(ts_index_)
                    values_.append(values_[-1]) 
                    indexes_.append((pd.Timestamp(ts_index_).replace(year=int(pd.Timestamp(ts_index_).year+year_repr[period_]))).isoformat())

                    ts_to_export = {"type": "time_series","data": dict(zip(indexes_,[i/max(values_) for i in values_]))}
                    add_parameter_value(target_db,"node","node_availability_factor",param_map["alternative_name"],param_map["entity_byname"],ts_to_export)
                    add_parameter_value(target_db,"node","node_state_cap",param_map["alternative_name"],param_map["entity_byname"],max(values_))
                    
    # unit flow coming from fossil nodes
    co2_params = source_db.get_parameter_value_items(entity_class_name="node",parameter_definition_name="co2_content",alternative_name="Base")
    co2_value  = {co2_param["entity_name"]:co2_param["parsed_value"] for co2_param in co2_params if co2_param["entity_name"] != "CO2"}

    for unit_entity in target_db.get_entity_items(entity_class_name="unit"):
        unit__from_nodes = [from_node for from_node in co2_value if target_db.get_entity_item(entity_class_name = "unit__from_node",entity_byname = (unit_entity["name"],from_node))]       
        unit_name = unit_entity["name"]
        if len(unit__from_nodes) > 1:
            add_entity(target_db,"unit__to_node",(unit_name,"atmosphere"))
            add_entity(target_db,"user_constraint",(unit_name+"_emissions",))
            add_entity(target_db,"unit__to_node__user_constraint",(unit_name,"atmosphere",unit_name+"_emissions"))
            add_parameter_value(target_db,"unit__to_node__user_constraint","unit_flow_coefficient","Base",(unit_name,"atmosphere",unit_name+"_emissions"),-1.0)
            for from_node in unit__from_nodes:
                add_entity(target_db,"unit__from_node__user_constraint",(unit_name,from_node,unit_name+"_emissions"))
                add_parameter_value(target_db,"unit__from_node__user_constraint","unit_flow_coefficient","Base",(unit_name,from_node,unit_name+"_emissions"),co2_value[from_node])
        elif len(unit__from_nodes) == 1:
            add_entity(target_db,"unit__to_node",(unit_name,"atmosphere"))
            add_entity(target_db,"unit__node__node",(unit_name,"atmosphere",unit__from_nodes[0]))
            add_parameter_value(target_db,"unit__node__node","fix_ratio_out_in_unit_flow","Base",(unit_name,"atmosphere",unit__from_nodes[0]),co2_value[unit__from_nodes[0]])

    for entity_items in [element for element in target_db.get_entity_items(entity_class_name="unit__to_node") if "CO2" in element["entity_byname"][1]]:
        entity_byname = entity_items["entity_byname"]
        unit_name, node_out = entity_byname
        add_entity(target_db,"unit__from_node",(unit_name,"atmosphere"))
        add_entity(target_db,"unit__node__node",(unit_name, node_out,"atmosphere"))
        add_parameter_value(target_db,"unit__node__node","fix_ratio_out_in_unit_flow","Base",(unit_name, node_out,"atmosphere"),1.0)

    try:
        target_db.commit_session("Added process capacities")
    except:
        print("commit process capacities error")

def map_of_periods_or_historical_to_ts(source_db,target_db,settings):
    
    starttime = {} 
    year_repr = {} 
    for period in json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "period")[0]["value"])["data"]:
        starttime[period] = json.loads(source_db.get_parameter_value_item(entity_class_name = "period", entity_byname = (period,), alternative_name = "Base", parameter_definition_name = "start_time")["value"])["data"]
        year_repr[period] = source_db.get_parameter_value_item(entity_class_name = "period", entity_byname = (period,), alternative_name = "Base", parameter_definition_name = "years_represented")["parsed_value"]

    duration      = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "duration")[0]["value"])
    starttime_sp  = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "start_time")[0]["value"])["data"]
    resolution    = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "time_resolution")[0]["value"])["data"]
                             
    for source_entity_class in settings:
        for target_entity_class in settings[source_entity_class]:
            for source_param in settings[source_entity_class][target_entity_class]:
                print(source_entity_class,target_entity_class,source_param)
                param_elements = settings[source_entity_class][target_entity_class][source_param]

                for param_map in source_db.get_parameter_value_items(entity_class_name = source_entity_class, parameter_definition_name = source_param):

                    target_param, target_order, multiplier = parameter_features(param_elements,source_db,source_entity_class,param_map["entity_byname"],param_map["alternative_name"])

                    if param_map["type"] == "map":

                        map_table = convert_map_to_table(param_map["parsed_value"])
                        index_names = nested_index_names(param_map["parsed_value"])
                        data = pd.DataFrame(map_table, columns=index_names + ["value"]).set_index(index_names[0])
                        data.index = data.index.astype("string")

                        if any(i in data.index for i in starttime):
                            indexes_ = []
                            values_ = []
                            for period_, ts_index_ in starttime.items():
                                values_.append(multiplier*(float(data.at[period_,"value"]) if period_ in data.index else 0.0))
                                
                                # this should be removed once the fixed resolution is repaired
                                indexes_.append(ts_index_)
                            values_.append(values_[-1]) 
                            indexes_.append((pd.Timestamp(ts_index_).replace(year=int(pd.Timestamp(ts_index_).year+year_repr[period_]))).isoformat())

                            ts_to_export = {"type": "time_series","data": dict(zip(indexes_,values_))}
                            target_names = tuple(["__".join([param_map["entity_byname"][int(i)-1] for i in k]) for k in target_order])
                            add_parameter_value(target_db,target_entity_class,target_param,param_map["alternative_name"],target_names,ts_to_export)
                    
                        if any(i in data.index for i in starttime_sp):
                            for index, element in enumerate(starttime_sp):
                                try:
                                    alternative_name = f"wy{str(pd.Timestamp(element).year)}"
                                    add_alternative(target_db,alternative_name)
                                except:
                                    pass
                                steps = pd.to_timedelta(duration) / pd.to_timedelta(resolution)
                                df_data = (multiplier*data.iloc[data.index.tolist().index(element):data.index.tolist().index(element)+int(steps),data.columns.tolist().index("value")]).tolist()
                                ts_export = {"type": "time_series","data": df_data,"index": {"start": f"2018{element[4:]}","resolution": resolution,"ignore_year": True}}
                                target_names = tuple(["__".join([param_map["entity_byname"][int(i)-1] for i in k]) for k in target_order])
                                add_parameter_value(target_db,target_entity_class,target_param,alternative_name,target_names,ts_export)
                    
                
                    elif param_map["type"] == "float":
                        target_names = tuple(["__".join([param_map["entity_byname"][int(i)-1] for i in k]) for k in target_order])
                        add_parameter_value(target_db,target_entity_class,target_param,param_map["alternative_name"],target_names,multiplier*param_map["parsed_value"])
          
    try:
        target_db.commit_session("Added map of periods, historical data to timeseries")
    except:
        print("commit map of periods, historical data to timeseries error")

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
    duration = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "duration")[0]["value"])
    py_yearrs = []
    # if not multiyear
    if len(periods) == 1:
        print("it is not a multiyear invesment problem")
        # model horizon
        for period in periods:
            py_start = json.loads(source_db.get_parameter_value_item(entity_class_name = "period", parameter_definition_name = "start_time", alternative_name = "Base", entity_byname = (period,))["value"])["data"]
            py_yearr = source_db.get_parameter_value_item(entity_class_name = "period", parameter_definition_name = "years_represented", alternative_name = "Base", entity_byname = (period,))["parsed_value"]
            py_yearrs.append(py_yearr)
            print("Leap Year: ", bool(pd.Timestamp(py_start).year % 4 == 0), period)
            extra_duration = pd.Timedelta("1D") if pd.Timestamp(py_start).year % 4 == 0 else pd.Timedelta("0h")
            py_end = (pd.Timestamp(py_start) + pd.Timedelta(duration) + extra_duration).isoformat()
            add_parameter_value(target_db,"model","model_start","Base",(model_name,),{"type":"date_time","data":py_start})
            add_parameter_value(target_db,"model","model_end","Base",(model_name,),{"type":"date_time","data":py_end})

            # operational_resolution
            temporal_block_name = "operations"
            add_entity(target_db,"temporal_block",(temporal_block_name,))
            add_entity(target_db,"model__default_temporal_block",(model_name,temporal_block_name))
            add_parameter_value(target_db,"temporal_block","resolution","Base",(temporal_block_name,),{"type":"duration","data":resolution})
            add_parameter_value(target_db,"temporal_block","weight","Base",(temporal_block_name,),py_yearr)

    else:
        print("Multiyear investment planning")
        py_starts = []
        py_ends = []
        py_yearrs = []
        # model horizon
        for period in periods:
            # add_alternative(target_db,period)
            py_start = json.loads(source_db.get_parameter_value_item(entity_class_name = "period", parameter_definition_name = "start_time", alternative_name = "Base", entity_byname = (period,))["value"])["data"]
            py_yearr = source_db.get_parameter_value_item(entity_class_name = "period", parameter_definition_name = "years_represented", alternative_name = "Base", entity_byname = (period,))["parsed_value"]
            py_yearrs.append(py_yearr)
            # operational_resolution
            temporal_block_name = f"operations_{period}"
            add_entity(target_db,"temporal_block",(temporal_block_name,))
            add_entity(target_db,"model__default_temporal_block",(model_name,temporal_block_name))
            add_parameter_value(target_db,"temporal_block","resolution","Base",(temporal_block_name,),{"type":"duration","data":resolution})
            if bool(pd.Timestamp(py_start).year % 4 == 0):
                print("Leap Year: ", bool(pd.Timestamp(py_start).year % 4 == 0), period)
                block_start = (pd.Timestamp(py_start) + pd.Timedelta(days=366) - (pd.Timedelta(resolution) if periods.index(period)>0 else pd.Timedelta("0h"))).isoformat()
                block_end = (pd.Timestamp(py_start) + pd.Timedelta(days=366) + pd.Timedelta(duration)).isoformat()
            else:
                block_start = (pd.Timestamp(py_start) - (pd.Timedelta(resolution) if periods.index(period)>0 else pd.Timedelta("0h"))).isoformat()
                block_end = (pd.Timestamp(py_start) + pd.Timedelta(duration)).isoformat()

            add_parameter_value(target_db,"temporal_block","block_start","Base",(temporal_block_name,),{"type":"date_time","data":block_start})
            add_parameter_value(target_db,"temporal_block","block_end","Base",(temporal_block_name,),{"type":"date_time","data":block_end})
            add_parameter_value(target_db,"temporal_block","weight","Base",(temporal_block_name,),py_yearr)
            
            py_starts.append(pd.Timestamp(py_start))
            py_ends.append(pd.Timestamp(str(int(py_start[:4]) + int(py_yearr))+"-01-01T00:00:00"))
        
        add_parameter_value(target_db,"model","model_start","Base",(model_name,),{"type":"date_time","data":min(py_starts).isoformat()})
        add_parameter_value(target_db,"model","model_end","Base",(model_name,),{"type":"date_time","data":max(py_ends).isoformat()})
        # add_parameter_value(target_db,"model","discount_year",period,(model_name,),{"type":"date_time","data":py_start})

    # investment_resolution
    temporal_block_name = "planning"
    add_entity(target_db,"temporal_block",(temporal_block_name,))
    add_entity(target_db,"model__default_investment_temporal_block",(model_name,temporal_block_name))
    add_parameter_value(target_db,"temporal_block","resolution","Base",(temporal_block_name,),{"type":"array","value_type":"duration","data":[f"{str(int(yearr))}Y" for yearr in py_yearrs]})

    try:
        target_db.commit_session("Added timeline")
    except:
        print("commit timeline error")

def storage_state_fix_method(source_db,target_db):
    
    for storage_method in source_db.get_parameter_value_items(parameter_definition_name = "storage_state_fix_method"):
        capacities_ = source_db.get_parameter_value_items(entity_class_name = storage_method["entity_class_name"], entity_byname= storage_method["entity_byname"], parameter_definition_name = "storage_capacity")
        if capacities_:
            if storage_method["parsed_value"] == "fix_start":
                values_ = source_db.get_parameter_value_items(entity_class_name = storage_method["entity_class_name"], entity_byname= storage_method["entity_byname"], parameter_definition_name = "storage_state_fix")
                if values_:
                    for capacity_ in capacities_:
                        for value_ in values_:
                            if value_["type"] == "float":
                                if capacity_["type"] == "float":
                                    target_value_ = value_["parsed_value"]*capacity_["parsed_value"]
                                if value_["alternative_name"] == capacity_["alternative_name"]:
                                    alternative_name = value_["alternative_name"]
                                else:
                                    if value_["alternative_name"] == "Base":
                                        alternative_name = capacity_["alternative_name"]
                                    elif capacity_["alternative_name"] == "Base":
                                        alternative_name = value_["alternative_name"]
                                    else:
                                        add_alternative(target_db,f"{capacity_['alternative_name']}_{value_['alternative_name']}")
                                        alternative_name = f"{capacity_['alternative_name']}_{value_['alternative_name']}"
                                add_parameter_value(target_db,"node","initial_node_state",alternative_name,value_["entity_byname"],target_value_)
                else:
                    print("WARNING: FIXED STATE DOES NOT EXIST ", storage_method["entity_byname"])
        else:
            print("WARNING: CAPACITY NOT DEFINED, THEN FIX STATE NOT ADDED", storage_method["entity_byname"])
    try:
        target_db.commit_session("Added fixed storage state method")
    except:
        print("commit fixed storage state error")

def storage_state_binding_method(source_db,target_db):

    for storage_method in source_db.get_parameter_value_items(parameter_definition_name = "storage_state_binding_method"):
        if storage_method["parsed_value"] == "leap_over_within_solve":
            for entity_map in target_db.get_entity_items(entity_class_name = "model__default_temporal_block"):
                add_entity(target_db,"node__temporal_block",(storage_method["entity_name"],entity_map["entity_byname"][1]))
                add_parameter_value(target_db,"node__temporal_block","cyclic_condition","Base",(storage_method["entity_name"],entity_map["entity_byname"][1]),True)
    try:
        target_db.commit_session("Added storage state binding method")
    except:
        print("commit storage state binding method error")

def limiting_investments_notallowed(source_db,target_db):

    target_candi = {"unit":"units_existing","link":"links_existing","node":"storages_existing"}
    target_class = {"unit":"unit","link":"connection","node":"node"}
    target_param = {"unit":"candidate_units","link":"candidate_connections","node":"candidate_storages"}
    fix_param = {"unit":"fix_units_invested","link":"fix_connections_invested","node":"fix_storages_invested"}

    starttime = {} 
    year_repr = {} 

    for period in json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "period")[0]["value"])["data"]:
        starttime[period] = json.loads(source_db.get_parameter_value_item(entity_class_name = "period", entity_byname = (period,), alternative_name = "Base", parameter_definition_name = "start_time")["value"])["data"]
        year_repr[period] = source_db.get_parameter_value_item(entity_class_name = "period", entity_byname = (period,), alternative_name = "Base", parameter_definition_name = "years_represented")["parsed_value"]

    for source_param in ["investment_method","storage_investment_method"]:
        for param_map in [i for i in source_db.get_parameter_value_items(parameter_definition_name = source_param) if i["parsed_value"]=="not_allowed"]:
            existing_ = source_db.get_parameter_value_item(entity_class_name = param_map["entity_class_name"], parameter_definition_name = target_candi[param_map["entity_class_name"]], entity_byname = param_map["entity_byname"], alternative_name = param_map["alternative_name"])
            if existing_:
                if existing_["type"] == "map":
                    
                    map_table = convert_map_to_table(existing_["parsed_value"])
                    index_names = nested_index_names(existing_["parsed_value"])
                    data = pd.DataFrame(map_table, columns=index_names + ["value"]).set_index(index_names[0])
                    data.index = data.index.astype("string")

                    if any(i in data.index for i in starttime):
                        indexes_ = []
                        values_ = []
                        for period_, ts_index_ in starttime.items():
                            values_.append((float(data.at[period_,"value"]) if period_ in data.index else 0.0))
                            
                            # this should be removed once the fixed resolution is repaired
                            indexes_.append(ts_index_)
                        values_.append(values_[-1]) 
                        indexes_.append((pd.Timestamp(ts_index_).replace(year=int(pd.Timestamp(ts_index_).year+year_repr[period_]))).isoformat())

                        if len(values_) > 1:
                            value_ = {"type": "time_series","data": dict(zip(indexes_,values_))}
                        else:
                            value_ = values_[0]

                        add_parameter_value(target_db,target_class[param_map["entity_class_name"]],target_param[param_map["entity_class_name"]],existing_["alternative_name"],existing_["entity_byname"],value_)
                        add_parameter_value(target_db,target_class[param_map["entity_class_name"]],fix_param[param_map["entity_class_name"]],existing_["alternative_name"],existing_["entity_byname"],0.0)
                
                elif existing_["type"] == "float":
                    value_ = existing_["parsed_value"]
                    add_parameter_value(target_db,target_class[param_map["entity_class_name"]],target_param[param_map["entity_class_name"]],existing_["alternative_name"],existing_["entity_byname"],value_)
                    add_parameter_value(target_db,target_class[param_map["entity_class_name"]],fix_param[param_map["entity_class_name"]],existing_["alternative_name"],existing_["entity_byname"],0.0)
                
            else: 
                print(f"There is no existing capacity in {param_map['entity_class_name']} {param_map['entity_byname']}")

    try:
        target_db.commit_session("Added candadite assets")
    except:
        print("commit candadite assets error")    

def set_to_entities_and_parameters(source_db,target_db):

    model_duration = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "duration")[0]["value"])
    resolution = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "time_resolution")[0]["value"])["data"]
    
    for source_parameter in ["max_cumulative","flow_max_cumulative"]:
        for source_dict_parameter in source_db.get_parameter_value_items(entity_class_name = "set", parameter_definition_name = source_parameter):
            source_relationships = {relation: [] for relation in ["set__unit_flow","set__node","set__unit","set__link"]}
            for relation in source_relationships:  
                for element in source_db.get_entity_items(entity_class_name = relation):
                    if element["entity_byname"][0] == source_dict_parameter["entity_byname"][0]:
                        source_relationships[relation].append(element["entity_byname"])
            if source_parameter == "max_cumulative":
                try:
                    add_entity(target_db,"investment_group",source_dict_parameter["entity_byname"])
                    print("Entity already created","investment_group",source_dict_parameter["entity_byname"])
                except:
                    pass
                add_parameter_value(target_db,"investment_group","maximum_entities_invested_available",source_dict_parameter["alternative_name"],source_dict_parameter["entity_byname"],source_dict_parameter["parsed_value"])
                for entity_relation, list_relation in source_relationships.items():
                    if entity_relation == "set__unit":
                        for names_relation in list_relation:
                            entity_byname = (names_relation[1],names_relation[0])
                            add_entity(target_db,"unit__investment_group",entity_byname)
                    if entity_relation == "set__node":
                        for names_relation in list_relation:
                            entity_byname = (names_relation[1],names_relation[0])
                            add_entity(target_db,"node__investment_group",entity_byname)
                    if entity_relation == "set__link":
                        for names_relation in list_relation:
                            entity_byname = (names_relation[1],names_relation[0])
                            add_entity(target_db,"connection__investment_group",entity_byname)

            elif source_parameter == "flow_max_cumulative":
                if len(source_relationships) == 1:
                    for entity_relation, names_relation in source_relationships.items():
                        if entity_relation == "set__unit_flow":
                            source_flow = source_db.get_entity_items(entity_byname = names_relation[1:])[0]["entity_class_name"]
                            target_entity_class = "unit__from_node" if source_flow == "node__to_unit" else "unit__to_node"
                            target_entity_names = (names_relation[2],names_relation[1]) if source_flow == "node__to_unit" else (names_relation[1],names_relation[2])
                            try:
                                add_entity(target_db,target_entity_class,target_entity_names)
                            except:
                                pass
                            model_duration_hours = pd.Timedelta(model_duration) / pd.Timedelta(resolution)
                            param_value = model_duration_hours * source_dict_parameter["parsed_value"]
                            add_parameter_value(target_db,target_entity_class,"max_total_cumulated_unit_flow_to_node",source_dict_parameter["alternative_name"],target_entity_names,param_value)
                else:
                    pass
    try:
        target_db.commit_session("Added set constraints")
    except:
        print("commit set constraints error")  

def default_parameters(target_db,settings):
    for target_entity_class in settings:
        for entity_item in target_db.get_entity_items(entity_class_name = target_entity_class):
            for target_parameter in settings[target_entity_class]:
                add_parameter_value(target_db,target_entity_class,target_parameter,"Base",entity_item["entity_byname"],settings[target_entity_class][target_parameter])
    try:
        target_db.commit_session("Added default_parameters")
    except:
        print("commit default_parameters error")  

def candidates_to_number_of(target_db):
    parameter_conversion = {"candidate_units":"number_of_units","candidate_connections":"number_of_connections","candidate_storages":"number_of_storages"}
    for parameter_name in parameter_conversion:
        for param_map in target_db.get_parameter_value_items(parameter_definition_name = parameter_name):
            add_parameter_value(target_db,param_map["entity_class_name"],parameter_conversion[parameter_name],param_map["alternative_name"],param_map["entity_byname"],0.0)

    try:
        target_db.commit_session("Added candidate to number of")
    except:
        print("commit candidate to number of error")

def existing_capacity(source_db,target_db):

    entity_map = {"unit":"unit","node":"node","link":"connection"}
    parameter_conversion = {"units_existing":"initial_units_invested_available","links_existing":"initial_connections_invested_available","storages_existing":"initial_storages_invested_available"}
    for source_parameter in parameter_conversion:
        target_parameter = parameter_conversion[source_parameter]
        for param_map in source_db.get_parameter_value_items(parameter_definition_name = source_parameter):
            param_dict = json.loads(param_map["value"].decode("utf-8"))
            param_value = param_dict["data"]
            target_entity = entity_map[param_map["entity_class_name"]]
            vals = np.fromiter(param_value.values(), dtype=float)
            add_parameter_value(target_db,target_entity,target_parameter,"Base",param_map["entity_byname"],vals[0])
    try:
        target_db.commit_session("Added existing capacity")
    except:
        print("commit existing capacity error")

def lifetime_to_duration(source_db,target_db,settings):

    for source_class in settings:
        for target_class in settings[source_class]:
            for source_param in settings[source_class][target_class]:
                for param_map in source_db.get_parameter_value_items(entity_class_name = source_class, parameter_definition_name = source_param):
                    if param_map["type"] == "float":
                        param_value = {"type":"duration","data":str(int(param_map["parsed_value"]))+"Y"}
                    
                    for target_param in settings[source_class][target_class][source_param]:
                        print(target_param, param_map["entity_byname"])
                        add_parameter_value(target_db,target_class,target_param,param_map["alternative_name"],param_map["entity_byname"],param_value)
                
    try:
        target_db.commit_session("Added lifetime conversion")
    except:
        print("commit lifetime conversion error")

def unit_flow_variants(source_db,target_db,settings):

    parameters_mapping = {"equality_ratio":"fix_ratio","less_than_ratio":"max_ratio_","greater_than_ration":"min_ratio_"}
    for param_map in source_db.get_parameter_value_items(entity_class_name = "unit_flow__unit_flow"):


        unit_flow_1 = (param_map["entity_byname"][0],param_map["entity_byname"][1])
        unit_flow_2 = (param_map["entity_byname"][2],param_map["entity_byname"][3])

        entity_1 = source_db.get_entity_items(entity_byname = unit_flow_1)[0]["entity_class_name"]
        entity_2 = source_db.get_entity_items(entity_byname = unit_flow_2)[0]["entity_class_name"]

        flow_direction_1 = "in" if entity_1 == "node__to_unit" else "out"
        flow_direction_2 = "in" if entity_2 == "node__to_unit" else "out"

        unit_name = unit_flow_1[1] if entity_1 == "node__to_unit" else unit_flow_1[0] 
        node_1 = unit_flow_1[0] if entity_1 == "node__to_unit" else unit_flow_1[1] 
        node_2 = unit_flow_2[0] if entity_2 == "node__to_unit" else unit_flow_2[1] 

        target_parameter = parameters_mapping[param_map["parameter_definition_name"]]+f"_{flow_direction_1}_{flow_direction_2}_unit_flow"

        add_entity(target_db,"unit__node__node",(unit_name,node_1,node_2))
        
        if param_map["type"] == "float":
            add_parameter_value(target_db,"unit__node__node",target_parameter,param_map["alternative_name"],(unit_name,node_1,node_2),param_map["parsed_value"])
        
        elif param_map["type"] == "map":
            
            duration   = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "duration")[0]["value"])
            starttime  = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "start_time")[0]["value"])["data"]
            resolution = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "time_resolution")[0]["value"])["data"]

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
                    except:
                        pass
                    steps = pd.to_timedelta(duration) / pd.to_timedelta(resolution)
                    df_data = (data.iloc[data.index.tolist().index(element):data.index.tolist().index(element)+int(steps),data.columns.tolist().index("value")]).tolist()
                    ts_export = {"type": "time_series","data": df_data,"index": {"start": f"2018{element[4:]}","resolution": resolution,"ignore_year": True}}
                    add_parameter_value(target_db,"unit__node__node",target_parameter,alternative_name,(unit_name,node_1,node_2),ts_export)
                   

    try:
        target_db.commit_session("Added unit flows")
    except:
        print("commit unit flows error")

def flow_profile_method(source_db,target_db):

    duration   = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "duration")[0]["value"])
    starttime  = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "start_time")[0]["value"])["data"]
    resolution = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "time_resolution")[0]["value"])["data"]
                

    for param_map in source_db.get_parameter_value_items(entity_class_name = "node", alternative_name = "Base", parameter_definition_name = "flow_profile"):

        flow_method = source_db.get_parameter_value_item(entity_class_name = "node", alternative_name = "Base", entity_byname = param_map["entity_byname"], parameter_definition_name = "flow_scaling_method")
        
        if flow_method["parsed_value"] == "scale_to_annual":
            target_name = param_map["entity_name"]+"-group"
            add_entity(target_db,"node",(target_name,))
            add_parameter_value(target_db,"node","balance_type","Base",(target_name,),"balance_type_none")
            add_entity_group(target_db,"node",target_name,param_map["entity_name"])
            definition_condition = True
        elif flow_method["parsed_value"] == "use_profile_directly":
            target_name = param_map["entity_name"]
            definition_condition = True
        else:
            print("flow profile wont be defined")
            definition_condition = False
        
        if definition_condition:
            if param_map["type"] == "map":
                index_names = nested_index_names(param_map["parsed_value"])
                map_table = convert_map_to_table(param_map["parsed_value"])
                index_names = nested_index_names(param_map["parsed_value"])
                data = pd.DataFrame(map_table, columns=index_names + ["value"]).set_index(index_names[0])
                data.index = data.index.astype("string")

                if any(i in data.index for i in starttime):
                    for index, element in enumerate(starttime):
                        try:
                            alternative_name = f"wy{str(pd.Timestamp(element).year)}"
                            add_alternative(target_db,alternative_name)
                        except:
                            pass
                        steps = pd.to_timedelta(duration) / pd.to_timedelta(resolution)
                        df_data = (-1.0*data.iloc[data.index.tolist().index(element):data.index.tolist().index(element)+int(steps),data.columns.tolist().index("value")]).tolist()
                        ts_export = {"type": "time_series","data": df_data,"index": {"start": f"2018{element[4:]}","resolution": resolution,"ignore_year": True}}
                        add_parameter_value(target_db,"node","demand",alternative_name,(target_name,),ts_export)
            
            elif param_map["type"] == "float":
                add_parameter_value(target_db,"node","demand",param_map["alternative_name"],(target_name,),-1.0*param_map["parsed_value"])
          
    try:
        target_db.commit_session("Added flow profile")
    except:
        print("commit flow profile error")

if __name__ == "__main__":
    main()