import spinedb_api as api
from spinedb_api import DatabaseMapping, DateTime, Map, to_database
from spinedb_api.parameter_value import convert_map_to_table, IndexedValue
from sqlalchemy.exc import DBAPIError
from spinedb_api.exception import NothingToCommit
import yaml
import sys
from ines_tools import ines_transform
import pandas as pd
import json
import numpy as np


def nested_index_names(value, names=None, depth=0):
    if names is None:
        names = []
    if depth == len(names):
        names.append(value.index_name)
    elif value.index_name != names[-1]:
        raise RuntimeError(
            f"Index names at depth {depth} do no match: {value.index_name} vs. {names[-1]}"
        )
    for y in value.values:
        if isinstance(y, IndexedValue):
            nested_index_names(y, names, depth + 1)
    return names


operations = {
    "multiply": lambda x, y: x * y,
    "add": lambda x, y: x + y,
    "subtract": lambda x, y: x - y,
    "divide": lambda x, y: x / y,
    "constant": lambda x, y: y,
}

if len(sys.argv) > 1:
    url_db_in = sys.argv[1]
else:
    exit(
        "Please provide input database url and output database url as arguments. They should be of the form "
        "sqlite:///path/db_file.sqlite"
        ""
    )
if len(sys.argv) > 2:
    url_db_out = sys.argv[2]
else:
    exit(
        "Please provide input database url and output database url as arguments. They should be of the form "
        "sqlite:///path/db_file.sqlite"
        ""
    )

with open("ines_to_spineopt_entities.yaml", "r") as file:
    entities_to_copy = yaml.load(file, yaml.BaseLoader)
with open("ines_to_spineopt_parameters.yaml", "r") as file:
    parameter_transforms = yaml.load(file, yaml.BaseLoader)
with open("ines_to_spineopt_methods.yaml", "r") as file:
    parameter_methods = yaml.safe_load(file)
with open("ines_to_spineopt_entities_to_parameters.yaml", "r") as file:
    entities_to_parameters = yaml.load(file, yaml.BaseLoader)
with open("settings.yaml", "r") as file:
    settings = yaml.safe_load(file)


def add_entity_group(
    db_map: DatabaseMapping, class_name: str, group: str, member: str
) -> None:
    _, error = db_map.add_entity_group_item(
        group_name=group, member_name=member, entity_class_name=class_name
    )
    if error is not None:
        raise RuntimeError(error)


def add_entity(
    db_map: DatabaseMapping, class_name: str, name: tuple, ent_description=None
) -> None:
    _, error = db_map.add_entity_item(
        entity_byname=name, entity_class_name=class_name, description=ent_description
    )
    if error is not None:
        raise RuntimeError(error)


def add_parameter_value(
    db_map: DatabaseMapping,
    class_name: str,
    parameter: str,
    alternative: str,
    elements: tuple,
    value: any,
) -> None:
    db_value, value_type = api.to_database(value)
    _, error = db_map.add_parameter_value_item(
        entity_class_name=class_name,
        entity_byname=elements,
        parameter_definition_name=parameter,
        alternative_name=alternative,
        value=db_value,
        type=value_type,
    )
    if error:
        raise RuntimeError(error)


def add_alternative(db_map: DatabaseMapping, name_alternative: str) -> None:
    _, error = db_map.add_alternative_item(name=name_alternative)
    if error is not None:
        raise RuntimeError(error)


def add_scenario(db_map: DatabaseMapping, name_scenario: str) -> None:
    _, error = db_map.add_scenario_item(name=name_scenario)
    if error is not None:
        raise RuntimeError(error)


def add_scenario_alternative(
    db_map: DatabaseMapping, name_scenario: str, name_alternative: str, rank_int=None
) -> None:
    _, error = db_map.add_scenario_alternative_item(
        scenario_name=name_scenario, alternative_name=name_alternative, rank=rank_int
    )
    if error is not None:
        raise RuntimeError(error)


def parameter_features(
    param_elements,
    source_db,
    source_entity_class,
    source_entity_names,
    source_alternative,
):

    if isinstance(param_elements, list):
        target_param = param_elements[0]
        multiplier = float(param_elements[1])
        target_order = param_elements[2]
    elif isinstance(param_elements, dict):
        target_param = param_elements["target"][0]
        conver_factor = float(param_elements["target"][1])
        target_order = param_elements["target"][2]
        op = operations[param_elements["operation"]]
        try:
            with_value = float(param_elements["with"])
        except:
            print("operating with ", param_elements["with"])
            value_ = source_db.get_parameter_value_item(
                entity_class_name=source_entity_class,
                parameter_definition_name=param_elements["with"],
                entity_byname=source_entity_names,
                alternative_name=source_alternative,
            )
            if value_:
                with_value = value_["parsed_value"]
            else:
                raise ValueError(
                    f"{param_elements['with']} does not exist for {source_entity_class} {source_entity_names}"
                )
        multiplier = conver_factor * op(float(param_elements["target"][1]), with_value)

    return target_param, target_order, multiplier


def main():
    with DatabaseMapping(url_db_in) as source_db:
        with DatabaseMapping(url_db_out) as target_db:
            ## Empty the database
            target_db.purge_items("parameter_value")
            target_db.purge_items("entity")
            target_db.purge_items("alternative")
            target_db.purge_items("scenario")
            target_db.refresh_session()
            target_db.commit_session("Purged stuff")

            ## Copy alternatives
            for alternative in source_db.get_alternative_items():
                target_db.add_alternative_item(name=alternative["name"])
            for scenario in source_db.get_scenario_items():
                target_db.add_scenario_item(name=scenario["name"])
            for scenario_alternative in source_db.get_scenario_alternative_items():
                target_db.add_scenario_alternative_item(
                    alternative_name=scenario_alternative["alternative_name"],
                    scenario_name=scenario_alternative["scenario_name"],
                    rank=scenario_alternative["rank"],
                )

            ## Copy entites
            target_db = ines_transform.copy_entities(
                source_db, target_db, entities_to_copy
            )
            ## Copy numeric parameters(source_db, target_db, copy_entities)
            target_db = ines_transform.transform_parameters(
                source_db, target_db, parameter_transforms
            )
            ## Copy methods(source_db, target_db, copy_entities)
            target_db = ines_transform.process_methods(
                source_db, target_db, parameter_methods
            )

            # node__to_unit capacity → unit__to_node capacity_per_unit (× max efficiency)
            process_input_capacity(source_db, target_db)

            # Convert min_up_time and min_down_time from float hours to duration
            for duration_param in ["min_up_time", "min_down_time"]:
                for pv in target_db.get_parameter_value_items(
                    entity_class_name="unit",
                    parameter_definition_name=duration_param,
                ):
                    hours = pv["parsed_value"]
                    if isinstance(hours, (int, float)):
                        db_val, val_type = api.to_database(
                            {"type": "duration", "data": f"{int(hours)}h"}
                        )
                        target_db.update_parameter_value_item(
                            entity_class_name="unit",
                            entity_byname=pv["entity_byname"],
                            parameter_definition_name=duration_param,
                            alternative_name=pv["alternative_name"],
                            value=db_val,
                            type=val_type,
                        )
            try:
                target_db.commit_session("Converted min_up/down_time to duration")
            except NothingToCommit:
                pass
            except DBAPIError as e:
                print("commit min_up/down_time to duration error:", e)
            ## Copy entities to parameters
            # target_db = ines_transform.copy_entities_to_parameters(source_db, target_db, entities_to_parameters)

            # Manual functions
            # timeline configuration for spineopt model
            timeline_setup(source_db, target_db)

            ## historical and future time series
            map_of_periods_or_historical_to_ts(
                source_db, target_db, settings["map_of_periods_or_historical_to_ts"]
            )

            ## flow profiles addition
            flow_profile_method(source_db, target_db)

            ## availability and profile_limit_upper
            process_availability(source_db, target_db)

            ## investments not allowed
            limiting_investments_notallowed(source_db, target_db)

            # Process emissions (CO2, SO2, NOx) - flows, limits, and prices
            process_emissions(source_db, target_db)

            # Fix boundary condition for storages
            storage_state_fix_method(source_db, target_db)
            storage_state_binding_method(source_db, target_db)

            # Set to group constraints
            set_to_entities_and_parameters(source_db, target_db)

            # Default parameters
            default_parameters(target_db, settings["default_parameters"])
            candidates_to_number_of(target_db)

            # existing capacity
            existing_capacity(source_db, target_db)

            # per-period investment limits to cumulative (add existing)
            process_invest_period(source_db, target_db)

            # lifetime to duration
            lifetime_to_duration(source_db, target_db, settings["lifetime_to_duration"])

            # unit flow transformation
            unit_flow_variants(source_db, target_db, settings)

            # efficiency to unit_flow__unit_flow ratios and operating_points
            process_efficiency(source_db, target_db)

            # conversion coefficients to unit_flow__unit_flow ratios
            process_conversion_coefficients(source_db, target_db)

            # user constraints from INES constraint coefficients
            process_constraints(source_db, target_db)

            # investment_uses_integer to investment_variable_type
            process_investment_integer(source_db, target_db)

            # system discount rate
            process_system_discount_rate(source_db, target_db)

            # reserves
            process_reserves(source_db, target_db)

            # stochastic structure
            process_stochastic_structure(source_db, target_db)

            # forecast parameters → scenario-indexed Maps
            process_forecasts(source_db, target_db)

            # node penalty defaults
            process_node_penalty(source_db, target_db, settings["node_penalty_default"])

            # commodity price to node__to_unit vom_cost
            process_commodity_price(source_db, target_db)

            # bidirectional link capacity and efficiency
            process_link_bidirectional(source_db, target_db)


def process_forecasts(source_db, target_db):
    """Transform INES _forecasts parameters to SpineOpt scenario-indexed Maps.

    INES _forecasts parameters are Maps where 1st index = forecast/scenario name.
    SpineOpt expects scenario-dependent data as Maps indexed by stochastic_scenario names.
    """

    # Mapping: (source_class, forecast_param) → (target_class, target_param, target_order, multiplier)
    forecast_mappings = [
        # unit__to_node forecasts
        ("unit__to_node", "other_operational_cost_forecasts", "unit__to_node", "vom_cost", [[1], [2]], 1.0),
        # node__to_unit forecasts
        ("node__to_unit", "other_operational_cost_forecasts", "node__to_unit", "vom_cost", [[1], [2]], 1.0),
        # node forecasts
        ("node", "storage_state_fix_forecasts", "node", "storage_state_fix", [[1]], 1.0),
        ("node", "storage_state_lower_limit_forecasts", "node", "storage_state_min_fraction", [[1]], 1.0),
        ("node", "storage_state_upper_limit_forecasts", "node", "storage_state_max_fraction", [[1]], 1.0),
        # node__link__node forecasts
        ("node__link__node", "operational_cost_forecasts", "connection__from_node", "connection_flow_cost", [[2], [1]], 1.0),
        ("node__link__node", "efficiency_forecasts", "connection__node__node", "fix_ratio_out_in_connection_flow", [[2], [3], [1]], 1.0),
    ]

    for src_class, src_param, tgt_class, tgt_param, tgt_order, multiplier in forecast_mappings:
        # Derive the base parameter name (without _forecasts suffix)
        base_param = src_param.replace("_forecasts", "")

        for pv in source_db.get_parameter_value_items(
            entity_class_name=src_class, parameter_definition_name=src_param
        ):
            if pv["type"] != "map":
                continue

            alt = pv["alternative_name"]
            parsed = pv["parsed_value"]

            # Build target entity byname from source byname with reordering
            target_names = tuple(
                "__".join(pv["entity_byname"][int(i) - 1] for i in k)
                for k in tgt_order
            )

            # Look up the realization (base) value for the same entity
            indexes = list(parsed.indexes)
            values = [
                multiplier * v if isinstance(v, (int, float)) else v
                for v in parsed.values
            ]
            base_items = source_db.get_parameter_value_items(
                entity_class_name=src_class,
                parameter_definition_name=base_param,
                entity_byname=pv["entity_byname"],
            )
            if base_items:
                base_val = base_items[0]["parsed_value"]
                if isinstance(base_val, (int, float)):
                    base_val = multiplier * base_val
                indexes = ["realization"] + indexes
                values = [base_val] + values

            # Build scenario-indexed Map from the forecast data
            scenario_map = Map(
                indexes=indexes,
                values=values,
                index_name="stochastic_scenario",
            )

            try:
                add_parameter_value(
                    target_db, tgt_class, tgt_param,
                    alt, target_names, scenario_map,
                )
            except RuntimeError:
                db_val, val_type = api.to_database(scenario_map)
                target_db.update_parameter_value_item(
                    entity_class_name=tgt_class,
                    entity_byname=target_names,
                    parameter_definition_name=tgt_param,
                    alternative_name=alt,
                    value=db_val,
                    type=val_type,
                )

    try:
        target_db.commit_session("Added forecast parameters")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit forecast parameters error:", e)


def process_stochastic_structure(source_db, target_db):
    """Map INES set stochastic parameters to SpineOpt stochastic_structure entities.

    Uses the single 'stochastic' stochastic_structure and 'realization' scenario
    already created in timeline_setup. Forecast scenarios are added to that structure.
    """

    sto_structure = "stochastic"
    realization_name = "realization"

    for pv_method in source_db.get_parameter_value_items(
        entity_class_name="set", parameter_definition_name="stochastic_method"
    ):
        method = pv_method["parsed_value"]
        if method == "none":
            continue

        set_name = pv_method["entity_byname"][0]
        alt = pv_method["alternative_name"]

        # Get forecast weights: map of {forecast_name: weight}
        forecast_weights = {}
        for pv_w in source_db.get_parameter_value_items(
            entity_class_name="set", parameter_definition_name="stochastic_forecast_weights"
        ):
            if pv_w["entity_byname"][0] == set_name:
                parsed = pv_w["parsed_value"]
                if hasattr(parsed, "indexes"):
                    for idx, val in zip(parsed.indexes, parsed.values):
                        forecast_weights[str(idx)] = float(val)

        # Create forecast scenario entities + relationships + weights + parent-child
        for forecast_name, weight in forecast_weights.items():
            try:
                add_entity(target_db, "stochastic_scenario", (forecast_name,))
            except RuntimeError:
                pass
            try:
                add_entity(
                    target_db, "stochastic_structure__stochastic_scenario",
                    (sto_structure, forecast_name),
                )
            except RuntimeError:
                pass
            # Set weight_relative_to_parents
            try:
                add_parameter_value(
                    target_db, "stochastic_structure__stochastic_scenario",
                    "weight_relative_to_parents",
                    alt, (sto_structure, forecast_name), weight,
                )
            except RuntimeError:
                pass
            # Parent-child: realization → forecast
            try:
                add_entity(
                    target_db, "parent_stochastic_scenario__child_stochastic_scenario",
                    (realization_name, forecast_name),
                )
            except RuntimeError:
                pass

        # Link set member nodes → node__stochastic_structure
        for member in source_db.get_entity_items(entity_class_name="set__node"):
            if member["entity_byname"][0] == set_name:
                node_name = member["entity_byname"][1]
                try:
                    add_entity(
                        target_db, "node__stochastic_structure",
                        (node_name, sto_structure),
                    )
                except RuntimeError:
                    pass

        # Link set member units → units_on__stochastic_structure
        for member in source_db.get_entity_items(entity_class_name="set__unit"):
            if member["entity_byname"][0] == set_name:
                unit_name = member["entity_byname"][1]
                try:
                    add_entity(
                        target_db, "units_on__stochastic_structure",
                        (unit_name, sto_structure),
                    )
                except RuntimeError:
                    pass

    try:
        target_db.commit_session("Added stochastic structure")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit stochastic structure error:", e)


def process_input_capacity(source_db, target_db):
    """Convert node__to_unit.capacity to unit__to_node.capacity_per_unit × max efficiency."""
    for pv in source_db.get_parameter_value_items(
        entity_class_name="node__to_unit", parameter_definition_name="capacity"
    ):
        node_name = pv["entity_byname"][0]
        unit_name = pv["entity_byname"][1]
        alt = pv["alternative_name"]
        capacity_val = pv["parsed_value"]

        # Get max efficiency of the unit
        eff_items = source_db.get_parameter_value_items(
            entity_class_name="unit",
            entity_byname=(unit_name,),
            parameter_definition_name="efficiency",
        )
        max_eff = 1.0
        if eff_items:
            eff_val = eff_items[0]["parsed_value"]
            if isinstance(eff_val, (int, float)):
                max_eff = eff_val
            elif isinstance(eff_val, list):
                max_eff = max(eff_val)

        output_capacity = capacity_val * max_eff

        # Find the output nodes for this unit
        unit_outputs = [
            f["entity_byname"][1]
            for f in source_db.get_entity_items(entity_class_name="unit__to_node")
            if f["entity_byname"][0] == unit_name
        ]
        for out_node in unit_outputs:
            try:
                add_parameter_value(
                    target_db, "unit__to_node", "capacity_per_unit",
                    alt, (unit_name, out_node), output_capacity,
                )
            except RuntimeError:
                pass

    try:
        target_db.commit_session("Added input capacity to output side")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit input capacity error:", e)


def process_reserves(source_db, target_db):
    """Map INES reserve entities and parameters to SpineOpt node-based reserves."""

    # Build mapping: INES reserve name → list of (spineopt_node_name, direction)
    # For upward/downward: single node with original name
    # For symmetric: two nodes {name}_up and {name}_down
    reserve_nodes = {}  # {ines_reserve_name: [(spineopt_node, direction), ...]}

    for pv in source_db.get_parameter_value_items(
        entity_class_name="reserve", parameter_definition_name="reserve_type"
    ):
        reserve_name = pv["entity_byname"][0]
        alt = pv["alternative_name"]
        rtype = pv["parsed_value"]

        if rtype == "symmetric":
            up_name = reserve_name + "_up"
            down_name = reserve_name + "_down"
            reserve_nodes[reserve_name] = [(up_name, "upward"), (down_name, "downward")]
        elif rtype == "upward":
            reserve_nodes[reserve_name] = [(reserve_name, "upward")]
        elif rtype == "downward":
            reserve_nodes[reserve_name] = [(reserve_name, "downward")]

    # 1. Create nodes and set reserve flags
    for reserve_name, node_list in reserve_nodes.items():
        # Get alternative from reserve_type parameter
        pv = source_db.get_parameter_value_items(
            entity_class_name="reserve", parameter_definition_name="reserve_type",
            entity_byname=(reserve_name,),
        )[0]
        alt = pv["alternative_name"]

        for node_name, direction in node_list:
            try:
                add_entity(target_db, "node", (node_name,))
            except RuntimeError:
                pass
            add_parameter_value(target_db, "node", "reserve_active", alt, (node_name,), True)
            add_parameter_value(target_db, "node", "balance_sense", alt, (node_name,), ">=")
            add_parameter_value(target_db, "node", "balance_type", alt, (node_name,), "node_balance")
            if direction == "upward":
                add_parameter_value(target_db, "node", "reserve_upward", alt, (node_name,), True)
            elif direction == "downward":
                add_parameter_value(target_db, "node", "reserve_downward", alt, (node_name,), True)

    # 2. node__reserve.reserve_requirement → node.demand on reserve node(s)
    for pv in source_db.get_parameter_value_items(
        entity_class_name="node__reserve", parameter_definition_name="reserve_requirement"
    ):
        reserve_name = pv["entity_byname"][1]
        alt = pv["alternative_name"]
        if reserve_name in reserve_nodes:
            for node_name, _ in reserve_nodes[reserve_name]:
                add_parameter_value(
                    target_db, "node", "demand", alt, (node_name,), pv["parsed_value"]
                )

    # 3. unit__node__reserve → unit__to_node entities, node groups, and capacity
    for ent in source_db.get_entity_items(entity_class_name="unit__node__reserve"):
        unit_name = ent["entity_byname"][0]
        energy_node = ent["entity_byname"][1]
        reserve_name = ent["entity_byname"][2]
        if reserve_name not in reserve_nodes:
            continue

        # Look up unit capacity on the energy node
        capacity_val = None
        from_input = False
        for cap_class in ["unit__to_node", "node__to_unit"]:
            for cap in source_db.get_parameter_value_items(
                entity_class_name=cap_class, parameter_definition_name="capacity"
            ):
                cap_byname = cap["entity_byname"]
                if cap_class == "unit__to_node" and cap_byname == (unit_name, energy_node):
                    capacity_val = cap["parsed_value"]
                    break
                elif cap_class == "node__to_unit" and cap_byname == (energy_node, unit_name):
                    capacity_val = cap["parsed_value"]
                    from_input = True
                    break
            if capacity_val is not None:
                break
        # If not found on specific node, sum all output capacities of the unit
        if capacity_val is None:
            total_cap = 0
            for cap in source_db.get_parameter_value_items(
                entity_class_name="unit__to_node", parameter_definition_name="capacity"
            ):
                if cap["entity_byname"][0] == unit_name:
                    total_cap += cap["parsed_value"]
            if total_cap > 0:
                capacity_val = total_cap
        # If capacity came from node__to_unit (input side), multiply by max efficiency
        if from_input and capacity_val is not None:
            eff_items = source_db.get_parameter_value_items(
                entity_class_name="unit",
                entity_byname=(unit_name,),
                parameter_definition_name="efficiency",
            )
            if eff_items:
                eff_val = eff_items[0]["parsed_value"]
                if isinstance(eff_val, (int, float)):
                    capacity_val = capacity_val * eff_val
                elif isinstance(eff_val, list):
                    capacity_val = capacity_val * max(eff_val)

        # Look up max_reserve_provision (default 1.0)
        mrp_items = source_db.get_parameter_value_items(
            entity_class_name="unit__node__reserve",
            parameter_definition_name="max_reserve_provision",
            entity_byname=(unit_name, energy_node, reserve_name),
        )
        share = mrp_items[0]["parsed_value"] if mrp_items else 1.0

        # Get alternative
        alt_items = source_db.get_parameter_value_items(
            entity_class_name="reserve", parameter_definition_name="reserve_type",
            entity_byname=(reserve_name,),
        )
        alt = alt_items[0]["alternative_name"] if alt_items else "Base"

        for res_node, _ in reserve_nodes[reserve_name]:
            group_name = unit_name + "_" + res_node + "_group"

            # Create group node entity
            try:
                add_entity(target_db, "node", (group_name,))
            except RuntimeError:
                pass
            # Group node has no balance_type
            add_parameter_value(target_db, "node", "balance_type", alt, (group_name,), "none")

            # Add group members: reserve node and all output nodes of this unit
            try:
                add_entity_group(target_db, "node", group_name, res_node)
            except RuntimeError:
                pass
            unit_outputs = [
                f["entity_byname"][1]
                for f in source_db.get_entity_items(entity_class_name="unit__to_node")
                if f["entity_byname"][0] == unit_name
            ]
            for out_node in unit_outputs:
                try:
                    add_entity_group(target_db, "node", group_name, out_node)
                except RuntimeError:
                    pass

            # Create unit__to_node to reserve node
            try:
                add_entity(target_db, "unit__to_node", (unit_name, res_node))
            except RuntimeError:
                pass

            # Create unit__to_node to group node
            try:
                add_entity(target_db, "unit__to_node", (unit_name, group_name))
            except RuntimeError:
                pass

            # capacity_per_unit on reserve node = capacity × max_reserve_provision
            if capacity_val is not None:
                add_parameter_value(
                    target_db, "unit__to_node", "capacity_per_unit",
                    alt, (unit_name, res_node), capacity_val * share,
                )

            # capacity_per_unit on group node = capacity
            if capacity_val is not None:
                add_parameter_value(
                    target_db, "unit__to_node", "capacity_per_unit",
                    alt, (unit_name, group_name), capacity_val,
                )

    # unit__node__reserve.reservation_cost → unit__to_node.reserve_procurement_cost
    for pv in source_db.get_parameter_value_items(
        entity_class_name="unit__node__reserve", parameter_definition_name="reservation_cost"
    ):
        unit_name = pv["entity_byname"][0]
        reserve_name = pv["entity_byname"][2]
        alt = pv["alternative_name"]
        if reserve_name in reserve_nodes:
            for node_name, _ in reserve_nodes[reserve_name]:
                add_parameter_value(
                    target_db, "unit__to_node", "reserve_procurement_cost",
                    alt, (unit_name, node_name), pv["parsed_value"],
                )

    # 4. link__node__reserve → connection__to_node(link, reserve_node) relationships and parameters
    for ent in source_db.get_entity_items(entity_class_name="link__node__reserve"):
        link_name = ent["entity_byname"][0]
        reserve_name = ent["entity_byname"][2]
        if reserve_name in reserve_nodes:
            for node_name, _ in reserve_nodes[reserve_name]:
                try:
                    add_entity(target_db, "connection__to_node", (link_name, node_name))
                except RuntimeError:
                    pass

    # link__node__reserve.reservation_cost → connection__to_node.reserve_procurement_cost
    for pv in source_db.get_parameter_value_items(
        entity_class_name="link__node__reserve", parameter_definition_name="reservation_cost"
    ):
        link_name = pv["entity_byname"][0]
        reserve_name = pv["entity_byname"][2]
        alt = pv["alternative_name"]
        if reserve_name in reserve_nodes:
            for node_name, _ in reserve_nodes[reserve_name]:
                add_parameter_value(
                    target_db, "connection__to_node", "reserve_procurement_cost",
                    alt, (link_name, node_name), pv["parsed_value"],
                )

    try:
        target_db.commit_session("Added reserves")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit reserves error:", e)


def process_investment_integer(source_db, target_db):
    """Set investment_variable_type for entities with investment parameters.

    Sets 'linear' by default for any entity that has an investment_method or investment_cost.
    Overrides to 'integer' if investment_uses_integer is True.
    """
    # First pass: set 'linear' for all entities with investment parameters
    investment_indicators = [
        ("unit", ["investment_method", "investment_cost"], "unit", "investment_variable_type"),
        ("link", ["investment_method", "investment_cost"], "connection", "investment_variable_type"),
        ("node", ["storage_investment_method", "storage_investment_cost"], "node", "storage_investment_variable_type"),
    ]
    # Also check flow-level investment_cost to identify units with investments
    flow_investment_indicators = [
        ("unit__to_node", "investment_cost", "unit", "investment_variable_type", 0),
        ("node__to_unit", "investment_cost", "unit", "investment_variable_type", 1),
    ]
    entities_with_investments = set()  # (target_class, entity_byname)
    for source_class, indicator_params, target_class, target_param in investment_indicators:
        for ind_param in indicator_params:
            for pv in source_db.get_parameter_value_items(
                entity_class_name=source_class,
                parameter_definition_name=ind_param,
            ):
                key = (target_class, pv["entity_byname"], target_param)
                if key not in entities_with_investments:
                    entities_with_investments.add(key)
                    alt = pv["alternative_name"]
                    try:
                        add_parameter_value(
                            target_db, target_class, target_param,
                            alt, pv["entity_byname"], "linear",
                        )
                    except RuntimeError:
                        pass  # already set
    for source_class, ind_param, target_class, target_param, unit_idx in flow_investment_indicators:
        for pv in source_db.get_parameter_value_items(
            entity_class_name=source_class,
            parameter_definition_name=ind_param,
        ):
            unit_byname = (pv["entity_byname"][unit_idx],)
            key = (target_class, unit_byname, target_param)
            if key not in entities_with_investments:
                entities_with_investments.add(key)
                alt = pv["alternative_name"]
                try:
                    add_parameter_value(
                        target_db, target_class, target_param,
                        alt, unit_byname, "linear",
                    )
                except RuntimeError:
                    pass

    # Second pass: override to 'integer' where investment_uses_integer is True
    integer_mappings = [
        ("unit", "investment_uses_integer", "unit", "investment_variable_type"),
        ("link", "investment_uses_integer", "connection", "investment_variable_type"),
        ("node", "storage_investment_uses_integer", "node", "storage_investment_variable_type"),
    ]
    for source_class, source_param, target_class, target_param in integer_mappings:
        for pv in source_db.get_parameter_value_items(
            entity_class_name=source_class,
            parameter_definition_name=source_param,
        ):
            if pv["parsed_value"]:
                db_val, val_type = api.to_database("integer")
                target_db.update_parameter_value_item(
                    entity_class_name=target_class,
                    entity_byname=pv["entity_byname"],
                    parameter_definition_name=target_param,
                    alternative_name=pv["alternative_name"],
                    value=db_val,
                    type=val_type,
                )
    try:
        target_db.commit_session("Added integer investment variable types")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit integer investment variable types error:", e)


def process_availability(source_db, target_db):
    """Combine INES unit.availability and profile_limit_upper into SpineOpt unit.availability_factor.

    If both unit.availability and profile_limit_upper exist for the same unit,
    the result is their product. profile_limit_upper can come from unit__to_node or node__to_unit.
    """
    periods_info = _get_periods_info(source_db)

    # Collect availability per (unit_name, alternative)
    availability = {}  # {(unit_name, alt): value}

    # 1. Unit-level availability
    for pv in source_db.get_parameter_value_items(
        entity_class_name="unit", parameter_definition_name="availability"
    ):
        unit_name = pv["entity_byname"][0]
        alt = pv["alternative_name"]
        if pv["type"] == "float":
            availability[(unit_name, alt)] = pv["parsed_value"]
        elif pv["type"] == "time_series":
            availability[(unit_name, alt)] = pv["parsed_value"]
        elif pv["type"] == "map":
            ts = _map_to_time_series(pv["parsed_value"], periods_info)
            if ts:
                availability[(unit_name, alt)] = ts

    # 2. profile_limit_upper from unit__to_node and node__to_unit
    profile = {}  # {(unit_name, alt): value}
    for flow_class in ["unit__to_node", "node__to_unit"]:
        for pv in source_db.get_parameter_value_items(
            entity_class_name=flow_class,
            parameter_definition_name="profile_limit_upper",
        ):
            if flow_class == "unit__to_node":
                unit_name = pv["entity_byname"][0]
            else:
                unit_name = pv["entity_byname"][1]
            alt = pv["alternative_name"]
            if (unit_name, alt) in profile:
                continue  # first flow wins
            if pv["type"] == "float":
                profile[(unit_name, alt)] = pv["parsed_value"]
            elif pv["type"] == "time_series":
                profile[(unit_name, alt)] = pv["parsed_value"]
            elif pv["type"] == "map":
                ts = _map_to_time_series(pv["parsed_value"], periods_info)
                if ts:
                    profile[(unit_name, alt)] = ts

    # 3. Combine: multiply if both present
    all_keys = set(list(availability.keys()) + list(profile.keys()))
    for (unit_name, alt) in all_keys:
        avail = availability.get((unit_name, alt))
        prof = profile.get((unit_name, alt))

        if avail is not None and prof is not None:
            value = _multiply_values(avail, prof)
        elif avail is not None:
            value = avail
        else:
            value = prof

        if value is not None:
            try:
                add_parameter_value(
                    target_db, "unit", "availability_factor",
                    alt, (unit_name,), value,
                )
            except RuntimeError:
                db_val, val_type = api.to_database(value)
                target_db.update_parameter_value_item(
                    entity_class_name="unit",
                    entity_byname=(unit_name,),
                    parameter_definition_name="availability_factor",
                    alternative_name=alt,
                    value=db_val,
                    type=val_type,
                )

    try:
        target_db.commit_session("Added availability")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit availability error:", e)


def _multiply_values(a, b):
    """Multiply two parameter values that may be floats, time_series dicts, or TimeSeriesVariableResolution objects."""
    from spinedb_api.parameter_value import TimeSeriesVariableResolution

    def _is_ts(v):
        return isinstance(v, TimeSeriesVariableResolution) or (isinstance(v, dict) and v.get("type") == "time_series")

    def _to_dict(v):
        """Normalize to {timestamp_str: float} dict."""
        if isinstance(v, TimeSeriesVariableResolution):
            return {str(idx): val for idx, val in zip(v.indexes, v.values)}
        if isinstance(v, dict):
            return v["data"]
        return None

    a_is_ts = _is_ts(a)
    b_is_ts = _is_ts(b)

    if not a_is_ts and not b_is_ts:
        return a * b

    if a_is_ts and not b_is_ts:
        a_data = _to_dict(a)
        return {
            "type": "time_series",
            "data": {k: v * b for k, v in a_data.items()},
        }

    if not a_is_ts and b_is_ts:
        b_data = _to_dict(b)
        return {
            "type": "time_series",
            "data": {k: v * a for k, v in b_data.items()},
        }

    # Both are time_series - multiply matching timestamps
    a_data = _to_dict(a)
    b_data = _to_dict(b)
    merged_data = {}
    all_keys = list(dict.fromkeys(list(a_data.keys()) + list(b_data.keys())))
    last_a = None
    last_b = None
    for k in all_keys:
        val_a = a_data.get(k, last_a)
        val_b = b_data.get(k, last_b)
        if val_a is not None and val_b is not None:
            merged_data[k] = val_a * val_b
        if k in a_data:
            last_a = a_data[k]
        if k in b_data:
            last_b = b_data[k]
    return {"type": "time_series", "data": merged_data}


def process_invest_period(source_db, target_db):
    """Transform investment limit params to SpineOpt cumulative params.

    Handles both cumulative params (direct copy) and per-period params (add existing capacity).
    """
    periods_info = _get_periods_info(source_db)

    mappings = [
        {
            "source_class": "unit",
            "target_class": "unit",
            "existing_param": "units_existing",
            "cumulative": [
                ("units_fix_cumulative", "investment_count_fix_cumulative"),
                ("units_max_cumulative", "investment_count_max_cumulative"),
            ],
            "period": [
                ("units_invest_fix_period", "investment_count_fix_cumulative"),
                ("units_invest_max_period", "investment_count_max_cumulative"),
            ],
        },
        {
            "source_class": "link",
            "target_class": "connection",
            "existing_param": "links_existing",
            "cumulative": [
                ("links_fix_cumulative", "investment_count_fix_cumulative"),
                ("links_max_cumulative", "investment_count_max_cumulative"),
            ],
            "period": [
                ("links_invest_fix_period", "investment_count_fix_cumulative"),
                ("links_invest_max_period", "investment_count_max_cumulative"),
            ],
        },
        {
            "source_class": "node",
            "target_class": "node",
            "existing_param": "storages_existing",
            "cumulative": [
                ("storages_fix_cumulative", "storage_investment_count_fix_cumulative"),
                ("storages_max_cumulative", "storage_investment_count_max_cumulative"),
            ],
            "period": [
                ("storages_invest_fix_period", "storage_investment_count_fix_cumulative"),
                ("storages_invest_max_period", "storage_investment_count_max_cumulative"),
            ],
        },
    ]

    for mapping in mappings:
        source_class = mapping["source_class"]
        target_class = mapping["target_class"]
        existing_param = mapping["existing_param"]

        # Cumulative params: direct copy (already include existing)
        for source_param, target_param in mapping["cumulative"]:
            for pv in source_db.get_parameter_value_items(
                entity_class_name=source_class,
                parameter_definition_name=source_param,
            ):
                entity_byname = pv["entity_byname"]
                alt = pv["alternative_name"]

                if pv["type"] == "map":
                    ts = _map_to_time_series(pv["parsed_value"], periods_info)
                    if ts:
                        value_to_set = ts
                    else:
                        continue
                elif pv["type"] == "float":
                    value_to_set = pv["parsed_value"]
                else:
                    continue

                try:
                    add_parameter_value(
                        target_db, target_class, target_param, alt,
                        entity_byname, value_to_set,
                    )
                except RuntimeError:
                    db_val, val_type = api.to_database(value_to_set)
                    target_db.update_parameter_value_item(
                        entity_class_name=target_class,
                        entity_byname=entity_byname,
                        parameter_definition_name=target_param,
                        alternative_name=alt,
                        value=db_val,
                        type=val_type,
                    )

        # Period params: add existing capacity to convert to cumulative
        for source_param, target_param in mapping["period"]:
            for pv in source_db.get_parameter_value_items(
                entity_class_name=source_class,
                parameter_definition_name=source_param,
            ):
                entity_byname = pv["entity_byname"]
                alt = pv["alternative_name"]

                # Get initial existing capacity
                existing_items = source_db.get_parameter_value_items(
                    entity_class_name=source_class,
                    entity_byname=entity_byname,
                    parameter_definition_name=existing_param,
                )
                existing_item = existing_items[0] if existing_items else None
                existing_count = 0.0
                if existing_item:
                    if existing_item["type"] == "float":
                        existing_count = existing_item["parsed_value"]
                    elif existing_item["type"] == "map":
                        existing_count = float(existing_item["parsed_value"].values[0])

                if pv["type"] == "map":
                    ts = _map_to_time_series(pv["parsed_value"], periods_info)
                    if ts:
                        ts["data"] = {
                            k: v + existing_count for k, v in ts["data"].items()
                        }
                        value_to_set = ts
                    else:
                        continue
                elif pv["type"] == "float":
                    value_to_set = existing_count + pv["parsed_value"]
                else:
                    continue

                try:
                    add_parameter_value(
                        target_db, target_class, target_param, alt,
                        entity_byname, value_to_set,
                    )
                except RuntimeError:
                    db_val, val_type = api.to_database(value_to_set)
                    target_db.update_parameter_value_item(
                        entity_class_name=target_class,
                        entity_byname=entity_byname,
                        parameter_definition_name=target_param,
                        alternative_name=alt,
                        value=db_val,
                        type=val_type,
                    )

    try:
        target_db.commit_session("Added investment period limits")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit investment period limits error:", e)


def _get_periods_info(source_db):
    """Get period start times and years represented from the source database.
    Aggregates periods across all solve_pattern entities."""
    all_periods = []
    for pv in source_db.get_parameter_value_items(
        entity_class_name="solve_pattern", parameter_definition_name="period"
    ):
        periods_value = json.loads(pv["value"])["data"]
        for p in _ensure_list(periods_value):
            if p not in all_periods:
                all_periods.append(p)
    starttime = {}
    year_repr = {}
    for period in all_periods:
        starttime[period] = json.loads(
            source_db.get_parameter_value_items(
                entity_class_name="period",
                entity_byname=(period,),
                parameter_definition_name="start_time",
            )[0]["value"]
        )["data"]
        year_repr[period] = source_db.get_parameter_value_items(
            entity_class_name="period",
            entity_byname=(period,),
            parameter_definition_name="years_represented",
        )[0]["parsed_value"]
    return {"periods": all_periods, "starttime": starttime, "year_repr": year_repr}


def _map_to_time_series(parsed_value, periods_info):
    """Convert a period-indexed map parameter to a time series dict. Returns None if no period matches."""
    starttime = periods_info["starttime"]
    year_repr = periods_info["year_repr"]
    map_table = convert_map_to_table(parsed_value)
    index_names = nested_index_names(parsed_value)
    data = pd.DataFrame(map_table, columns=index_names + ["value"]).set_index(index_names[0])
    data.index = data.index.astype("string")
    if not any(i in data.index for i in starttime):
        return None
    indexes_ = []
    values_ = []
    for period_, ts_index_ in starttime.items():
        values_.append(float(data.at[period_, "value"]) if period_ in data.index else 0.0)
        indexes_.append(ts_index_)
    last_period = list(starttime.keys())[-1]
    values_.append(values_[-1])
    indexes_.append(
        pd.Timestamp(starttime[last_period]).replace(
            year=int(pd.Timestamp(starttime[last_period]).year + year_repr[last_period])
        ).isoformat()
    )
    return {"type": "time_series", "data": dict(zip(indexes_, values_))}


def _get_emission_rates_for_type(source_db, target_db, config):
    """Get per-unit emission rates for a given emission type.

    For CO2: uses co2_content on fuel nodes to determine emission rates per input flow.
    For SO2/NOx: uses so2_emission_rate/nox_emission_rate on individual flows.

    Returns: dict {unit_name: [(flow_class, node_name, rate, alternative_name), ...]}
    """
    rates = {}

    if config["content_param"]:
        # CO2-style: emission rate is defined on fuel nodes
        content_params = source_db.get_parameter_value_items(
            entity_class_name="node",
            parameter_definition_name=config["content_param"],
        )
        content_values = {}
        for cp in content_params:
            node_name = cp["entity_name"]
            if node_name.upper() == config["type"].upper():
                continue
            content_values[node_name] = (cp["parsed_value"], cp["alternative_name"])

        for unit_entity in target_db.get_entity_items(entity_class_name="unit"):
            unit_name = unit_entity["name"]
            for node_name, (rate, alt) in content_values.items():
                if target_db.get_entity_item(
                    entity_class_name="node__to_unit",
                    entity_byname=(node_name, unit_name),
                ):
                    if unit_name not in rates:
                        rates[unit_name] = []
                    rates[unit_name].append(("node__to_unit", node_name, rate, alt))

    if config["rate_param"]:
        # SO2/NOx-style: emission rate is defined per flow
        for flow_class in ["node__to_unit", "unit__to_node"]:
            for param in source_db.get_parameter_value_items(
                entity_class_name=flow_class,
                parameter_definition_name=config["rate_param"],
            ):
                if flow_class == "node__to_unit":
                    node_name, unit_name = param["entity_byname"]
                else:
                    unit_name, node_name = param["entity_byname"]
                if unit_name not in rates:
                    rates[unit_name] = []
                rates[unit_name].append(
                    (flow_class, node_name, param["parsed_value"], param["alternative_name"])
                )

    return rates


def _create_emission_flows(target_db, emission_node, emission_rates):
    """Create emission flow relationships (unit__to_node + unit_flow__unit_flow or user_constraint).

    For single-flow units: uses unit_flow__unit_flow with flow_ratio_equality_coefficient.
    For multi-flow units: uses user_constraint with coefficients.
    """
    for unit_name, flows in emission_rates.items():
        try:
            add_entity(target_db, "unit__to_node", (unit_name, emission_node))
        except RuntimeError:
            pass

        if len(flows) == 1:
            flow_class, node_name, rate, alt = flows[0]
            if flow_class == "node__to_unit":
                uf_byname = (unit_name, emission_node, node_name, unit_name)
            else:
                uf_byname = (unit_name, emission_node, unit_name, node_name)
            try:
                add_entity(target_db, "unit_flow__unit_flow", uf_byname)
            except RuntimeError:
                pass
            add_parameter_value(
                target_db, "unit_flow__unit_flow", "flow_ratio_equality_coefficient",
                alt, uf_byname, rate,
            )
        else:
            constraint_name = unit_name + "_" + emission_node + "_emissions"
            try:
                add_entity(target_db, "user_constraint", (constraint_name,))
            except RuntimeError:
                pass
            try:
                add_entity(
                    target_db, "unit_flow__user_constraint",
                    (unit_name, emission_node, constraint_name),
                )
            except RuntimeError:
                pass
            add_parameter_value(
                target_db, "unit_flow__user_constraint", "coefficient_for_unit_flow",
                flows[0][3], (unit_name, emission_node, constraint_name), -1.0,
            )
            for flow_class, node_name, rate, alt in flows:
                if flow_class == "node__to_unit":
                    uf_uc_byname = (node_name, unit_name, constraint_name)
                else:
                    uf_uc_byname = (unit_name, node_name, constraint_name)
                try:
                    add_entity(target_db, "unit_flow__user_constraint", uf_uc_byname)
                except RuntimeError:
                    pass
                add_parameter_value(
                    target_db, "unit_flow__user_constraint",
                    "coefficient_for_unit_flow", alt, uf_uc_byname, rate,
                )


def _process_cumulative_emission_limits(target_db, emission_node, cumul_limits, periods_info):
    """Process cumulative emission limits (*_max_cumulative) from sets."""
    for param_map in cumul_limits:
        add_parameter_value(
            target_db, "node", "storage_active",
            param_map["alternative_name"], (emission_node,), True,
        )
        if param_map["type"] == "map":
            ts = _map_to_time_series(param_map["parsed_value"], periods_info)
            if ts:
                max_val = max(ts["data"].values())
                if max_val > 0:
                    fraction_data = {k: v / max_val for k, v in ts["data"].items()}
                    add_parameter_value(
                        target_db, "node", "storage_state_max_fraction",
                        param_map["alternative_name"], (emission_node,),
                        {"type": "time_series", "data": fraction_data},
                    )
                    add_parameter_value(
                        target_db, "node", "storage_state_max",
                        param_map["alternative_name"], (emission_node,), max_val,
                    )
        elif param_map["type"] == "float":
            add_parameter_value(
                target_db, "node", "storage_state_max",
                param_map["alternative_name"], (emission_node,), param_map["parsed_value"],
            )


def _process_period_emission_limits(target_db, config, param_map, emission_rates, periods_info):
    """Handle per-period emission limits using separate cap nodes per period.

    Creates one cap node per period with storage_active=True and storage_state_max = period limit.
    Links emitting units to cap nodes with time-varying flow ratios (active only during each period).
    """
    emission_type = config["type"]
    if param_map["type"] != "map":
        return

    starttime = periods_info["starttime"]
    year_repr = periods_info["year_repr"]
    map_table = convert_map_to_table(param_map["parsed_value"])
    index_names = nested_index_names(param_map["parsed_value"])
    data = pd.DataFrame(map_table, columns=index_names + ["value"]).set_index(index_names[0])
    data.index = data.index.astype("string")

    for period_name in data.index:
        if period_name not in starttime:
            continue

        cap_node = f"{emission_type}_cap_{period_name}"
        limit = float(data.at[period_name, "value"])

        add_entity(target_db, "node", (cap_node,))
        add_parameter_value(
            target_db, "node", "storage_active",
            param_map["alternative_name"], (cap_node,), True,
        )
        add_parameter_value(
            target_db, "node", "storage_state_max",
            param_map["alternative_name"], (cap_node,), limit,
        )

        # Build time series for ratio: emission_rate during this period, 0 otherwise
        def _make_period_ratio(rate):
            ts_indexes = []
            ts_values = []
            for p_name, p_start in starttime.items():
                ts_values.append(rate if p_name == period_name else 0.0)
                ts_indexes.append(p_start)
            last_p = list(starttime.keys())[-1]
            ts_values.append(ts_values[-1])
            ts_indexes.append(
                pd.Timestamp(starttime[last_p]).replace(
                    year=int(pd.Timestamp(starttime[last_p]).year + year_repr[last_p])
                ).isoformat()
            )
            return {"type": "time_series", "data": dict(zip(ts_indexes, ts_values))}

        for unit_name, flows in emission_rates.items():
            try:
                add_entity(target_db, "unit__to_node", (unit_name, cap_node))
            except RuntimeError:
                pass

            if len(flows) == 1:
                flow_class, node_name, rate, alt = flows[0]
                if not isinstance(rate, (int, float)):
                    continue
                ts_ratio = _make_period_ratio(rate)
                if flow_class == "node__to_unit":
                    uf_byname = (unit_name, cap_node, node_name, unit_name)
                else:
                    uf_byname = (unit_name, cap_node, unit_name, node_name)
                try:
                    add_entity(target_db, "unit_flow__unit_flow", uf_byname)
                except RuntimeError:
                    pass
                add_parameter_value(
                    target_db, "unit_flow__unit_flow", "flow_ratio_equality_coefficient",
                    alt, uf_byname, ts_ratio,
                )
            else:
                # Multiple flows: use user_constraint with time-varying coefficients.
                # The emission flow coefficient is constant -1 (forces emission_flow=0 when all others are 0).
                constraint_name = f"{unit_name}_{cap_node}_emissions"
                try:
                    add_entity(target_db, "user_constraint", (constraint_name,))
                except RuntimeError:
                    pass
                try:
                    add_entity(
                        target_db, "unit_flow__user_constraint",
                        (unit_name, cap_node, constraint_name),
                    )
                except RuntimeError:
                    pass
                add_parameter_value(
                    target_db, "unit_flow__user_constraint", "coefficient_for_unit_flow",
                    flows[0][3], (unit_name, cap_node, constraint_name), -1.0,
                )
                for flow_class, node_name, rate, alt in flows:
                    if not isinstance(rate, (int, float)):
                        continue
                    ts_coeff = _make_period_ratio(rate)
                    if flow_class == "node__to_unit":
                        uf_uc_byname = (node_name, unit_name, constraint_name)
                    else:
                        uf_uc_byname = (unit_name, node_name, constraint_name)
                    try:
                        add_entity(target_db, "unit_flow__user_constraint", uf_uc_byname)
                    except RuntimeError:
                        pass
                    add_parameter_value(
                        target_db, "unit_flow__user_constraint",
                        "coefficient_for_unit_flow", alt, uf_uc_byname, ts_coeff,
                    )


def _process_emission_prices(target_db, emission_node, prices, periods_info):
    """Process emission prices (*_price) from sets as tax_in_unit_flow on the emission node."""
    for param_map in prices:
        if param_map["type"] == "map":
            ts = _map_to_time_series(param_map["parsed_value"], periods_info)
            if ts:
                add_parameter_value(
                    target_db, "node", "tax_in_unit_flow",
                    param_map["alternative_name"], (emission_node,), ts,
                )
        elif param_map["type"] == "float":
            add_parameter_value(
                target_db, "node", "tax_in_unit_flow",
                param_map["alternative_name"], (emission_node,), param_map["parsed_value"],
            )


def _handle_explicit_co2_outputs(target_db):
    """Handle units that explicitly output to nodes containing 'CO2' in their name.
    Links those flows to the atmosphere node."""
    for entity_items in [
        element
        for element in target_db.get_entity_items(entity_class_name="unit__to_node")
        if "CO2" in element["entity_byname"][1]
    ]:
        entity_byname = entity_items["entity_byname"]
        unit_name, node_out = entity_byname
        try:
            add_entity(target_db, "node__to_unit", ("atmosphere", unit_name))
        except RuntimeError:
            pass
        try:
            add_entity(target_db, "unit_flow__unit_flow", (unit_name, node_out, "atmosphere", unit_name))
        except RuntimeError:
            pass
        default_alt = target_db.get_alternative_items()[0]["name"]
        add_parameter_value(
            target_db,
            "unit_flow__unit_flow",
            "flow_ratio_equality_coefficient",
            default_alt,
            (unit_name, node_out, "atmosphere", unit_name),
            1.0,
        )


def process_emissions(source_db, target_db):
    """Process CO2, SO2, and NOx emissions - create emission nodes, emission flows, limits, and prices."""

    emission_configs = [
        {
            "type": "co2",
            "node_name": "atmosphere",
            "content_param": "co2_content",
            "rate_param": None,
            "max_cumulative": "co2_max_cumulative",
            "max_period": "co2_max_period",
            "price": "co2_price",
        },
        {
            "type": "so2",
            "node_name": "so2_emissions",
            "content_param": None,
            "rate_param": "so2_emission_rate",
            "max_cumulative": "so2_max_cumulative",
            "max_period": "so2_max_period",
            "price": "so2_price",
        },
        {
            "type": "nox",
            "node_name": "nox_emissions",
            "content_param": None,
            "rate_param": "nox_emission_rate",
            "max_cumulative": "nox_max_cumulative",
            "max_period": "nox_max_period",
            "price": "nox_price",
        },
    ]

    periods_info = _get_periods_info(source_db)

    for config in emission_configs:
        emission_node = config["node_name"]

        # Get emission rates for this type
        emission_rates = _get_emission_rates_for_type(source_db, target_db, config)

        # Get limits and prices from sets
        cumul_limits = source_db.get_parameter_value_items(
            entity_class_name="set", parameter_definition_name=config["max_cumulative"]
        )
        period_limits = source_db.get_parameter_value_items(
            entity_class_name="set", parameter_definition_name=config["max_period"]
        )
        prices = source_db.get_parameter_value_items(
            entity_class_name="set", parameter_definition_name=config["price"]
        )

        if not emission_rates and not cumul_limits and not period_limits and not prices:
            continue

        # Create emission node
        try:
            add_entity(target_db, "node", (emission_node,))
        except RuntimeError:
            pass

        # Cumulative limits
        _process_cumulative_emission_limits(target_db, emission_node, cumul_limits, periods_info)

        # Per-period limits
        for param_map in period_limits:
            _process_period_emission_limits(
                target_db, config, param_map, emission_rates, periods_info
            )

        # Create emission flows
        _create_emission_flows(target_db, emission_node, emission_rates)

        # Prices
        _process_emission_prices(target_db, emission_node, prices, periods_info)

    # Handle explicit CO2 output nodes (units with outputs to nodes containing "CO2")
    _handle_explicit_co2_outputs(target_db)

    try:
        target_db.commit_session("Added emissions")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit emissions error:", e)


def map_of_periods_or_historical_to_ts(source_db, target_db, settings):

    starttime = {}
    year_repr = {}
    all_periods = []
    for pv in source_db.get_parameter_value_items(
        entity_class_name="solve_pattern", parameter_definition_name="period"
    ):
        for period in _ensure_list(json.loads(pv["value"])["data"]):
            if period not in all_periods:
                all_periods.append(period)
    for period in all_periods:
        starttime[period] = json.loads(
            source_db.get_parameter_value_items(
                entity_class_name="period",
                entity_byname=(period,),
                parameter_definition_name="start_time",
            )[0]["value"]
        )["data"]
        year_repr[period] = source_db.get_parameter_value_items(
            entity_class_name="period",
            entity_byname=(period,),
            parameter_definition_name="years_represented",
        )[0]["parsed_value"]

    duration_value = json.loads(
        source_db.get_parameter_value_items(
            entity_class_name="solve_pattern", parameter_definition_name="duration"
        )[0]["value"]
    )["data"]
    duration = _ensure_list(duration_value)[0]
    starttime_sp_value = json.loads(
        source_db.get_parameter_value_items(
            entity_class_name="solve_pattern", parameter_definition_name="start_time"
        )[0]["value"]
    )["data"]
    starttime_sp = _ensure_list(starttime_sp_value)
    resolution = json.loads(
        source_db.get_parameter_value_items(
            entity_class_name="solve_pattern",
            parameter_definition_name="time_resolution",
        )[0]["value"]
    )["data"]

    for source_entity_class in settings:
        for target_entity_class in settings[source_entity_class]:
            for source_param in settings[source_entity_class][target_entity_class]:
                print(source_entity_class, target_entity_class, source_param)
                param_elements = settings[source_entity_class][target_entity_class][
                    source_param
                ]

                for param_map in source_db.get_parameter_value_items(
                    entity_class_name=source_entity_class,
                    parameter_definition_name=source_param,
                ):

                    target_param, target_order, multiplier = parameter_features(
                        param_elements,
                        source_db,
                        source_entity_class,
                        param_map["entity_byname"],
                        param_map["alternative_name"],
                    )

                    if param_map["type"] == "map":

                        map_table = convert_map_to_table(param_map["parsed_value"])
                        index_names = nested_index_names(param_map["parsed_value"])
                        data = pd.DataFrame(
                            map_table, columns=index_names + ["value"]
                        ).set_index(index_names[0])
                        data.index = data.index.astype("string")

                        if any(i in data.index for i in starttime):
                            indexes_ = []
                            values_ = []
                            for period_, ts_index_ in starttime.items():
                                values_.append(
                                    multiplier
                                    * (
                                        float(data.at[period_, "value"])
                                        if period_ in data.index
                                        else 0.0
                                    )
                                )

                                # this should be removed once the fixed resolution is repaired
                                indexes_.append(ts_index_)
                            values_.append(values_[-1])
                            indexes_.append(
                                (
                                    pd.Timestamp(ts_index_).replace(
                                        year=int(
                                            pd.Timestamp(ts_index_).year
                                            + year_repr[period_]
                                        )
                                    )
                                ).isoformat()
                            )

                            ts_to_export = {
                                "type": "time_series",
                                "data": dict(zip(indexes_, values_)),
                            }
                            target_names = tuple(
                                [
                                    "__".join(
                                        [
                                            param_map["entity_byname"][int(i) - 1]
                                            for i in k
                                        ]
                                    )
                                    for k in target_order
                                ]
                            )
                            add_parameter_value(
                                target_db,
                                target_entity_class,
                                target_param,
                                param_map["alternative_name"],
                                target_names,
                                ts_to_export,
                            )

                        if any(i in data.index for i in starttime_sp):
                            for index, element in enumerate(starttime_sp):
                                try:
                                    alternative_name = (
                                        f"wy{str(pd.Timestamp(element).year)}"
                                    )
                                    add_alternative(target_db, alternative_name)
                                except:
                                    pass
                                steps = pd.to_timedelta(duration) / pd.to_timedelta(
                                    resolution
                                )
                                df_data = (
                                    multiplier
                                    * data.iloc[
                                        data.index.tolist()
                                        .index(element) : data.index.tolist()
                                        .index(element)
                                        + int(steps),
                                        data.columns.tolist().index("value"),
                                    ]
                                ).tolist()
                                ts_export = {
                                    "type": "time_series",
                                    "data": df_data,
                                    "index": {
                                        "start": f"2018{element[4:]}",
                                        "resolution": resolution,
                                        "ignore_year": True,
                                    },
                                }
                                target_names = tuple(
                                    [
                                        "__".join(
                                            [
                                                param_map["entity_byname"][int(i) - 1]
                                                for i in k
                                            ]
                                        )
                                        for k in target_order
                                    ]
                                )
                                add_parameter_value(
                                    target_db,
                                    target_entity_class,
                                    target_param,
                                    alternative_name,
                                    target_names,
                                    ts_export,
                                )

                    elif param_map["type"] == "float":
                        target_names = tuple(
                            [
                                "__".join(
                                    [param_map["entity_byname"][int(i) - 1] for i in k]
                                )
                                for k in target_order
                            ]
                        )
                        add_parameter_value(
                            target_db,
                            target_entity_class,
                            target_param,
                            param_map["alternative_name"],
                            target_names,
                            multiplier * param_map["parsed_value"],
                        )

    try:
        target_db.commit_session("Added map of periods, historical data to timeseries")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit map of periods, historical data to timeseries error:", e)


def _ensure_list(value):
    """Ensure a parsed JSON value is a list (wrap single values in a list)."""
    if isinstance(value, list):
        return value
    return [value]


def _has_investment_parameters(source_db):
    """Check if the INES source database has any investment parameters defined."""
    investment_params = [
        ("unit", "investment_cost"),
        ("link", "investment_cost"),
        ("node", "storage_investment_cost"),
        ("unit", "investment_method"),
        ("link", "investment_method"),
        ("node", "storage_investment_method"),
    ]
    for entity_class, param in investment_params:
        items = source_db.get_parameter_value_items(
            entity_class_name=entity_class, parameter_definition_name=param,
        )
        if items:
            return True
    return False


def timeline_setup(source_db, target_db):

    # Determine default alternative name from source DB
    default_alt = source_db.get_alternative_items()[0]["name"]

    # Process scenario realizations (shared across all models)
    sto_structure = "stochastic"
    sto_scenario = "realization"
    add_entity(target_db, "stochastic_structure", (sto_structure,))
    add_entity(target_db, "stochastic_scenario", (sto_scenario,))
    add_entity(
        target_db,
        "stochastic_structure__stochastic_scenario",
        (sto_structure, sto_scenario),
    )

    # Check if investment parameters exist in source DB
    has_investments = _has_investment_parameters(source_db)

    # Loop over all solve_pattern entities — each becomes a model
    for sp_entity in source_db.get_entity_items(entity_class_name="solve_pattern"):
        model_name = sp_entity["name"]
        sp_byname = (model_name,)

        add_entity(
            target_db, "model__default_stochastic_structure", (model_name, sto_structure)
        )
        add_entity(
            target_db,
            "model__default_investment_stochastic_structure",
            (model_name, sto_structure),
        )

        resolution = json.loads(
            source_db.get_parameter_value_items(
                entity_class_name="solve_pattern",
                parameter_definition_name="time_resolution",
                entity_byname=sp_byname,
            )[0]["value"]
        )["data"]

        duration_value = json.loads(
            source_db.get_parameter_value_items(
                entity_class_name="solve_pattern", parameter_definition_name="duration",
                entity_byname=sp_byname,
            )[0]["value"]
        )["data"]
        durations = _ensure_list(duration_value)

        start_time_value = json.loads(
            source_db.get_parameter_value_items(
                entity_class_name="solve_pattern", parameter_definition_name="start_time",
                entity_byname=sp_byname,
            )[0]["value"]
        )["data"]
        start_times = _ensure_list(start_time_value)

        # rolling optimization parameters (optional)
        rolling_jump_items = source_db.get_parameter_value_items(
            entity_class_name="solve_pattern", parameter_definition_name="rolling_jump",
            entity_byname=sp_byname,
        )
        rolling_jump = json.loads(rolling_jump_items[0]["value"])["data"] if rolling_jump_items else None
        rolling_horizon_items = source_db.get_parameter_value_items(
            entity_class_name="solve_pattern", parameter_definition_name="rolling_horizon",
            entity_byname=sp_byname,
        )
        rolling_horizon = json.loads(rolling_horizon_items[0]["value"])["data"] if rolling_horizon_items else None

        def get_duration(idx):
            if len(durations) > idx:
                return durations[idx]
            return durations[0]

        # Create one temporal block per start_time
        all_starts = []
        all_ends = []
        for i, st in enumerate(start_times):
            duration = get_duration(i)
            block_start = pd.Timestamp(st)
            block_end = block_start + pd.Timedelta(duration)
            all_starts.append(block_start)
            all_ends.append(block_end)

            temporal_block_name = f"{model_name}_tb{i}"
            add_entity(target_db, "temporal_block", (temporal_block_name,))
            add_entity(
                target_db,
                "model__default_temporal_block",
                (model_name, temporal_block_name),
            )
            add_parameter_value(
                target_db, "temporal_block", "resolution",
                default_alt, (temporal_block_name,),
                {"type": "duration", "data": resolution},
            )
            add_parameter_value(
                target_db, "temporal_block", "block_start",
                default_alt, (temporal_block_name,),
                {"type": "date_time", "data": block_start.isoformat()},
            )
            if rolling_horizon is not None:
                add_parameter_value(
                    target_db, "temporal_block", "block_end",
                    default_alt, (temporal_block_name,),
                    {"type": "duration", "data": rolling_horizon},
                )
            else:
                add_parameter_value(
                    target_db, "temporal_block", "block_end",
                    default_alt, (temporal_block_name,),
                    {"type": "date_time", "data": block_end.isoformat()},
                )

        # Model start/end from min/max of temporal blocks
        add_parameter_value(
            target_db, "model", "model_start",
            default_alt, (model_name,),
            {"type": "date_time", "data": min(all_starts).isoformat()},
        )
        add_parameter_value(
            target_db, "model", "model_end",
            default_alt, (model_name,),
            {"type": "date_time", "data": max(all_ends).isoformat()},
        )

        if rolling_jump is not None:
            add_parameter_value(
                target_db, "model", "roll_forward",
                default_alt, (model_name,),
                {"type": "duration", "data": rolling_jump},
            )

        if rolling_horizon is not None:
            add_parameter_value(
                target_db, "model", "window_duration",
                default_alt, (model_name,),
                {"type": "duration", "data": rolling_horizon},
            )

        # Investment temporal block (if investment parameters exist)
        if has_investments:
            inv_tb_name = f"{model_name}_investments"
            add_entity(target_db, "temporal_block", (inv_tb_name,))
            add_entity(
                target_db,
                "model__default_investment_temporal_block",
                (model_name, inv_tb_name),
            )
            add_parameter_value(
                target_db, "temporal_block", "resolution",
                default_alt, (inv_tb_name,),
                {"type": "duration", "data": durations[0]},
            )

    # Set-based time resolution override
    time_res_scope_items = source_db.get_parameter_value_items(
        entity_class_name="solve_pattern",
        parameter_definition_name="time_resolution_scope",
    )
    if time_res_scope_items and time_res_scope_items[0]["parsed_value"] == "set_based_override":
        _process_set_based_temporal_blocks(source_db, target_db, default_alt, has_investments)

    try:
        target_db.commit_session("Added timeline")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit timeline error:", e)


def _process_set_based_temporal_blocks(source_db, target_db, default_alt, has_investments):
    """Create per-set temporal blocks when time_resolution_scope is set_based_override.

    For each set with a time_resolution parameter, creates copies of the existing
    realization temporal blocks (and investment temporal block if applicable) with
    the set's resolution. Links set member nodes, units, and connections to these blocks.
    """
    # Collect existing temporal blocks from model__default_temporal_block
    realization_blocks = []
    for ent in target_db.get_entity_items(entity_class_name="model__default_temporal_block"):
        tb_name = ent["entity_byname"][1]
        realization_blocks.append(tb_name)

    investment_blocks = []
    if has_investments:
        for ent in target_db.get_entity_items(entity_class_name="model__default_investment_temporal_block"):
            tb_name = ent["entity_byname"][1]
            investment_blocks.append(tb_name)

    for pv in source_db.get_parameter_value_items(
        entity_class_name="set", parameter_definition_name="time_resolution"
    ):
        set_name = pv["entity_byname"][0]
        set_resolution = json.loads(pv["value"])["data"]

        # Create copies of realization temporal blocks for this set
        set_tb_names = []
        for orig_tb in realization_blocks:
            set_tb_name = f"{set_name}_{orig_tb}"
            set_tb_names.append(set_tb_name)
            add_entity(target_db, "temporal_block", (set_tb_name,))
            # Copy block_start and block_end from original, override resolution
            for param_name in ["block_start", "block_end"]:
                orig_param = target_db.get_parameter_value_item(
                    entity_class_name="temporal_block",
                    entity_byname=(orig_tb,),
                    parameter_definition_name=param_name,
                    alternative_name=default_alt,
                )
                if orig_param:
                    add_parameter_value(
                        target_db, "temporal_block", param_name,
                        default_alt, (set_tb_name,),
                        orig_param["parsed_value"],
                    )
            add_parameter_value(
                target_db, "temporal_block", "resolution",
                default_alt, (set_tb_name,),
                {"type": "duration", "data": set_resolution},
            )

        # Create copies of investment temporal blocks for this set
        set_inv_tb_names = []
        for orig_tb in investment_blocks:
            set_inv_tb_name = f"{set_name}_{orig_tb}"
            set_inv_tb_names.append(set_inv_tb_name)
            add_entity(target_db, "temporal_block", (set_inv_tb_name,))
            # Copy resolution from original investment block (duration-based)
            orig_res = target_db.get_parameter_value_item(
                entity_class_name="temporal_block",
                entity_byname=(orig_tb,),
                parameter_definition_name="resolution",
                alternative_name=default_alt,
            )
            if orig_res:
                add_parameter_value(
                    target_db, "temporal_block", "resolution",
                    default_alt, (set_inv_tb_name,),
                    orig_res["parsed_value"],
                )

        # Link set member nodes
        for member in source_db.get_entity_items(entity_class_name="set__node"):
            if member["entity_byname"][0] == set_name:
                node_name = member["entity_byname"][1]
                for tb in set_tb_names:
                    try:
                        add_entity(target_db, "node__temporal_block", (node_name, tb))
                    except RuntimeError:
                        pass
                for tb in set_inv_tb_names:
                    try:
                        add_entity(target_db, "node__investment_temporal_block", (node_name, tb))
                    except RuntimeError:
                        pass

        # Link set member units
        for member in source_db.get_entity_items(entity_class_name="set__unit"):
            if member["entity_byname"][0] == set_name:
                unit_name = member["entity_byname"][1]
                for tb in set_tb_names:
                    try:
                        add_entity(target_db, "units_on__temporal_block", (unit_name, tb))
                    except RuntimeError:
                        pass
                for tb in set_inv_tb_names:
                    try:
                        add_entity(target_db, "unit__investment_temporal_block", (unit_name, tb))
                    except RuntimeError:
                        pass

        # Link set member links (INES link → SpineOpt connection)
        for member in source_db.get_entity_items(entity_class_name="set__link"):
            if member["entity_byname"][0] == set_name:
                link_name = member["entity_byname"][1]
                for tb in set_inv_tb_names:
                    try:
                        add_entity(target_db, "connection__investment_temporal_block", (link_name, tb))
                    except RuntimeError:
                        pass


def storage_state_fix_method(source_db, target_db):

    all_periods = []
    for pv in source_db.get_parameter_value_items(
        entity_class_name="solve_pattern", parameter_definition_name="period"
    ):
        for p in _ensure_list(json.loads(pv["value"])["data"]):
            if p not in all_periods:
                all_periods.append(p)
    resolution = json.loads(
        source_db.get_parameter_value_items(
            entity_class_name="solve_pattern",
            parameter_definition_name="time_resolution",
        )[0]["value"]
    )["data"]
    block_starts = {}
    for period in all_periods:
        py_start = json.loads(
            source_db.get_parameter_value_items(
                entity_class_name="period",
                parameter_definition_name="start_time",
                entity_byname=(period,),
            )[0]["value"]
        )["data"]
        block_starts[period] = (
            (pd.Timestamp(py_start) + pd.Timedelta(days=366)).isoformat()
            if bool(pd.Timestamp(py_start).year % 4 == 0)
            else py_start
        )
    for storage_method in source_db.get_parameter_value_items(
        parameter_definition_name="storage_state_fix_method"
    ):
        capacities_ = source_db.get_parameter_value_items(
            entity_class_name=storage_method["entity_class_name"],
            entity_byname=storage_method["entity_byname"],
            parameter_definition_name="storage_capacity",
        )
        if capacities_:
            if storage_method["parsed_value"] == "fix_start":
                values_ = source_db.get_parameter_value_items(
                    entity_class_name=storage_method["entity_class_name"],
                    entity_byname=storage_method["entity_byname"],
                    parameter_definition_name="storage_state_fix",
                )
                if values_:
                    existing_items = source_db.get_parameter_value_items(
                        entity_class_name=storage_method["entity_class_name"],
                        entity_byname=storage_method["entity_byname"],
                        parameter_definition_name="storages_existing",
                    )
                    existing_ = existing_items[0] if existing_items else None
                    if not existing_:
                        multiplier = 1.0
                    else:
                        if existing_["type"] == "float":
                            multiplier = existing_["parsed_value"]
                        elif existing_["type"] == "map":
                            if len(existing_["parsed_value"].values) == 1:
                                multiplier = existing_["parsed_value"].values[0]
                            else:
                                multiplier = dict(
                                    zip(
                                        existing_["parsed_value"].indexes,
                                        existing_["parsed_value"].values,
                                    )
                                )

                    for capacity_ in capacities_:
                        for value_ in values_:
                            if value_["type"] == "float":
                                if capacity_["type"] == "float":
                                    target_value_ = (
                                        value_["parsed_value"]
                                        * capacity_["parsed_value"]
                                    )
                                if (
                                    value_["alternative_name"]
                                    == capacity_["alternative_name"]
                                ):
                                    alternative_name = value_["alternative_name"]
                                else:
                                    if value_["alternative_name"] == existing_["alternative_name"]:
                                        alternative_name = capacity_["alternative_name"]
                                    elif capacity_["alternative_name"] == existing_["alternative_name"]:
                                        alternative_name = value_["alternative_name"]
                                    else:
                                        add_alternative(
                                            target_db,
                                            f"{capacity_['alternative_name']}_{value_['alternative_name']}",
                                        )
                                        alternative_name = f"{capacity_['alternative_name']}_{value_['alternative_name']}"

                                indexes_ = []
                                vals_ = []
                                for period, block_start in block_starts.items():
                                    indexes_.append(
                                        (
                                            pd.Timestamp(block_start)
                                            - pd.Timedelta(resolution)
                                        ).isoformat()
                                    )
                                    indexes_.append(block_start)
                                    vals_.append(
                                        (
                                            multiplier
                                            if isinstance(multiplier, float)
                                            else multiplier[period]
                                        )
                                        * target_value_
                                    )
                                    vals_.append(None)
                                target_ts_ = {
                                    "type": "time_series",
                                    "data": dict(zip(indexes_, vals_)),
                                }
                                add_parameter_value(
                                    target_db,
                                    "node",
                                    "storage_state_fix",
                                    alternative_name,
                                    value_["entity_byname"],
                                    target_ts_,
                                )
                else:
                    print(
                        "WARNING: FIXED STATE DOES NOT EXIST ",
                        storage_method["entity_byname"],
                    )
        else:
            print(
                "WARNING: CAPACITY NOT DEFINED, THEN FIX STATE NOT ADDED",
                storage_method["entity_byname"],
            )
    try:
        target_db.commit_session("Added fixed storage state method")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit fixed storage state error:", e)


def storage_state_binding_method(source_db, target_db):

    for storage_method in source_db.get_parameter_value_items(
        parameter_definition_name="storage_state_binding_method"
    ):
        if storage_method["parsed_value"] == "leap_over_within_period":
            for entity_map in target_db.get_entity_items(
                entity_class_name="model__default_temporal_block"
            ):
                add_entity(
                    target_db,
                    "node__temporal_block",
                    (storage_method["entity_name"], entity_map["entity_byname"][1]),
                )
                add_parameter_value(
                    target_db,
                    "node__temporal_block",
                    "cyclic_condition",
                    storage_method["alternative_name"],
                    (storage_method["entity_name"], entity_map["entity_byname"][1]),
                    True,
                )
    try:
        target_db.commit_session("Added storage state binding method")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit storage state binding method error:", e)


def limiting_investments_notallowed(source_db, target_db):

    retirement_method = {
        "unit": "retirement_method",
        "link": "retirement_method",
        "node": "storage_retirement_method",
    }
    target_candi = {
        "unit": "units_existing",
        "link": "links_existing",
        "node": "storages_existing",
    }
    target_class = {"unit": "unit", "link": "connection", "node": "node"}
    target_param = {
        "unit": "investment_count_max_cumulative",
        "link": "investment_count_max_cumulative",
        "node": "storage_investment_count_max_cumulative",
    }
    fix_param = {
        "unit": "investment_count_fix_cumulative",
        "link": "investment_count_fix_cumulative",
        "node": "storage_investment_count_fix_cumulative",
    }
    fix_av_param = {
        "unit": "investment_count_fix_cumulative",
        "link": "investment_count_fix_cumulative",
        "node": "storage_investment_count_fix_cumulative",
    }
    starttime = {}
    year_repr = {}

    all_periods = []
    for pv in source_db.get_parameter_value_items(
        entity_class_name="solve_pattern", parameter_definition_name="period"
    ):
        for p in _ensure_list(json.loads(pv["value"])["data"]):
            if p not in all_periods:
                all_periods.append(p)
    for period in all_periods:
        starttime[period] = json.loads(
            source_db.get_parameter_value_items(
                entity_class_name="period",
                entity_byname=(period,),
                parameter_definition_name="start_time",
            )[0]["value"]
        )["data"]
        year_repr[period] = source_db.get_parameter_value_items(
            entity_class_name="period",
            entity_byname=(period,),
            parameter_definition_name="years_represented",
        )[0]["parsed_value"]

    for source_param in ["investment_method", "storage_investment_method"]:
        for param_map in [
            i
            for i in source_db.get_parameter_value_items(
                parameter_definition_name=source_param
            )
            if i["parsed_value"] == "not_allowed"
        ]:
            existing_ = source_db.get_parameter_value_item(
                entity_class_name=param_map["entity_class_name"],
                parameter_definition_name=target_candi[param_map["entity_class_name"]],
                entity_byname=param_map["entity_byname"],
                alternative_name=param_map["alternative_name"],
            )
            if existing_:
                if existing_["type"] == "map":

                    map_table = convert_map_to_table(existing_["parsed_value"])
                    index_names = nested_index_names(existing_["parsed_value"])
                    data = pd.DataFrame(
                        map_table, columns=index_names + ["value"]
                    ).set_index(index_names[0])
                    data.index = data.index.astype("string")

                    if any(i in data.index for i in starttime):
                        indexes_ = []
                        values_ = []
                        for period_, ts_index_ in starttime.items():
                            if period_ in data.index:
                                values_.append(float(data.at[period_, "value"]))
                                # this should be removed once the fixed resolution is repaired
                                indexes_.append(ts_index_)

                        values_.append(values_[-1])
                        indexes_.append(
                            (
                                pd.Timestamp(ts_index_).replace(
                                    year=int(
                                        pd.Timestamp(ts_index_).year
                                        + year_repr[period_]
                                    )
                                )
                            ).isoformat()
                        )

                        if len(data) > 1:
                            value_ = {
                                "type": "time_series",
                                "data": dict(zip(indexes_, values_)),
                            }
                        else:
                            value_ = values_[0]

                        add_parameter_value(
                            target_db,
                            target_class[param_map["entity_class_name"]],
                            target_param[param_map["entity_class_name"]],
                            existing_["alternative_name"],
                            existing_["entity_byname"],
                            value_,
                        )
                        add_parameter_value(
                            target_db,
                            target_class[param_map["entity_class_name"]],
                            fix_param[param_map["entity_class_name"]],
                            existing_["alternative_name"],
                            existing_["entity_byname"],
                            0.0,
                        )

                        retirement_method_items = source_db.get_parameter_value_items(
                            entity_class_name=param_map["entity_class_name"],
                            parameter_definition_name=retirement_method[
                                param_map["entity_class_name"]
                            ],
                            entity_byname=param_map["entity_byname"],
                        )
                        retirement_method_value = retirement_method_items[0] if retirement_method_items else None
                        if retirement_method_value:
                            if retirement_method_value["parsed_value"] == "not_retired":
                                add_parameter_value(
                                    target_db,
                                    target_class[param_map["entity_class_name"]],
                                    fix_av_param[param_map["entity_class_name"]],
                                    existing_["alternative_name"],
                                    existing_["entity_byname"],
                                    value_,
                                )

                elif existing_["type"] == "float":
                    value_ = existing_["parsed_value"]
                    add_parameter_value(
                        target_db,
                        target_class[param_map["entity_class_name"]],
                        target_param[param_map["entity_class_name"]],
                        existing_["alternative_name"],
                        existing_["entity_byname"],
                        value_,
                    )
                    retirement_method_items = source_db.get_parameter_value_items(
                        entity_class_name=param_map["entity_class_name"],
                        parameter_definition_name=retirement_method[
                            param_map["entity_class_name"]
                        ],
                        entity_byname=param_map["entity_byname"],
                    )
                    retirement_method_value = retirement_method_items[0] if retirement_method_items else None
                    if retirement_method_value and retirement_method_value["parsed_value"] == "not_retired":
                        fix_value = value_
                    else:
                        fix_value = 0.0
                    add_parameter_value(
                        target_db,
                        target_class[param_map["entity_class_name"]],
                        fix_param[param_map["entity_class_name"]],
                        existing_["alternative_name"],
                        existing_["entity_byname"],
                        fix_value,
                    )

            else:
                print(
                    f"There is no existing capacity in {param_map['entity_class_name']} {param_map['entity_byname']}"
                )

    try:
        target_db.commit_session("Added candadite assets")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit candadite assets error:", e)


def set_to_entities_and_parameters(source_db, target_db):

    model_duration_value = json.loads(
        source_db.get_parameter_value_items(
            entity_class_name="solve_pattern", parameter_definition_name="duration"
        )[0]["value"]
    )["data"]
    model_duration = _ensure_list(model_duration_value)[0]
    resolution = json.loads(
        source_db.get_parameter_value_items(
            entity_class_name="solve_pattern",
            parameter_definition_name="time_resolution",
        )[0]["value"]
    )["data"]

    for source_parameter in ["max_cumulative", "flow_max_cumulative"]:
        for source_dict_parameter in source_db.get_parameter_value_items(
            entity_class_name="set", parameter_definition_name=source_parameter
        ):
            source_relationships = {
                relation: []
                for relation in [
                    "set__unit_flow",
                    "set__node",
                    "set__unit",
                    "set__link",
                ]
            }
            for relation in source_relationships:
                for element in source_db.get_entity_items(entity_class_name=relation):
                    if (
                        element["entity_byname"][0]
                        == source_dict_parameter["entity_byname"][0]
                    ):
                        source_relationships[relation].append(element["entity_byname"])
            if source_parameter == "max_cumulative":
                try:
                    add_entity(
                        target_db,
                        "investment_group",
                        source_dict_parameter["entity_byname"],
                    )
                    print(
                        "Entity already created",
                        "investment_group",
                        source_dict_parameter["entity_byname"],
                    )
                except:
                    pass
                add_parameter_value(
                    target_db,
                    "investment_group",
                    "investment_count_total_max_cumulative",
                    source_dict_parameter["alternative_name"],
                    source_dict_parameter["entity_byname"],
                    source_dict_parameter["parsed_value"],
                )
                for entity_relation, list_relation in source_relationships.items():
                    if entity_relation == "set__unit":
                        for names_relation in list_relation:
                            entity_byname = (names_relation[1], names_relation[0])
                            add_entity(
                                target_db, "unit__investment_group", entity_byname
                            )
                    if entity_relation == "set__node":
                        for names_relation in list_relation:
                            entity_byname = (names_relation[1], names_relation[0])
                            add_entity(
                                target_db, "node__investment_group", entity_byname
                            )
                    if entity_relation == "set__link":
                        for names_relation in list_relation:
                            entity_byname = (names_relation[1], names_relation[0])
                            add_entity(
                                target_db, "connection__investment_group", entity_byname
                            )

            elif source_parameter == "flow_max_cumulative":
                if len(source_relationships) == 1:
                    for entity_relation, names_relation in source_relationships.items():
                        if entity_relation == "set__unit_flow":
                            source_flow = source_db.get_entity_items(
                                entity_byname=names_relation[1:]
                            )[0]["entity_class_name"]
                            target_entity_class = (
                                "node__to_unit"
                                if source_flow == "node__to_unit"
                                else "unit__to_node"
                            )
                            target_entity_names = (
                                (names_relation[1], names_relation[2])
                                if source_flow == "node__to_unit"
                                else (names_relation[1], names_relation[2])
                            )
                            target_cumulated_param = (
                                "flow_limits_max_cumulative"
                            )
                            try:
                                add_entity(
                                    target_db, target_entity_class, target_entity_names
                                )
                            except:
                                pass
                            model_duration_hours = pd.Timedelta(
                                model_duration
                            ) / pd.Timedelta(resolution)
                            param_value = (
                                model_duration_hours
                                * source_dict_parameter["parsed_value"]
                            )
                            add_parameter_value(
                                target_db,
                                target_entity_class,
                                target_cumulated_param,
                                source_dict_parameter["alternative_name"],
                                target_entity_names,
                                param_value,
                            )
                else:
                    pass

    # invest_max_total and invest_max_period → investment_capacity_total_max_cumulative
    periods_info = _get_periods_info(source_db)
    for source_parameter in ["invest_max_total", "invest_max_period"]:
        for pv in source_db.get_parameter_value_items(
            entity_class_name="set", parameter_definition_name=source_parameter
        ):
            set_name = pv["entity_byname"][0]
            alt = pv["alternative_name"]

            # Ensure investment_group entity and member relationships exist
            try:
                add_entity(target_db, "investment_group", (set_name,))
            except RuntimeError:
                pass
            for relation, target_rel in [
                ("set__unit", "unit__investment_group"),
                ("set__node", "node__investment_group"),
                ("set__link", "connection__investment_group"),
            ]:
                for elem in source_db.get_entity_items(entity_class_name=relation):
                    if elem["entity_byname"][0] == set_name:
                        try:
                            add_entity(target_db, target_rel, (elem["entity_byname"][1], set_name))
                        except RuntimeError:
                            pass

            if pv["type"] == "float":
                value_to_set = pv["parsed_value"]
            elif pv["type"] == "map":
                ts = _map_to_time_series(pv["parsed_value"], periods_info)
                if ts:
                    value_to_set = ts
                else:
                    continue
            else:
                continue

            try:
                add_parameter_value(
                    target_db, "investment_group",
                    "investment_capacity_total_max_cumulative",
                    alt, (set_name,), value_to_set,
                )
            except RuntimeError:
                db_val, val_type = api.to_database(value_to_set)
                target_db.update_parameter_value_item(
                    entity_class_name="investment_group",
                    entity_byname=(set_name,),
                    parameter_definition_name="investment_capacity_total_max_cumulative",
                    alternative_name=alt,
                    value=db_val,
                    type=val_type,
                )

    # flow_max_instant / flow_min_instant → user_constraint with right_hand_side and constraint_sense
    for source_parameter, sense in [("flow_max_instant", "<="), ("flow_min_instant", ">=")]:
        for pv in source_db.get_parameter_value_items(
            entity_class_name="set", parameter_definition_name=source_parameter
        ):
            set_name = pv["entity_byname"][0]
            alt = pv["alternative_name"]

            # Create user_constraint entity
            try:
                add_entity(target_db, "user_constraint", (set_name,))
            except RuntimeError:
                pass

            # Set right_hand_side
            add_parameter_value(
                target_db, "user_constraint", "right_hand_side",
                alt, (set_name,), pv["parsed_value"],
            )

            # Set constraint_sense
            try:
                add_parameter_value(
                    target_db, "user_constraint", "constraint_sense",
                    alt, (set_name,), sense,
                )
            except RuntimeError:
                pass

            # Add set__unit_flow members as unit_flow__user_constraint
            for member in source_db.get_entity_items(entity_class_name="set__unit_flow"):
                if member["entity_byname"][0] != set_name:
                    continue
                flow_byname = member["entity_byname"][1:]
                flow_class = source_db.get_entity_items(
                    entity_byname=flow_byname
                )[0]["entity_class_name"]
                if flow_class == "node__to_unit":
                    uf_uc_byname = (flow_byname[0], flow_byname[1], set_name)
                else:
                    uf_uc_byname = (flow_byname[0], flow_byname[1], set_name)
                try:
                    add_entity(target_db, "unit_flow__user_constraint", uf_uc_byname)
                except RuntimeError:
                    pass
                try:
                    add_parameter_value(
                        target_db, "unit_flow__user_constraint",
                        "coefficient_for_unit_flow",
                        alt, uf_uc_byname, 1.0,
                    )
                except RuntimeError:
                    pass

    try:
        target_db.commit_session("Added set constraints")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit set constraints error:", e)


def default_parameters(target_db, settings):
    if not settings:
        return
    default_alt = target_db.get_alternative_items()[0]["name"]
    for target_entity_class in settings:
        for entity_item in target_db.get_entity_items(
            entity_class_name=target_entity_class
        ):
            for target_parameter in settings[target_entity_class]:
                add_parameter_value(
                    target_db,
                    target_entity_class,
                    target_parameter,
                    default_alt,
                    entity_item["entity_byname"],
                    settings[target_entity_class][target_parameter],
                )
    try:
        target_db.commit_session("Added default_parameters")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit default_parameters error:", e)


def candidates_to_number_of(target_db):
    # For investment candidates, set existing count to 0
    for entity_class, inv_param, existing_param in [
        ("unit", "investment_count_max_cumulative", "existing_units"),
        ("connection", "investment_count_max_cumulative", "existing_connections"),
        ("node", "storage_investment_count_max_cumulative", "existing_storages"),
    ]:
        for param_map in target_db.get_parameter_value_items(
            entity_class_name=entity_class,
            parameter_definition_name=inv_param,
        ):
            add_parameter_value(
                target_db,
                entity_class,
                existing_param,
                param_map["alternative_name"],
                param_map["entity_byname"],
                0.0,
            )

    try:
        target_db.commit_session("Added candidate to number of")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit candidate to number of error:", e)


def existing_capacity(source_db, target_db):

    entity_map = {"unit": "unit", "node": "node", "link": "connection"}
    parameter_conversion = {
        "units_existing": "existing_units",
        "links_existing": "existing_connections",
        "storages_existing": "existing_storages",
    }
    for source_parameter in parameter_conversion:
        target_parameter = parameter_conversion[source_parameter]
        for param_map in source_db.get_parameter_value_items(
            parameter_definition_name=source_parameter
        ):
            target_entity = entity_map[param_map["entity_class_name"]]
            if param_map["type"] == "map":
                param_dict = json.loads(param_map["value"].decode("utf-8"))
                param_value = param_dict["data"]
                vals = np.fromiter(param_value.values(), dtype=float)
                value_to_set = vals[0]
            elif param_map["type"] == "float":
                value_to_set = param_map["parsed_value"]
            else:
                continue
            # Try add, if already exists (from candidates_to_number_of), update
            try:
                add_parameter_value(
                    target_db,
                    target_entity,
                    target_parameter,
                    param_map["alternative_name"],
                    param_map["entity_byname"],
                    value_to_set,
                )
            except RuntimeError:
                db_val, val_type = api.to_database(value_to_set)
                target_db.update_parameter_value_item(
                    entity_class_name=target_entity,
                    entity_byname=param_map["entity_byname"],
                    parameter_definition_name=target_parameter,
                    alternative_name=param_map["alternative_name"],
                    value=db_val,
                    type=val_type,
                )
    try:
        target_db.commit_session("Added existing capacity")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit existing capacity error:", e)


def lifetime_to_duration(source_db, target_db, settings):

    for source_class in settings:
        for target_class in settings[source_class]:
            for source_param in settings[source_class][target_class]:
                for param_map in source_db.get_parameter_value_items(
                    entity_class_name=source_class,
                    parameter_definition_name=source_param,
                ):
                    if param_map["type"] == "float":
                        param_value = {
                            "type": "duration",
                            "data": str(int(param_map["parsed_value"])) + "Y",
                        }

                    for target_param in settings[source_class][target_class][
                        source_param
                    ]:
                        print(target_param, param_map["entity_byname"])
                        add_parameter_value(
                            target_db,
                            target_class,
                            target_param,
                            param_map["alternative_name"],
                            param_map["entity_byname"],
                            param_value,
                        )

    try:
        target_db.commit_session("Added lifetime conversion")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit lifetime conversion error:", e)


def unit_flow_variants(source_db, target_db, settings):

    parameters_mapping = {
        "equality_ratio": "flow_ratio_equality_coefficient",
        "less_than_ratio": "flow_ratio_less_than_coefficient",
        "greater_than_ration": "flow_ratio_greater_than_coefficient",
    }
    for param_map in source_db.get_parameter_value_items(
        entity_class_name="unit_flow__unit_flow"
    ):

        entity_byname = param_map["entity_byname"]  # 4-tuple
        target_parameter = parameters_mapping[param_map["parameter_definition_name"]]

        try:
            add_entity(target_db, "unit_flow__unit_flow", entity_byname)
        except RuntimeError:
            pass

        if param_map["type"] == "float":
            add_parameter_value(
                target_db,
                "unit_flow__unit_flow",
                target_parameter,
                param_map["alternative_name"],
                entity_byname,
                param_map["parsed_value"],
            )

        elif param_map["type"] == "map":

            starttime = {}
            year_repr = {}
            all_periods = []
            for pv_sp in source_db.get_parameter_value_items(
                entity_class_name="solve_pattern",
                parameter_definition_name="period",
            ):
                for p in _ensure_list(json.loads(pv_sp["value"])["data"]):
                    if p not in all_periods:
                        all_periods.append(p)
            for period in all_periods:
                starttime[period] = json.loads(
                    source_db.get_parameter_value_items(
                        entity_class_name="period",
                        entity_byname=(period,),
                        parameter_definition_name="start_time",
                    )[0]["value"]
                )["data"]
                year_repr[period] = source_db.get_parameter_value_items(
                    entity_class_name="period",
                    entity_byname=(period,),
                    parameter_definition_name="years_represented",
                )[0]["parsed_value"]

            duration_value = json.loads(
                source_db.get_parameter_value_items(
                    entity_class_name="solve_pattern",
                    parameter_definition_name="duration",
                )[0]["value"]
            )["data"]
            duration = _ensure_list(duration_value)[0]
            starttime_sp_value = json.loads(
                source_db.get_parameter_value_items(
                    entity_class_name="solve_pattern",
                    parameter_definition_name="start_time",
                )[0]["value"]
            )["data"]
            starttime_sp = _ensure_list(starttime_sp_value)
            resolution = json.loads(
                source_db.get_parameter_value_items(
                    entity_class_name="solve_pattern",
                    parameter_definition_name="time_resolution",
                )[0]["value"]
            )["data"]

            index_names = nested_index_names(param_map["parsed_value"])
            map_table = convert_map_to_table(param_map["parsed_value"])
            index_names = nested_index_names(param_map["parsed_value"])
            data = pd.DataFrame(map_table, columns=index_names + ["value"]).set_index(
                index_names[0]
            )
            data.index = data.index.astype("string")

            if any(i in data.index for i in starttime):
                indexes_ = []
                values_ = []
                for period_, ts_index_ in starttime.items():
                    values_.append(
                        (
                            float(data.at[period_, "value"])
                            if period_ in data.index
                            else 0.0
                        )
                    )

                    # this should be removed once the fixed resolution is repaired
                    indexes_.append(ts_index_)
                values_.append(values_[-1])
                indexes_.append(
                    (
                        pd.Timestamp(ts_index_).replace(
                            year=int(pd.Timestamp(ts_index_).year + year_repr[period_])
                        )
                    ).isoformat()
                )
                ts_export = {
                    "type": "time_series",
                    "data": dict(zip(indexes_, values_)),
                }
                add_parameter_value(
                    target_db,
                    "unit_flow__unit_flow",
                    target_parameter,
                    param_map["alternative_name"],
                    entity_byname,
                    ts_export,
                )

            if any(i in data.index for i in starttime_sp):
                for index, element in enumerate(starttime_sp):
                    try:
                        alternative_name = f"wy{str(pd.Timestamp(element).year)}"
                        add_alternative(target_db, alternative_name)
                    except:
                        pass
                    steps = pd.to_timedelta(duration) / pd.to_timedelta(resolution)
                    df_data = (
                        data.iloc[
                            data.index.tolist()
                            .index(element) : data.index.tolist()
                            .index(element)
                            + int(steps),
                            data.columns.tolist().index("value"),
                        ]
                    ).tolist()
                    ts_export = {
                        "type": "time_series",
                        "data": df_data,
                        "index": {
                            "start": f"2018{element[4:]}",
                            "resolution": resolution,
                            "ignore_year": True,
                        },
                    }
                    add_parameter_value(
                        target_db,
                        "unit_flow__unit_flow",
                        target_parameter,
                        alternative_name,
                        entity_byname,
                        ts_export,
                    )

    try:
        target_db.commit_session("Added unit flows")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit unit flows error:", e)


def flow_profile_method(source_db, target_db):

    duration_value = json.loads(
        source_db.get_parameter_value_items(
            entity_class_name="solve_pattern", parameter_definition_name="duration"
        )[0]["value"]
    )["data"]
    duration = _ensure_list(duration_value)[0]
    starttime_value = json.loads(
        source_db.get_parameter_value_items(
            entity_class_name="solve_pattern", parameter_definition_name="start_time"
        )[0]["value"]
    )["data"]
    starttime = _ensure_list(starttime_value)
    resolution = json.loads(
        source_db.get_parameter_value_items(
            entity_class_name="solve_pattern",
            parameter_definition_name="time_resolution",
        )[0]["value"]
    )["data"]

    for param_map in source_db.get_parameter_value_items(
        entity_class_name="node",
        parameter_definition_name="flow_profile",
    ):
        alt = param_map["alternative_name"]

        flow_method_items = source_db.get_parameter_value_items(
            entity_class_name="node",
            entity_byname=param_map["entity_byname"],
            parameter_definition_name="flow_scaling_method",
        )
        flow_method = flow_method_items[0] if flow_method_items else None

        flow_annual_items = None
        if flow_method and flow_method["parsed_value"] == "scale_to_annual":
            target_name = param_map["entity_name"]

            # Get flow_annual for scaling
            flow_annual_items = source_db.get_parameter_value_items(
                entity_class_name="node",
                entity_byname=param_map["entity_byname"],
                parameter_definition_name="flow_annual",
            )

            definition_condition = True
        elif flow_method and flow_method["parsed_value"] == "use_profile_directly":
            target_name = param_map["entity_name"]
            definition_condition = True
        else:
            target_name = param_map["entity_name"]
            definition_condition = True

        if definition_condition:
            if param_map["type"] == "map":
                index_names = nested_index_names(param_map["parsed_value"])
                map_table = convert_map_to_table(param_map["parsed_value"])
                index_names = nested_index_names(param_map["parsed_value"])
                data = pd.DataFrame(
                    map_table, columns=index_names + ["value"]
                ).set_index(index_names[0])
                data.index = data.index.astype("string")

                if any(i in data.index for i in starttime):
                    for index, element in enumerate(starttime):
                        try:
                            alternative_name = f"wy{str(pd.Timestamp(element).year)}"
                            add_alternative(target_db, alternative_name)
                        except:
                            pass
                        steps = pd.to_timedelta(duration) / pd.to_timedelta(resolution)
                        raw_data = data.iloc[
                            data.index.tolist()
                            .index(element) : data.index.tolist()
                            .index(element)
                            + int(steps),
                            data.columns.tolist().index("value"),
                        ]

                        # Apply scaling for scale_to_annual
                        if flow_method and flow_method["parsed_value"] == "scale_to_annual" and flow_annual_items:
                            flow_annual_val = flow_annual_items[0]["parsed_value"]
                            # flow_annual can be a map (period-indexed) or float
                            if hasattr(flow_annual_val, 'indexes'):
                                # Map: find matching period value
                                periods_info = _get_periods_info(source_db)
                                annual_value = None
                                for p_idx, p_name in enumerate(periods_info["periods"]):
                                    if periods_info["starttime"][p_name] == element:
                                        for fa_idx, fa_key in enumerate(flow_annual_val.indexes):
                                            if str(fa_key) == p_name:
                                                annual_value = float(flow_annual_val.values[fa_idx])
                                                break
                                        break
                                if annual_value is None:
                                    annual_value = float(flow_annual_val.values[0])
                            else:
                                annual_value = float(flow_annual_val)
                            profile_sum = abs(raw_data.sum())
                            num_steps = len(raw_data)
                            ts_duration = num_steps * pd.to_timedelta(resolution)
                            year_factor = pd.to_timedelta("8760h") / ts_duration
                            profile_sum_annual = profile_sum * year_factor
                            if profile_sum_annual > 0:
                                scale_factor = annual_value / profile_sum_annual
                            else:
                                scale_factor = 1.0
                            df_data = (-1.0 * scale_factor * raw_data).tolist()
                        else:
                            df_data = (-1.0 * raw_data).tolist()
                        ts_export = {
                            "type": "time_series",
                            "data": df_data,
                            "index": {
                                "start": f"2018{element[4:]}",
                                "resolution": resolution,
                                "ignore_year": True,
                            },
                        }
                        add_parameter_value(
                            target_db,
                            "node",
                            "demand",
                            alternative_name,
                            (target_name,),
                            ts_export,
                        )

            elif param_map["type"] == "time_series":
                ts_val = param_map["parsed_value"]
                if flow_method and flow_method["parsed_value"] == "scale_to_annual" and flow_annual_items:
                    flow_annual_val = flow_annual_items[0]["parsed_value"]
                    if isinstance(flow_annual_val, (int, float)):
                        annual_value = float(flow_annual_val)
                    else:
                        annual_value = float(flow_annual_val.values[0])
                    profile_sum = abs(sum(ts_val.values))
                    ts_timestamps = [pd.Timestamp(t) for t in ts_val.indexes]
                    if len(ts_timestamps) > 1:
                        ts_duration = ts_timestamps[-1] - ts_timestamps[0] + (ts_timestamps[1] - ts_timestamps[0])
                    else:
                        ts_duration = pd.to_timedelta(resolution)
                    year_factor = pd.to_timedelta("8760h") / ts_duration
                    profile_sum_annual = profile_sum * year_factor
                    scale_factor = annual_value / profile_sum_annual if profile_sum_annual > 0 else 1.0
                    scaled_values = [-1.0 * scale_factor * v for v in ts_val.values]
                else:
                    scaled_values = [-1.0 * v for v in ts_val.values]
                result_ts = api.TimeSeriesVariableResolution(ts_val.indexes, scaled_values, ignore_year=False, repeat=False, index_name="time step")
                add_parameter_value(
                    target_db,
                    "node",
                    "demand",
                    param_map["alternative_name"],
                    (target_name,),
                    result_ts,
                )

            elif param_map["type"] == "float":
                value = param_map["parsed_value"]
                if flow_method and flow_method["parsed_value"] == "scale_to_annual" and flow_annual_items:
                    flow_annual_val = flow_annual_items[0]["parsed_value"]
                    if isinstance(flow_annual_val, (int, float)):
                        annual_value = float(flow_annual_val)
                    else:
                        annual_value = float(flow_annual_val.values[0])
                    num_steps_year = pd.to_timedelta("8760h") / pd.to_timedelta(resolution)
                    profile_sum_annual = abs(value) * num_steps_year
                    if profile_sum_annual > 0:
                        scale_factor = annual_value / profile_sum_annual
                    else:
                        scale_factor = 1.0
                    demand_value = -1.0 * scale_factor * value
                else:
                    demand_value = -1.0 * value
                add_parameter_value(
                    target_db,
                    "node",
                    "demand",
                    param_map["alternative_name"],
                    (target_name,),
                    demand_value,
                )

    # flow_profile_forecasts → stochastic_scenario-indexed Map for demand
    for param_map in source_db.get_parameter_value_items(
        entity_class_name="node",
        parameter_definition_name="flow_profile_forecasts",
    ):
        if param_map["type"] != "map":
            continue
        node_name = param_map["entity_byname"][0]
        alt = param_map["alternative_name"]
        parsed = param_map["parsed_value"]

        # Get flow_scaling_method and flow_annual for this node
        flow_method_items = source_db.get_parameter_value_items(
            entity_class_name="node",
            entity_byname=(node_name,),
            parameter_definition_name="flow_scaling_method",
        )
        flow_method = flow_method_items[0] if flow_method_items else None
        flow_annual_items = None
        annual_value = None
        if flow_method and flow_method["parsed_value"] == "scale_to_annual":
            flow_annual_items = source_db.get_parameter_value_items(
                entity_class_name="node",
                entity_byname=(node_name,),
                parameter_definition_name="flow_annual",
            )
            if flow_annual_items:
                fav = flow_annual_items[0]["parsed_value"]
                annual_value = float(fav) if isinstance(fav, (int, float)) else float(fav.values[0])

        def _process_profile_value(profile_val):
            """Process a single profile value (float or nested ts) into demand value."""
            if isinstance(profile_val, (int, float)):
                if annual_value is not None:
                    num_steps_year = pd.to_timedelta("8760h") / pd.to_timedelta(resolution)
                    profile_sum_annual = abs(profile_val) * num_steps_year
                    scale_factor = annual_value / profile_sum_annual if profile_sum_annual > 0 else 1.0
                    return -1.0 * scale_factor * profile_val
                return -1.0 * profile_val
            # For nested Map/TimeSeries values, negate the values
            if hasattr(profile_val, 'indexes') and hasattr(profile_val, 'values'):
                negated_values = [-1.0 * v for v in profile_val.values]
                return type(profile_val)(profile_val.indexes, negated_values, profile_val.ignore_year, profile_val.repeat)
            return profile_val

        # Build scenario-indexed Map
        indexes = list(parsed.indexes)
        values = [_process_profile_value(v) for v in parsed.values]

        # Prepend realization from base flow_profile
        base_items = source_db.get_parameter_value_items(
            entity_class_name="node",
            parameter_definition_name="flow_profile",
            entity_byname=(node_name,),
        )
        if base_items:
            base_val = base_items[0]["parsed_value"]
            realization_demand = _process_profile_value(base_val)
            indexes = ["realization"] + indexes
            values = [realization_demand] + values

        scenario_map = Map(
            indexes=indexes,
            values=values,
            index_name="stochastic_scenario",
        )

        try:
            add_parameter_value(
                target_db, "node", "demand",
                alt, (node_name,), scenario_map,
            )
        except RuntimeError:
            db_val, val_type = api.to_database(scenario_map)
            target_db.update_parameter_value_item(
                entity_class_name="node",
                entity_byname=(node_name,),
                parameter_definition_name="demand",
                alternative_name=alt,
                value=db_val,
                type=val_type,
            )

    try:
        target_db.commit_session("Added flow profile")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit flow profile error:", e)


def process_efficiency(source_db, target_db):
    """Transform INES unit.efficiency and unit.conversion_method to SpineOpt flow ratios and operating_points."""

    for eff_param in source_db.get_parameter_value_items(
        entity_class_name="unit", parameter_definition_name="efficiency"
    ):
        unit_name = eff_param["entity_byname"][0]
        alt = eff_param["alternative_name"]

        # Get conversion method
        method_item = source_db.get_parameter_value_item(
            entity_class_name="unit",
            entity_byname=(unit_name,),
            parameter_definition_name="conversion_method",
            alternative_name=alt,
        )
        if not method_item:
            method_items = source_db.get_parameter_value_items(
                entity_class_name="unit",
                entity_byname=(unit_name,),
                parameter_definition_name="conversion_method",
            )
            method_item = method_items[0] if method_items else None
        conversion_method = method_item["parsed_value"] if method_item else "constant_efficiency"

        # Skip methods that use conversion_coefficient or unit_flow__unit_flow directly
        if conversion_method in ("coefficients_only", "piecewise_linear_for_each_flow"):
            continue

        # Get output and input nodes from SOURCE db to avoid emission nodes
        unit_outputs = [
            f["entity_byname"][1]
            for f in source_db.get_entity_items(entity_class_name="unit__to_node")
            if f["entity_byname"][0] == unit_name
        ]
        unit_inputs = [
            f["entity_byname"][0]
            for f in source_db.get_entity_items(entity_class_name="node__to_unit")
            if f["entity_byname"][1] == unit_name
        ]

        if not unit_inputs or not unit_outputs:
            continue

        efficiency = eff_param["parsed_value"]

        if conversion_method == "constant_efficiency":
            _process_constant_efficiency(
                target_db, unit_name, efficiency, unit_outputs, unit_inputs, alt
            )
        elif conversion_method in ("partial_load_efficiency", "piecewise_linear", "piecewise_SOS2"):
            _process_piecewise_efficiency(
                target_db, unit_name, efficiency, unit_outputs, unit_inputs, alt
            )

    try:
        target_db.commit_session("Added efficiency conversions")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit efficiency conversions error:", e)


def _process_constant_efficiency(target_db, unit_name, efficiency, unit_outputs, unit_inputs, alt):
    """Handle constant_efficiency: set flow_ratio_equality_coefficient = efficiency for each (out, in) pair."""
    for out_node in unit_outputs:
        for in_node in unit_inputs:
            if out_node == in_node:
                continue
            uf_byname = (unit_name, out_node, in_node, unit_name)
            existing = target_db.get_entity_item(
                entity_class_name="unit_flow__unit_flow", entity_byname=uf_byname
            )
            if not existing:
                try:
                    add_entity(target_db, "unit_flow__unit_flow", uf_byname)
                except RuntimeError:
                    pass
            existing_param = target_db.get_parameter_value_item(
                entity_class_name="unit_flow__unit_flow",
                entity_byname=uf_byname,
                parameter_definition_name="flow_ratio_equality_coefficient",
                alternative_name=alt,
            )
            if not existing_param:
                add_parameter_value(
                    target_db, "unit_flow__unit_flow",
                    "flow_ratio_equality_coefficient", alt, uf_byname, efficiency,
                )


def _process_piecewise_efficiency(target_db, unit_name, efficiency, unit_outputs, unit_inputs, alt):
    """Handle piecewise efficiency (partial_load, piecewise_linear, piecewise_SOS2).

    Extracts operating points and efficiencies from the efficiency Map,
    computes incremental flow ratios, sets operating_points and minimum_operating_point
    on input flows, and constraint_equality_flow_ratio as array on unit_flow__unit_flow.
    """
    if not isinstance(efficiency, Map):
        return

    operating_points = [float(idx) for idx in efficiency.indexes]
    efficiencies = [float(val) for val in efficiency.values]

    if len(operating_points) < 2:
        return

    # Set operating_points and minimum_operating_point on output flows
    #op_array = {"type": "array", "data": operating_points}
    min_op = operating_points[0]
    for out_node in unit_outputs:
        try:
            add_parameter_value(
                target_db, "unit__to_node", "operating_points",
                alt, (unit_name, out_node), api.Array(operating_points),
            )
        except RuntimeError:
            pass
        try:
            add_parameter_value(
                target_db, "unit__to_node", "minimum_operating_point",
                alt, (unit_name, out_node), min_op,
            )
        except RuntimeError:
            pass

    # Compute incremental flow ratios for each segment
    incremental_ratios = [1/eff for eff in efficiencies]
    ratio_array = api.Array(incremental_ratios)
    for out_node in unit_outputs:
        for in_node in unit_inputs:
            if out_node == in_node:
                continue
            uf_byname = (in_node, unit_name, unit_name, out_node)
            try:
                add_entity(target_db, "unit_flow__unit_flow", uf_byname)
            except RuntimeError:
                pass
            existing_param = target_db.get_parameter_value_item(
                entity_class_name="unit_flow__unit_flow",
                entity_byname=uf_byname,
                parameter_definition_name="flow_ratio_equality_coefficient",
                alternative_name=alt,
            )
            if not existing_param:
                add_parameter_value(
                    target_db, "unit_flow__unit_flow",
                    "flow_ratio_equality_coefficient", alt, uf_byname, ratio_array,
                )


def process_conversion_coefficients(source_db, target_db):
    """Convert INES conversion_coefficients to SpineOpt flow_ratio_equality_coefficient on unit_flow__unit_flow."""

    # Collect units that have efficiency defined (those are handled by process_efficiency)
    units_with_efficiency = set()
    for eff_param in source_db.get_parameter_value_items(
        entity_class_name="unit", parameter_definition_name="efficiency"
    ):
        method_items = source_db.get_parameter_value_items(
            entity_class_name="unit",
            entity_byname=eff_param["entity_byname"],
            parameter_definition_name="conversion_method",
        )
        method_item = method_items[0] if method_items else None
        conversion_method = method_item["parsed_value"] if method_item else "constant_efficiency"
        if conversion_method not in ("coefficients_only", "piecewise_linear_for_each_flow"):
            units_with_efficiency.add(eff_param["entity_byname"][0])

    for unit_entity in target_db.get_entity_items(entity_class_name="unit"):
        unit_name = unit_entity["name"]

        # Skip units where efficiency already set the flow ratios
        if unit_name in units_with_efficiency:
            continue

        # Find output nodes (unit__to_node in both INES and SpineOpt)
        unit_outputs = [
            f["entity_byname"][1]
            for f in target_db.get_entity_items(entity_class_name="unit__to_node")
            if f["entity_byname"][0] == unit_name
        ]

        # Find input nodes (node__to_unit in new SpineOpt, dims: [node, unit])
        unit_inputs = [
            f["entity_byname"][0]
            for f in target_db.get_entity_items(entity_class_name="node__to_unit")
            if f["entity_byname"][1] == unit_name
        ]

        if not unit_inputs or not unit_outputs:
            continue

        # Collect conversion coefficients from source for output flows
        output_coeffs = {}
        for out_node in unit_outputs:
            for cc_item in source_db.get_parameter_value_items(
                entity_class_name="unit__to_node",
                parameter_definition_name="conversion_coefficient",
            ):
                if cc_item["entity_byname"] == (unit_name, out_node):
                    alt = cc_item["alternative_name"]
                    if alt not in output_coeffs:
                        output_coeffs[alt] = {}
                    output_coeffs[alt][out_node] = cc_item["parsed_value"]

        # Collect conversion coefficients from source for input flows
        input_coeffs = {}
        for in_node in unit_inputs:
            for cc_item in source_db.get_parameter_value_items(
                entity_class_name="node__to_unit",
                parameter_definition_name="conversion_coefficient",
            ):
                if cc_item["entity_byname"] == (in_node, unit_name):
                    alt = cc_item["alternative_name"]
                    if alt not in input_coeffs:
                        input_coeffs[alt] = {}
                    input_coeffs[alt][in_node] = cc_item["parsed_value"]

        # Create unit_flow__unit_flow relationships with ratios
        all_alternatives = set(list(output_coeffs.keys()) + list(input_coeffs.keys()))
        for alt in all_alternatives:
            out_coeffs_alt = output_coeffs.get(alt, {})
            in_coeffs_alt = input_coeffs.get(alt, {})
            for out_node, out_coeff in out_coeffs_alt.items():
                for in_node, in_coeff in in_coeffs_alt.items():
                    if out_node == in_node:
                        continue
                    if isinstance(in_coeff, (int, float)) and in_coeff != 0:
                        ratio = out_coeff / in_coeff if isinstance(out_coeff, (int, float)) else out_coeff
                    else:
                        ratio = out_coeff
                    # entity_byname: (out_flow_unit, out_flow_node, in_flow_node, in_flow_unit)
                    uf_byname = (unit_name, out_node, in_node, unit_name)
                    # Check if unit_flow__unit_flow already exists (e.g. from unit_flow_variants)
                    existing = target_db.get_entity_item(
                        entity_class_name="unit_flow__unit_flow",
                        entity_byname=uf_byname,
                    )
                    if not existing:
                        add_entity(
                            target_db,
                            "unit_flow__unit_flow",
                            uf_byname,
                        )
                    # Check if ratio is already set by unit_flow_variants
                    existing_param = target_db.get_parameter_value_item(
                        entity_class_name="unit_flow__unit_flow",
                        entity_byname=uf_byname,
                        parameter_definition_name="flow_ratio_equality_coefficient",
                        alternative_name=alt,
                    )
                    if not existing_param:
                        add_parameter_value(
                            target_db,
                            "unit_flow__unit_flow",
                            "flow_ratio_equality_coefficient",
                            alt,
                            uf_byname,
                            ratio,
                        )

    try:
        target_db.commit_session("Added conversion coefficients")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit conversion coefficients error:", e)


def process_constraints(source_db, target_db):
    """Map INES constraint coefficients to SpineOpt user_constraint relationships."""

    # Map constraint_flow_coefficient from unit__to_node flows
    for param in source_db.get_parameter_value_items(
        entity_class_name="unit__to_node",
        parameter_definition_name="constraint_flow_coefficient",
    ):
        if param["type"] == "map":
            parsed = param["parsed_value"]
            for idx, val in zip(parsed.indexes, parsed.values):
                constraint_name = str(idx)
                unit_name = param["entity_byname"][0]
                node_name = param["entity_byname"][1]
                try:
                    add_entity(
                        target_db,
                        "unit_flow__user_constraint",
                        (unit_name, node_name, constraint_name),
                    )
                except RuntimeError:
                    pass
                add_parameter_value(
                    target_db,
                    "unit_flow__user_constraint",
                    "coefficient_for_unit_flow",
                    param["alternative_name"],
                    (unit_name, node_name, constraint_name),
                    float(val),
                )

    # Map constraint_flow_coefficient from node__to_unit flows (→ unit_flow__user_constraint)
    for param in source_db.get_parameter_value_items(
        entity_class_name="node__to_unit",
        parameter_definition_name="constraint_flow_coefficient",
    ):
        if param["type"] == "map":
            parsed = param["parsed_value"]
            for idx, val in zip(parsed.indexes, parsed.values):
                constraint_name = str(idx)
                node_name = param["entity_byname"][0]
                unit_name = param["entity_byname"][1]
                try:
                    add_entity(
                        target_db,
                        "unit_flow__user_constraint",
                        (node_name, unit_name, constraint_name),
                    )
                except RuntimeError:
                    pass
                add_parameter_value(
                    target_db,
                    "unit_flow__user_constraint",
                    "coefficient_for_unit_flow",
                    param["alternative_name"],
                    (node_name, unit_name, constraint_name),
                    float(val),
                )

    # Map constraint_unit_count_coefficient from unit → unit__user_constraint
    for param in source_db.get_parameter_value_items(
        entity_class_name="unit",
        parameter_definition_name="constraint_unit_count_coefficient",
    ):
        if param["type"] == "map":
            parsed = param["parsed_value"]
            for idx, val in zip(parsed.indexes, parsed.values):
                constraint_name = str(idx)
                unit_name = param["entity_byname"][0]
                try:
                    add_entity(
                        target_db,
                        "unit__user_constraint",
                        (unit_name, constraint_name),
                    )
                except RuntimeError:
                    pass
                add_parameter_value(
                    target_db,
                    "unit__user_constraint",
                    "coefficient_for_units_invested_available",
                    param["alternative_name"],
                    (unit_name, constraint_name),
                    float(val),
                )

    # Map constraint_online_coefficient from unit → unit__user_constraint
    for param in source_db.get_parameter_value_items(
        entity_class_name="unit",
        parameter_definition_name="constraint_online_coefficient",
    ):
        if param["type"] == "map":
            parsed = param["parsed_value"]
            for idx, val in zip(parsed.indexes, parsed.values):
                constraint_name = str(idx)
                unit_name = param["entity_byname"][0]
                try:
                    add_entity(
                        target_db,
                        "unit__user_constraint",
                        (unit_name, constraint_name),
                    )
                except RuntimeError:
                    pass
                add_parameter_value(
                    target_db,
                    "unit__user_constraint",
                    "coefficient_for_units_on",
                    param["alternative_name"],
                    (unit_name, constraint_name),
                    float(val),
                )

    # Map constraint_storage_count_coefficient from node → node__user_constraint
    for param in source_db.get_parameter_value_items(
        entity_class_name="node",
        parameter_definition_name="constraint_storage_count_coefficient",
    ):
        if param["type"] == "map":
            parsed = param["parsed_value"]
            for idx, val in zip(parsed.indexes, parsed.values):
                constraint_name = str(idx)
                node_name = param["entity_byname"][0]
                try:
                    add_entity(
                        target_db,
                        "node__user_constraint",
                        (node_name, constraint_name),
                    )
                except RuntimeError:
                    pass
                add_parameter_value(
                    target_db,
                    "node__user_constraint",
                    "coefficient_for_storages_invested_available",
                    param["alternative_name"],
                    (node_name, constraint_name),
                    float(val),
                )

    # Map constraint_storage_state_coefficient from node → node__user_constraint
    for param in source_db.get_parameter_value_items(
        entity_class_name="node",
        parameter_definition_name="constraint_storage_state_coefficient",
    ):
        if param["type"] == "map":
            parsed = param["parsed_value"]
            for idx, val in zip(parsed.indexes, parsed.values):
                constraint_name = str(idx)
                node_name = param["entity_byname"][0]
                try:
                    add_entity(
                        target_db,
                        "node__user_constraint",
                        (node_name, constraint_name),
                    )
                except RuntimeError:
                    pass
                add_parameter_value(
                    target_db,
                    "node__user_constraint",
                    "coefficient_for_node_state",
                    param["alternative_name"],
                    (node_name, constraint_name),
                    float(val),
                )

    # Map constraint_link_count_coefficient from link → connection__user_constraint
    for param in source_db.get_parameter_value_items(
        entity_class_name="link",
        parameter_definition_name="constraint_link_count_coefficient",
    ):
        if param["type"] == "map":
            parsed = param["parsed_value"]
            for idx, val in zip(parsed.indexes, parsed.values):
                constraint_name = str(idx)
                link_name = param["entity_byname"][0]
                try:
                    add_entity(
                        target_db,
                        "connection__user_constraint",
                        (link_name, constraint_name),
                    )
                except RuntimeError:
                    pass
                add_parameter_value(
                    target_db,
                    "connection__user_constraint",
                    "coefficient_for_connections_invested_available",
                    param["alternative_name"],
                    (link_name, constraint_name),
                    float(val),
                )

    # Map constraint_flow_coefficient from node__link__node → connection__to_node__user_constraint
    for param in source_db.get_parameter_value_items(
        entity_class_name="node__link__node",
        parameter_definition_name="constraint_flow_coefficient",
    ):
        if param["type"] == "map":
            parsed = param["parsed_value"]
            node1, link_name, node2 = param["entity_byname"]
            for idx, val in zip(parsed.indexes, parsed.values):
                constraint_name = str(idx)
                try:
                    add_entity(
                        target_db,
                        "connection__to_node__user_constraint",
                        (link_name, node2, constraint_name),
                    )
                except RuntimeError:
                    pass
                add_parameter_value(
                    target_db,
                    "connection__to_node__user_constraint",
                    "coefficient_for_connection_flow",
                    param["alternative_name"],
                    (link_name, node2, constraint_name),
                    float(val),
                )

    try:
        target_db.commit_session("Added user constraints")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit user constraints error:", e)


def process_system_discount_rate(source_db, target_db):
    """Map INES system discount_rate to SpineOpt model discount_rate.

    Priority: 1) system entity parameter value, 2) DB default value, 3) skip.
    """
    model_entities = target_db.get_entity_items(entity_class_name="model")
    if not model_entities:
        return

    default_alt = target_db.get_alternative_items()[0]["name"]

    # Try to get discount_rate from the INES system entity
    system_params = source_db.get_parameter_value_items(
        entity_class_name="system", parameter_definition_name="discount_rate"
    )
    if system_params:
        for param in system_params:
            if param["parsed_value"] is not None:
                for model_ent in model_entities:
                    model_name = model_ent["name"]
                    try:
                        add_parameter_value(
                            target_db, "model", "discount_rate",
                            param["alternative_name"], (model_name,),
                            param["parsed_value"],
                        )
                    except RuntimeError:
                        db_val, val_type = api.to_database(param["parsed_value"])
                        target_db.update_parameter_value_item(
                            entity_class_name="model",
                            entity_byname=(model_name,),
                            parameter_definition_name="discount_rate",
                            alternative_name=param["alternative_name"],
                            value=db_val,
                            type=val_type,
                        )
    else:
        # Try to get the default value from the source DB parameter definition
        param_defs = source_db.get_parameter_definition_items(
            entity_class_name="system", name="discount_rate"
        )
        if param_defs and param_defs[0]["parsed_value"] is not None:
            default_value = param_defs[0]["parsed_value"]
            for model_ent in model_entities:
                model_name = model_ent["name"]
                try:
                    add_parameter_value(
                        target_db, "model", "discount_rate",
                        default_alt, (model_name,), default_value,
                    )
                except RuntimeError:
                    pass

    try:
        target_db.commit_session("Added system discount rate")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit system discount rate error:", e)


def process_node_penalty(source_db, target_db, default_penalty):
    """Set balance_penalty on all nodes. Use penalty_upward if available, otherwise default.
    Skips commodity nodes (node_type: commodity)."""
    default_alt = target_db.get_alternative_items()[0]["name"]
    # Collect commodity node names to exclude
    commodity_nodes = set()
    for pv in source_db.get_parameter_value_items(
        entity_class_name="node", parameter_definition_name="node_type"
    ):
        if pv["parsed_value"] == "commodity":
            commodity_nodes.add(pv["entity_byname"][0])
    for node_entity in target_db.get_entity_items(entity_class_name="node"):
        node_name = node_entity["entity_byname"][0]
        if node_name in commodity_nodes:
            continue
        # Skip nodes with balance_type "none"
        bt_items = target_db.get_parameter_value_items(
            entity_class_name="node",
            entity_byname=(node_name,),
            parameter_definition_name="balance_type",
        )
        if bt_items and bt_items[0]["parsed_value"] == "none":
            continue
        existing = target_db.get_parameter_value_items(
            entity_class_name="node",
            entity_byname=(node_name,),
            parameter_definition_name="balance_penalty",
        )
        if not existing:
            add_parameter_value(
                target_db, "node", "balance_penalty",
                default_alt, (node_name,), default_penalty,
            )
    try:
        target_db.commit_session("Added node penalty defaults")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit node penalty defaults error:", e)


def process_commodity_price(source_db, target_db):
    """Map INES node.commodity_price to SpineOpt node__to_unit.vom_cost.

    Also handles commodity_price_forecasts as scenario-indexed Maps.
    """
    periods_info = _get_periods_info(source_db)

    # commodity_price (float or map)
    for pv in source_db.get_parameter_value_items(
        entity_class_name="node", parameter_definition_name="commodity_price"
    ):
        node_name = pv["entity_byname"][0]
        alt = pv["alternative_name"]
        if pv["type"] == "map":
            value = _map_to_time_series(pv["parsed_value"], periods_info)
            if not value:
                continue
        elif pv["type"] == "float":
            value = pv["parsed_value"]
        elif pv["type"] == "time_series":
            value = pv["parsed_value"]
        else:
            continue
        for ntu in source_db.get_entity_items(entity_class_name="node__to_unit"):
            if ntu["entity_byname"][0] == node_name:
                unit_name = ntu["entity_byname"][1]
                try:
                    add_parameter_value(
                        target_db, "node__to_unit", "vom_cost",
                        alt, (node_name, unit_name), value,
                    )
                except RuntimeError:
                    db_val, val_type = api.to_database(value)
                    target_db.update_parameter_value_item(
                        entity_class_name="node__to_unit",
                        entity_byname=(node_name, unit_name),
                        parameter_definition_name="vom_cost",
                        alternative_name=alt,
                        value=db_val,
                        type=val_type,
                    )

    # commodity_price_forecasts (scenario-indexed Map)
    for pv in source_db.get_parameter_value_items(
        entity_class_name="node", parameter_definition_name="commodity_price_forecasts"
    ):
        if pv["type"] != "map":
            continue
        node_name = pv["entity_byname"][0]
        alt = pv["alternative_name"]
        parsed = pv["parsed_value"]

        # Look up realization value from commodity_price
        indexes = list(parsed.indexes)
        values = list(parsed.values)
        base_items = source_db.get_parameter_value_items(
            entity_class_name="node",
            parameter_definition_name="commodity_price",
            entity_byname=(node_name,),
        )
        if base_items:
            base_val = base_items[0]["parsed_value"]
            indexes = ["realization"] + indexes
            values = [base_val] + values

        scenario_map = Map(
            indexes=indexes,
            values=values,
            index_name="stochastic_scenario",
        )
        for ntu in source_db.get_entity_items(entity_class_name="node__to_unit"):
            if ntu["entity_byname"][0] == node_name:
                unit_name = ntu["entity_byname"][1]
                try:
                    add_parameter_value(
                        target_db, "node__to_unit", "vom_cost",
                        alt, (node_name, unit_name), scenario_map,
                    )
                except RuntimeError:
                    db_val, val_type = api.to_database(scenario_map)
                    target_db.update_parameter_value_item(
                        entity_class_name="node__to_unit",
                        entity_byname=(node_name, unit_name),
                        parameter_definition_name="vom_cost",
                        alternative_name=alt,
                        value=db_val,
                        type=val_type,
                    )

    try:
        target_db.commit_session("Added commodity price")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit commodity price error:", e)


def process_link_bidirectional(source_db, target_db):
    """Set capacity on all connection__from_node and connection__to_node entities,
    and efficiency on both connection__node__node entities for each link."""
    periods_info = _get_periods_info(source_db)

    for pv in source_db.get_parameter_value_items(
        entity_class_name="node__link__node", parameter_definition_name="capacity"
    ):
        node1, link, node2 = pv["entity_byname"]
        alt = pv["alternative_name"]
        if pv["type"] == "map":
            value = _map_to_time_series(pv["parsed_value"], periods_info)
            if not value:
                continue
        elif pv["type"] == "float":
            value = pv["parsed_value"]
        else:
            continue
        for target_class, target_byname in [
            ("connection__from_node", (link, node1)),
            ("connection__from_node", (link, node2)),
            ("connection__to_node", (link, node1)),
            ("connection__to_node", (link, node2)),
        ]:
            try:
                add_parameter_value(
                    target_db, target_class, "capacity_per_connection",
                    alt, target_byname, value,
                )
            except RuntimeError:
                pass

    # Capacity from link entity
    for pv in source_db.get_parameter_value_items(
        entity_class_name="link", parameter_definition_name="capacity"
    ):
        link = pv["entity_byname"][0]
        alt = pv["alternative_name"]
        if pv["type"] == "map":
            value = _map_to_time_series(pv["parsed_value"], periods_info)
            if not value:
                continue
        elif pv["type"] == "float":
            value = pv["parsed_value"]
        else:
            continue
        for nln in source_db.get_entity_items(entity_class_name="node__link__node"):
            if nln["entity_byname"][1] == link:
                node1, _, node2 = nln["entity_byname"]
                for target_class, target_byname in [
                    ("connection__from_node", (link, node1)),
                    ("connection__from_node", (link, node2)),
                    ("connection__to_node", (link, node1)),
                    ("connection__to_node", (link, node2)),
                ]:
                    try:
                        add_parameter_value(
                            target_db, target_class, "capacity_per_connection",
                            alt, target_byname, value,
                        )
                    except RuntimeError:
                        pass
                break

    # Efficiency from node__link__node entity
    for pv in source_db.get_parameter_value_items(
        entity_class_name="node__link__node", parameter_definition_name="efficiency"
    ):
        node1, link, node2 = pv["entity_byname"]
        alt = pv["alternative_name"]
        if pv["type"] == "map":
            value = _map_to_time_series(pv["parsed_value"], periods_info)
            if not value:
                continue
        elif pv["type"] == "float":
            value = pv["parsed_value"]
        else:
            continue
        for target_byname in [(link, node2, node1), (link, node1, node2)]:
            try:
                add_parameter_value(
                    target_db, "connection__node__node",
                    "fix_ratio_out_in_connection_flow",
                    alt, target_byname, value,
                )
            except RuntimeError:
                pass

    # Efficiency from link entity
    for pv in source_db.get_parameter_value_items(
        entity_class_name="link", parameter_definition_name="efficiency"
    ):
        link = pv["entity_byname"][0]
        alt = pv["alternative_name"]
        if pv["type"] == "map":
            value = _map_to_time_series(pv["parsed_value"], periods_info)
            if not value:
                continue
        elif pv["type"] == "float":
            value = pv["parsed_value"]
        else:
            continue
        # Find associated nodes from node__link__node entities
        for nln in source_db.get_entity_items(entity_class_name="node__link__node"):
            if nln["entity_byname"][1] == link:
                node1, _, node2 = nln["entity_byname"]
                for target_byname in [(link, node2, node1), (link, node1, node2)]:
                    try:
                        add_parameter_value(
                            target_db, "connection__node__node",
                            "fix_ratio_out_in_connection_flow",
                            alt, target_byname, value,
                        )
                    except RuntimeError:
                        pass
                break

    try:
        target_db.commit_session("Added bidirectional link capacity and efficiency")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("commit bidirectional link error:", e)


if __name__ == "__main__":
    main()
