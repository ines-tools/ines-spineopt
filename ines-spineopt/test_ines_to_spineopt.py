"""
Test script for ines_to_spineopt.py transformation.
Creates source (ines-spec) and target (spineopt) databases from templates,
populates source with test data covering all transformed parameters,
runs the transformation, and verifies the expected output parameters exist.
"""

import json
import os
import sys
import traceback

import spinedb_api as api
from spinedb_api import DatabaseMapping, import_data


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)

INES_TEMPLATE = os.path.join(PARENT_DIR, "ines-spec-template.json")
SPINEOPT_TEMPLATE = os.path.join(PARENT_DIR, "spineopt_template.json")

SOURCE_DB = os.path.join(SCRIPT_DIR, "test_ines_spec.sqlite")
TARGET_DB = os.path.join(SCRIPT_DIR, "test_spineopt.sqlite")

SOURCE_URL = f"sqlite:///{SOURCE_DB}"
TARGET_URL = f"sqlite:///{TARGET_DB}"


def create_db_from_template(db_url, template_path):
    """Create a Spine database from a JSON template."""
    db_file = db_url.replace("sqlite:///", "")
    if os.path.exists(db_file):
        os.remove(db_file)

    with open(template_path, "r") as f:
        template = json.load(f)

    db = DatabaseMapping(db_url, create=True)
    import_data(db, **template)
    db.commit_session("Imported template")
    db.close()


def populate_source_db():
    """Populate the source INES database with test data covering all transformed parameters."""
    with DatabaseMapping(SOURCE_URL) as db:
        db.add_alternative_item(name="Base")
        db.add_alternative_item(name="cumul_alt")
        db.add_scenario_item(name="base")
        db.add_scenario_alternative_item(
            scenario_name="base", alternative_name="Base", rank=1
        )
        db.add_scenario_alternative_item(
            scenario_name="base", alternative_name="cumul_alt", rank=2
        )
        db.commit_session("Added alternatives and scenarios")

        def add_val(cls, param, entity_byname, value, alt="Base"):
            db_val, val_type = api.to_database(value)
            _, err = db.add_parameter_value_item(
                entity_class_name=cls,
                entity_byname=entity_byname,
                parameter_definition_name=param,
                alternative_name=alt,
                value=db_val,
                type=val_type,
            )
            if err:
                print(f"  WARN add_val {cls}.{param} {entity_byname}: {err}")

        def add_ent(cls, byname):
            _, err = db.add_entity_item(
                entity_class_name=cls, entity_byname=byname
            )
            if err:
                print(f"  WARN add_ent {cls} {byname}: {err}")

        # === ENTITIES ===
        add_ent("solve_pattern", ("sp1",))
        add_ent("period", ("p2025",))
        add_ent("period", ("p2030",))
        add_ent("system", ("sys1",))
        add_ent("node", ("elec_node",))
        add_ent("node", ("gas_node",))
        add_ent("node", ("storage_node",))
        add_ent("node", ("demand_node",))
        add_ent("unit", ("gas_plant",))
        add_ent("unit", ("wind_farm",))
        add_ent("unit", ("invest_unit",))
        add_ent("link", ("power_line",))
        add_ent("constraint", ("co2_limit",))
        add_ent("node__to_unit", ("gas_node", "gas_plant"))
        add_ent("node__to_unit", ("elec_node", "wind_farm"))
        add_ent("unit__to_node", ("gas_plant", "elec_node"))
        add_ent("unit__to_node", ("wind_farm", "elec_node"))
        add_ent("node__link__node", ("elec_node", "power_line", "gas_node"))
        add_ent("node__node", ("elec_node", "gas_node"))
        add_ent("set", ("co2_set",))
        add_ent("set", ("invest_group",))
        add_ent("set__unit", ("invest_group", "invest_unit"))
        # flow_max_instant / flow_min_instant test entities
        add_ent("set", ("max_flow_group",))
        add_ent("set__unit_flow", ("max_flow_group", "gas_plant", "elec_node"))
        add_ent("set__unit_flow", ("max_flow_group", "gas_node", "gas_plant"))
        # Efficiency test entities
        add_ent("unit", ("eff_unit",))
        add_ent("node", ("fuel_node",))
        add_ent("node__to_unit", ("fuel_node", "eff_unit"))
        add_ent("unit__to_node", ("eff_unit", "elec_node"))
        add_ent("unit", ("pw_unit",))
        add_ent("node__to_unit", ("fuel_node", "pw_unit"))
        add_ent("unit__to_node", ("pw_unit", "elec_node"))
        add_ent(
            "unit_flow__unit_flow",
            ("gas_node", "gas_plant", "gas_plant", "elec_node"),
        )
        # profile_limit_upper time_series test entity
        add_ent("unit", ("ts_profile_unit",))
        add_ent("unit__to_node", ("ts_profile_unit", "elec_node"))
        # Reserve test entities
        add_ent("reserve", ("up_reserve",))
        add_ent("reserve", ("sym_reserve",))
        add_ent("node__reserve", ("elec_node", "up_reserve"))
        add_ent("node__reserve", ("elec_node", "sym_reserve"))
        add_ent("unit__node__reserve", ("gas_plant", "elec_node", "up_reserve"))
        add_ent("unit__node__reserve", ("gas_plant", "elec_node", "sym_reserve"))
        # Stochastic test entities
        add_ent("set", ("stoch_set",))
        add_ent("set__node", ("stoch_set", "elec_node"))
        add_ent("set__unit", ("stoch_set", "gas_plant"))
        add_ent("set__link", ("stoch_set", "power_line"))
        db.commit_session("Added entities")

        # === SOLVE PATTERN / PERIOD PARAMETERS ===
        add_val("solve_pattern", "period", ("sp1",), {"type": "array", "data": ["p2025", "p2030"]})
        add_val("solve_pattern", "duration", ("sp1",), {"type": "duration", "data": "8760h"})
        add_val("solve_pattern", "start_time", ("sp1",), {
            "type": "array", "data": ["2025-01-01T00:00:00", "2025-06-01T00:00:00"],
        })
        add_val("solve_pattern", "time_resolution", ("sp1",), {"type": "duration", "data": "1h"})
        add_val("solve_pattern", "rolling_jump", ("sp1",), {"type": "duration", "data": "168h"})
        add_val("solve_pattern", "rolling_horizon", ("sp1",), {"type": "duration", "data": "336h"})
        add_val("solve_pattern", "time_resolution_scope", ("sp1",), "set_based_override")
        add_val("set", "time_resolution", ("stoch_set",), {"type": "duration", "data": "4h"})
        add_val("period", "start_time", ("p2025",), {"type": "date_time", "data": "2025-01-01T00:00:00"})
        add_val("period", "years_represented", ("p2025",), 5.0)
        add_val("period", "start_time", ("p2030",), {"type": "date_time", "data": "2030-01-01T00:00:00"})
        add_val("period", "years_represented", ("p2030",), 5.0)
        add_val("system", "discount_rate", ("sys1",), 0.05)

        # === YAML-DRIVEN transform_parameters ===
        add_val("node__to_unit", "capacity", ("gas_node", "gas_plant"), 500.0)
        add_val("node__to_unit", "ramp_limit_up", ("gas_node", "gas_plant"), 0.5)
        add_val("node__to_unit", "ramp_limit_down", ("gas_node", "gas_plant"), 0.3)
        add_val("unit__to_node", "capacity", ("gas_plant", "elec_node"), 400.0)
        add_val("unit__to_node", "ramp_limit_up", ("gas_plant", "elec_node"), 0.4)
        add_val("unit__to_node", "ramp_limit_down", ("gas_plant", "elec_node"), 0.25)
        add_val("unit__to_node", "flow_min_cumulative", ("gas_plant", "elec_node"), 500.0)
        add_val("unit", "startup_cost", ("gas_plant",), 5000.0)
        add_val("unit", "shutdown_cost", ("gas_plant",), 1000.0)
        add_val("unit", "online_cost", ("gas_plant",), 50.0)
        add_val("unit", "min_uptime", ("gas_plant",), 4.0)
        add_val("unit", "min_downtime", ("gas_plant",), 2.0)
        add_val("unit", "interest_rate", ("gas_plant",), 0.08)
        add_val("node", "storage_capacity", ("storage_node",), 1000.0)
        add_val("node", "storage_interest_rate", ("storage_node",), 0.06)
        add_val("node", "storage_loss_from_stored_energy", ("storage_node",), 0.001)
        add_val("node", "penalty_upward", ("elec_node",), 10000.0)
        add_val("link", "interest_rate", ("power_line",), 0.07)
        add_val("node__node", "diffusion_coefficient", ("elec_node", "gas_node"), 0.1)
        add_val("constraint", "constant", ("co2_limit",), 100000.0)

        # === YAML-DRIVEN process_methods ===
        add_val("node", "node_type", ("gas_node",), "commodity")
        add_val("node", "node_type", ("storage_node",), "storage")
        add_val("node", "node_type", ("demand_node",), "balance")
        add_val("node", "storage_investment_method", ("storage_node",), "no_limits")
        add_val("unit", "investment_method", ("invest_unit",), "no_limits")
        add_val("unit", "investment_uses_integer", ("invest_unit",), True)
        add_val("unit", "startup_method", ("gas_plant",), "integer")
        add_val("link", "investment_method", ("power_line",), "no_limits")
        add_val("link", "investment_uses_integer", ("power_line",), True)
        add_val("node", "storage_investment_uses_integer", ("storage_node",), True)
        add_val("link", "transfer_method", ("power_line",), "regular_linear")
        add_val("constraint", "sense", ("co2_limit",), "less_than")

        # === map_of_periods_or_historical_to_ts (float) ===
        add_val("unit", "availability", ("gas_plant",), 0.95)
        add_val("unit__to_node", "profile_limit_upper", ("gas_plant", "elec_node"), 0.9)
        add_val("unit__to_node", "investment_cost", ("gas_plant", "elec_node"), 100000.0)
        add_val("unit__to_node", "salvage_value", ("gas_plant", "elec_node"), 5000.0)
        add_val("unit__to_node", "fixed_cost", ("gas_plant", "elec_node"), 50000.0)
        add_val("node__to_unit", "fixed_cost", ("elec_node", "wind_farm"), 30000.0)
        add_val("link", "investment_cost", ("power_line",), 200000.0)
        add_val("link", "availability", ("power_line",), 0.98)
        add_val("node__link__node", "capacity", ("elec_node", "power_line", "gas_node"), 300.0)
        add_val("node__link__node", "efficiency", ("elec_node", "power_line", "gas_node"), 0.97)
        add_val("node__link__node", "operational_cost", ("elec_node", "power_line", "gas_node"), 2.5)
        add_val("node", "commodity_price", ("gas_node",), 25.0)
        add_val("node", "storage_investment_cost", ("storage_node",), 50000.0)
        add_val("node", "storage_salvage_value", ("storage_node",), 3000.0)
        add_val("node", "storage_fixed_cost", ("storage_node",), 10000.0)

        # === profile_limit_upper as time_series with float availability ===
        add_val("unit", "availability", ("ts_profile_unit",), 0.8)
        ts_profile = {"type": "time_series", "data": {"2025-01-01T00:00:00": 0.5, "2025-01-01T01:00:00": 0.7}}
        add_val("unit__to_node", "profile_limit_upper", ("ts_profile_unit", "elec_node"), ts_profile)

        # === map_of_periods_or_historical_to_ts (map) ===
        period_map_val = {"type": "map", "index_type": "str", "data": {"p2025": 0.9, "p2030": 0.85}}
        add_val("unit", "availability", ("wind_farm",), period_map_val)
        add_val("unit__to_node", "other_operational_cost", ("wind_farm", "elec_node"), period_map_val)
        add_val("node", "flow_annual", ("elec_node",), period_map_val)

        # === flow_profile_method ===
        add_val("node", "flow_scaling_method", ("elec_node",), "use_profile_directly")
        add_val("node", "flow_profile", ("elec_node",), 100.0)
        profile_forecasts = {"type": "map", "index_type": "str", "data": {"high": 120.0, "low": 80.0}}
        add_val("node", "flow_profile_forecasts", ("elec_node",), profile_forecasts)

        # === flow_profile_method: scale_to_annual ===
        add_val("node", "flow_scaling_method", ("demand_node",), "scale_to_annual")
        add_val("node", "flow_profile", ("demand_node",), -50.0)
        add_val("node", "flow_annual", ("demand_node",), 1000.0)

        # === limiting_investments_notallowed ===
        add_ent("unit", ("existing_unit",))
        add_val("unit", "investment_method", ("existing_unit",), "not_allowed")
        add_val("unit", "units_existing", ("existing_unit",), 3.0)
        add_val("unit", "retirement_method", ("existing_unit",), "not_retired")
        add_ent("unit__to_node", ("existing_unit", "elec_node"))

        # === process_emissions ===
        add_val("set", "co2_max_cumulative", ("co2_set",), 500000.0)
        add_val("node", "co2_content", ("gas_node",), 0.2)

        # === process_emissions: CO2 price ===
        add_val("set", "co2_price", ("co2_set",), 30.0)

        # === process_emissions: SO2 ===
        add_ent("set", ("so2_set",))
        add_val("set", "so2_max_cumulative", ("so2_set",), 10000.0)
        add_val("set", "so2_price", ("so2_set",), 5.0)
        add_val("node__to_unit", "so2_emission_rate", ("gas_node", "gas_plant"), 0.05)

        # === process_emissions: NOx ===
        add_ent("set", ("nox_set",))
        add_val("set", "nox_max_cumulative", ("nox_set",), 20000.0)
        add_val("set", "nox_price", ("nox_set",), 8.0)
        add_val("unit__to_node", "nox_emission_rate", ("gas_plant", "elec_node"), 0.03)

        # === storage_state_fix_method & binding ===
        add_val("node", "storage_state_fix_method", ("storage_node",), "fix_start")
        add_val("node", "storage_state_fix", ("storage_node",), 500.0)
        add_val("node", "storage_state_binding_method", ("storage_node",), "leap_over_within_period")

        # === set_to_entities_and_parameters ===
        add_val("set", "max_cumulative", ("invest_group",), 5.0)
        add_ent("set", ("invest_cap_group",))
        add_ent("set__unit", ("invest_cap_group", "gas_plant"))
        add_val("set", "invest_max_total", ("invest_cap_group",), 1000.0)

        # === flow_max_instant ===
        add_val("set", "flow_max_instant", ("max_flow_group",), 500.0)

        # === existing_capacity ===
        add_val("unit", "units_existing", ("gas_plant",), 2.0)
        add_val("link", "links_existing", ("power_line",), 1.0)
        add_val("node", "storages_existing", ("storage_node",), 1.0)

        # === process_invest_period (period path, Base alt) ===
        invest_period_map = {"type": "map", "index_type": "str", "data": {"p2025": 3.0, "p2030": 5.0}}
        add_val("unit", "units_invest_max_period", ("gas_plant",), invest_period_map)
        add_val("link", "links_invest_max_period", ("power_line",), invest_period_map)
        add_val("node", "storages_invest_fix_period", ("storage_node",), invest_period_map)

        # === process_invest_period (cumulative path, cumul_alt) ===
        cumul_map = {"type": "map", "index_type": "str", "data": {"p2025": 5.0, "p2030": 7.0}}
        add_val("unit", "units_max_cumulative", ("gas_plant",), cumul_map, alt="cumul_alt")
        add_val("link", "links_fix_cumulative", ("power_line",), cumul_map, alt="cumul_alt")
        add_val("node", "storages_max_cumulative", ("storage_node",), cumul_map, alt="cumul_alt")

        # === lifetime_to_duration ===
        add_val("unit", "lifetime", ("gas_plant",), 30.0)
        add_val("link", "lifetime", ("power_line",), 40.0)
        add_val("node", "storage_lifetime", ("storage_node",), 25.0)

        # === unit_flow_variants ===
        add_val("unit_flow__unit_flow", "equality_ratio",
                ("gas_node", "gas_plant", "gas_plant", "elec_node"), 0.45)

        # === process_conversion_coefficients ===
        add_val("unit__to_node", "conversion_coefficient", ("gas_plant", "elec_node"), 1.0)
        add_val("node__to_unit", "conversion_coefficient", ("gas_node", "gas_plant"), 2.5)

        # === process_efficiency: constant_efficiency ===
        add_val("unit", "efficiency", ("eff_unit",), 0.45)
        add_val("unit", "conversion_method", ("eff_unit",), "constant_efficiency")

        # === process_efficiency: partial_load_efficiency (piecewise) ===
        pw_eff_map = {"type": "map", "index_type": "float", "data": {"0.3": 0.35, "1.0": 0.42}}
        add_val("unit", "efficiency", ("pw_unit",), pw_eff_map)
        add_val("unit", "conversion_method", ("pw_unit",), "partial_load_efficiency")

        # === process_constraints ===
        constraint_coeff_map = {"type": "map", "index_type": "str", "data": {"co2_limit": 1.5}}
        add_val("unit__to_node", "constraint_flow_coefficient", ("gas_plant", "elec_node"), constraint_coeff_map)
        add_val("node__to_unit", "constraint_flow_coefficient", ("gas_node", "gas_plant"), constraint_coeff_map)
        add_val("unit", "constraint_unit_count_coefficient", ("gas_plant",), constraint_coeff_map)
        add_val("unit", "constraint_online_coefficient", ("gas_plant",), constraint_coeff_map)
        add_val("node", "constraint_storage_count_coefficient", ("storage_node",), constraint_coeff_map)
        add_val("node", "constraint_storage_state_coefficient", ("storage_node",), constraint_coeff_map)
        add_val("link", "constraint_link_count_coefficient", ("power_line",), constraint_coeff_map)
        add_val("node__link__node", "constraint_flow_coefficient", ("elec_node", "power_line", "gas_node"), constraint_coeff_map)

        # === process_reserves ===
        add_val("reserve", "reserve_type", ("up_reserve",), "upward")
        add_val("node__reserve", "reserve_requirement", ("elec_node", "up_reserve"), 20.0)
        add_val("unit__node__reserve", "reservation_cost", ("gas_plant", "elec_node", "up_reserve"), 5.0)
        add_val("unit__node__reserve", "max_reserve_provision", ("gas_plant", "elec_node", "up_reserve"), 0.5)

        add_val("reserve", "reserve_type", ("sym_reserve",), "symmetric")
        add_val("node__reserve", "reserve_requirement", ("elec_node", "sym_reserve"), 15.0)
        add_val("unit__node__reserve", "reservation_cost", ("gas_plant", "elec_node", "sym_reserve"), 3.0)

        # === process_stochastic_structure ===
        add_val("solve_pattern", "stochastic_scope", ("sp1",), "whole_model")
        add_val("set", "stochastic_method", ("stoch_set",), "interpolate_time_series_forecasts")
        forecast_weights = {"type": "map", "index_type": "str", "data": {"high": 0.3, "low": 0.7}}
        add_val("set", "stochastic_forecast_weights", ("stoch_set",), forecast_weights)

        # === process_forecasts ===
        # Base (realization) values for forecast parameters
        add_val("node", "commodity_price", ("elec_node",), 40.0)
        add_val("unit__to_node", "other_operational_cost", ("gas_plant", "elec_node"), 7.0)
        price_forecasts = {"type": "map", "index_type": "str", "data": {"high": 50.0, "low": 30.0}}
        add_val("node", "commodity_price_forecasts", ("elec_node",), price_forecasts)
        cost_forecasts = {"type": "map", "index_type": "str", "data": {"high": 10.0, "low": 5.0}}
        add_val("unit__to_node", "other_operational_cost_forecasts", ("gas_plant", "elec_node"), cost_forecasts)
        # time series forecast as 2D map (1st index: scenario name, 2nd index: datetime)
        map_high = {"type": "map", "index_type": "date_time", "data": [
            ["2025-01-01T00:00:00", 100.0], ["2025-01-01T01:00:00", 120.0]]}
        map_low = {"type": "map", "index_type": "date_time", "data": [
            ["2025-01-01T00:00:00", 80.0], ["2025-01-01T01:00:00", 90.0]]}
        ts_forecasts = {"type": "map", "index_type": "str", "data": [["high", map_high], ["low", map_low]]}
        add_val("node", "storage_state_fix_forecasts", ("storage_node",), ts_forecasts)

        db.commit_session("Added all test parameter values")
        print("Source database populated successfully.")


def run_transformation():
    """Run ines_to_spineopt.py with the test databases."""
    import subprocess
    script = os.path.join(SCRIPT_DIR, "ines_to_spineopt.py")
    result = subprocess.run(
        [sys.executable, script, SOURCE_URL, TARGET_URL],
        capture_output=True, text=True, cwd=SCRIPT_DIR,
    )
    print("\n=== TRANSFORMATION STDOUT ===")
    print(result.stdout)
    if result.stderr:
        print("\n=== TRANSFORMATION STDERR ===")
        print(result.stderr)
    print(f"\n=== Return code: {result.returncode} ===")
    return result.returncode == 0


def verify_results():
    """Verify that expected parameters exist in the target database."""

    expected = [
        # === From transform_parameters (YAML) ===
        ("node__to_unit", "ramp_limits_up", ("gas_node", "gas_plant"), "ramp_limit_up via YAML"),
        ("node__to_unit", "ramp_limits_down", ("gas_node", "gas_plant"), "ramp_limit_down via YAML"),
        ("unit__to_node", "capacity_per_unit", ("gas_plant", "elec_node"), "capacity via YAML"),
        ("unit__to_node", "ramp_limits_up", ("gas_plant", "elec_node"), "ramp_limit_up via YAML"),
        ("unit__to_node", "ramp_limits_down", ("gas_plant", "elec_node"), "ramp_limit_down via YAML"),
        ("unit__to_node", "flow_limits_min_cumulative", ("gas_plant", "elec_node"), "flow_min_cumulative via YAML"),
        ("unit", "start_up_cost", ("gas_plant",), "startup_cost via YAML"),
        ("unit", "shut_down_cost", ("gas_plant",), "shutdown_cost via YAML"),
        ("unit", "units_on_cost", ("gas_plant",), "online_cost via YAML"),
        ("unit", "min_up_time", ("gas_plant",), "min_uptime via YAML"),
        ("unit", "min_down_time", ("gas_plant",), "min_downtime via YAML"),
        ("unit", "discount_rate_technology_specific", ("gas_plant",), "interest_rate via YAML"),
        ("node", "storage_state_max", ("storage_node",), "storage_capacity via YAML"),
        ("node", "storage_discount_rate_technology_specific", ("storage_node",), "storage_interest_rate via YAML"),
        ("node", "storage_self_discharge", ("storage_node",), "storage_loss via YAML"),
        ("node", "balance_penalty", ("elec_node",), "penalty_upward via YAML"),
        ("connection", "discount_rate_technology_specific", ("power_line",), "link interest_rate via YAML"),
        ("node__node", "diffusion_coefficient", ("elec_node", "gas_node"), "diffusion via YAML"),
        ("user_constraint", "right_hand_side", ("co2_limit",), "constant via YAML"),

        # === From process_methods (YAML) ===
        ("node", "balance_type", ("gas_node",), "commodity balance_type"),
        ("node", "balance_type", ("demand_node",), "balance node_balance"),
        ("node", "storage_investment_variable_type", ("storage_node",), "storage inv type"),
        ("node", "storage_active", ("storage_node",), "storage_active"),
        ("node", "storage_investment_count_max_cumulative", ("storage_node",), "storage inv no_limits"),
        ("unit", "investment_count_max_cumulative", ("invest_unit",), "unit inv no_limits"),
        ("unit", "online_variable_type", ("gas_plant",), "startup integer"),
        ("connection", "investment_count_max_cumulative", ("power_line",), "link inv no_limits"),
        ("connection", "connection_type", ("power_line",), "transfer regular"),
        ("user_constraint", "constraint_sense", ("co2_limit",), "sense less_than"),

        # === From timeline_setup ===
        ("model", "model_start", None, "timeline model_start"),
        ("model", "model_end", None, "timeline model_end"),
        ("temporal_block", "resolution", None, "timeline resolution"),

        # === From map_of_periods_or_historical_to_ts ===
        ("unit", "unit_investment_cost", ("gas_plant",), "investment_cost"),
        ("unit", "unit_decommissioning_cost", ("gas_plant",), "salvage_value * -1"),
        ("unit", "fom_cost", ("gas_plant",), "fom_cost from unit__to_node"),
        ("unit", "fom_cost", ("wind_farm",), "fom_cost from node__to_unit"),
        ("connection", "connection_investment_cost", ("power_line",), "link inv_cost"),
        ("connection", "availability_factor", ("power_line",), "link availability"),
        ("connection__from_node", "connection_flow_cost", None, "link op_cost"),
        ("connection__from_node", "capacity_per_connection", ("power_line", "elec_node"), "link capacity from_node1"),
        ("connection__from_node", "capacity_per_connection", ("power_line", "gas_node"), "link capacity from_node2"),
        ("connection__to_node", "capacity_per_connection", ("power_line", "elec_node"), "link capacity to_node1"),
        ("connection__to_node", "capacity_per_connection", ("power_line", "gas_node"), "link capacity to_node2"),
        ("connection__node__node", "fix_ratio_out_in_connection_flow", ("power_line", "gas_node", "elec_node"), "link efficiency dir1"),
        ("connection__node__node", "fix_ratio_out_in_connection_flow", ("power_line", "elec_node", "gas_node"), "link efficiency dir2"),
        ("node__to_unit", "vom_cost", ("gas_node", "gas_plant"), "commodity_price"),
        ("node", "storage_investment_cost", ("storage_node",), "storage_inv_cost"),
        ("node", "storage_decommissioning_cost", ("storage_node",), "storage_salvage * -1"),
        ("node", "storage_fixed_annual_cost", ("storage_node",), "storage_fixed_cost"),
        ("unit__to_node", "vom_cost", ("wind_farm", "elec_node"), "other_op_cost"),

        # === From flow_profile_method ===
        ("node", "demand", ("elec_node",), "flow_profile use_profile_directly"),
        ("node", "demand", ("demand_node",), "flow_profile scale_to_annual"),

        # === From process_availability ===
        ("unit", "availability_factor", ("gas_plant",), "avail * profile_limit_upper"),
        ("unit", "availability_factor", ("wind_farm",), "availability map only"),
        ("unit", "availability_factor", ("ts_profile_unit",), "avail float * profile_limit_upper ts"),

        # === From process_investment_integer (linear default + integer override) ===
        ("unit", "investment_variable_type", ("invest_unit",), "unit integer investment"),
        ("unit", "investment_variable_type", ("gas_plant",), "unit linear investment (has inv_cost)"),
        ("unit", "investment_variable_type", ("existing_unit",), "unit linear investment (has inv_method)"),
        ("connection", "investment_variable_type", ("power_line",), "conn integer investment"),
        ("node", "storage_investment_variable_type", ("storage_node",), "node integer storage investment"),
        ("model", "discount_rate", None, "system discount_rate"),

        # === From process_invest_period (period path, Base) ===
        ("unit", "investment_count_max_cumulative", ("gas_plant",), "unit invest_max_period"),
        ("connection", "investment_count_max_cumulative", ("power_line",), "link invest_max_period"),
        ("node", "storage_investment_count_fix_cumulative", ("storage_node",), "storage invest_fix_period"),

        # === From process_invest_period (cumulative path, cumul_alt) ===
        ("unit", "investment_count_max_cumulative", ("gas_plant",), "unit cumul max_cumulative"),
        ("connection", "investment_count_fix_cumulative", ("power_line",), "link cumul fix_cumulative"),
        ("node", "storage_investment_count_max_cumulative", ("storage_node",), "storage cumul max_cumulative"),

        # === From candidates_to_number_of ===
        ("unit", "existing_units", ("invest_unit",), "candidate existing=0"),
        ("connection", "existing_connections", ("power_line",), "candidate existing=0"),
        ("node", "existing_storages", ("storage_node",), "candidate existing=0"),

        # === From existing_capacity ===
        ("unit", "existing_units", ("gas_plant",), "units_existing"),
        ("connection", "existing_connections", ("power_line",), "links_existing"),
        ("node", "existing_storages", ("storage_node",), "storages_existing"),

        # === From lifetime_to_duration ===
        ("unit", "lifetime_economic", ("gas_plant",), "lifetime economic"),
        ("unit", "lifetime_technical", ("gas_plant",), "lifetime technical"),
        ("connection", "lifetime_economic", ("power_line",), "link lifetime economic"),
        ("connection", "lifetime_technical", ("power_line",), "link lifetime technical"),
        ("node", "storage_lifetime_economic", ("storage_node",), "storage lifetime economic"),
        ("node", "storage_lifetime_technical", ("storage_node",), "storage lifetime technical"),

        # === From unit_flow_variants ===
        ("unit_flow__unit_flow", "flow_ratio_equality_coefficient",
         ("gas_node", "gas_plant", "gas_plant", "elec_node"), "equality_ratio"),

        # === From process_conversion_coefficients ===
        ("unit_flow__unit_flow", "flow_ratio_equality_coefficient",
         ("gas_plant", "elec_node", "gas_node", "gas_plant"), "conversion_coeff"),

        # === From process_efficiency: constant_efficiency ===
        ("unit_flow__unit_flow", "flow_ratio_equality_coefficient",
         ("eff_unit", "elec_node", "fuel_node", "eff_unit"), "constant efficiency ratio"),

        # === From process_efficiency: partial_load_efficiency ===
        ("unit__to_node", "operating_points",
         ("pw_unit", "elec_node"), "piecewise operating_points"),
        ("unit__to_node", "minimum_operating_point",
         ("pw_unit", "elec_node"), "piecewise min_operating_point"),
        ("unit_flow__unit_flow", "flow_ratio_equality_coefficient",
         ("fuel_node", "pw_unit", "pw_unit", "elec_node"), "piecewise efficiency ratio"),

        # === From process_emissions: CO2 ===
        ("node", "storage_active", ("atmosphere",), "atmosphere storage_active"),
        ("node", "storage_state_max", ("atmosphere",), "atmosphere storage_state_max"),
        ("node", "tax_in_unit_flow", ("atmosphere",), "co2_price"),
        ("unit_flow__unit_flow", "flow_ratio_equality_coefficient",
         ("gas_plant", "atmosphere", "gas_node", "gas_plant"), "co2 emission flow ratio"),

        # === From process_emissions: SO2 ===
        ("node", "storage_active", ("so2_emissions",), "so2 storage_active"),
        ("node", "storage_state_max", ("so2_emissions",), "so2 storage_state_max"),
        ("node", "tax_in_unit_flow", ("so2_emissions",), "so2_price"),
        ("unit_flow__unit_flow", "flow_ratio_equality_coefficient",
         ("gas_plant", "so2_emissions", "gas_node", "gas_plant"), "so2 emission flow ratio"),

        # === From process_emissions: NOx ===
        ("node", "storage_active", ("nox_emissions",), "nox storage_active"),
        ("node", "storage_state_max", ("nox_emissions",), "nox storage_state_max"),
        ("node", "tax_in_unit_flow", ("nox_emissions",), "nox_price"),
        ("unit_flow__unit_flow", "flow_ratio_equality_coefficient",
         ("gas_plant", "nox_emissions", "gas_plant", "elec_node"), "nox emission flow ratio"),

        # === From storage_state_fix_method ===
        ("node", "storage_state_fix", ("storage_node",), "storage_state_fix"),

        # === From storage_state_binding_method ===
        ("node__temporal_block", "cyclic_condition", None, "binding cyclic_condition"),

        # === From set_to_entities_and_parameters ===
        ("investment_group", "investment_count_total_max_cumulative", ("invest_group",), "max_cumulative"),
        ("investment_group", "investment_capacity_total_max_cumulative", ("invest_cap_group",), "invest_max_total"),

        # === From flow_max_instant ===
        ("user_constraint", "right_hand_side", ("max_flow_group",), "flow_max_instant rhs"),
        ("user_constraint", "constraint_sense", ("max_flow_group",), "flow_max_instant sense"),
        ("unit_flow__user_constraint", "coefficient_for_unit_flow",
         ("gas_plant", "elec_node", "max_flow_group"), "flow_max_instant unit__to_node member"),
        ("unit_flow__user_constraint", "coefficient_for_unit_flow",
         ("gas_node", "gas_plant", "max_flow_group"), "flow_max_instant node__to_unit member"),

        # === From process_constraints ===
        ("unit_flow__user_constraint", "coefficient_for_unit_flow",
         ("gas_plant", "elec_node", "co2_limit"), "constraint flow output"),
        ("unit_flow__user_constraint", "coefficient_for_unit_flow",
         ("gas_node", "gas_plant", "co2_limit"), "constraint flow input"),
        ("unit__user_constraint", "coefficient_for_units_invested_available",
         ("gas_plant", "co2_limit"), "constraint unit count"),
        ("unit__user_constraint", "coefficient_for_units_on",
         ("gas_plant", "co2_limit"), "constraint online"),
        ("node__user_constraint", "coefficient_for_storages_invested_available",
         ("storage_node", "co2_limit"), "constraint storage count"),
        ("node__user_constraint", "coefficient_for_node_state",
         ("storage_node", "co2_limit"), "constraint storage state"),
        ("connection__user_constraint", "coefficient_for_connections_invested_available",
         ("power_line", "co2_limit"), "constraint link count"),
        ("connection__to_node__user_constraint", "coefficient_for_connection_flow",
         ("power_line", "gas_node", "co2_limit"), "constraint link flow"),

        # === From process_reserves ===
        ("node", "reserve_active", ("up_reserve",), "reserve_active flag"),
        ("node", "reserve_upward", ("up_reserve",), "reserve_upward flag"),
        ("node", "balance_sense", ("up_reserve",), "reserve balance_sense"),
        ("node", "balance_type", ("up_reserve",), "reserve balance_type"),
        ("node", "demand", ("up_reserve",), "reserve demand from requirement"),
        ("unit__to_node", "reserve_procurement_cost",
         ("gas_plant", "up_reserve"), "reserve procurement cost"),
        ("unit__to_node", "capacity_per_unit",
         ("gas_plant", "up_reserve"), "reserve capacity from max_provision * capacity"),
        # symmetric reserve creates two nodes
        ("node", "reserve_active", ("sym_reserve_up",), "sym reserve_active up"),
        ("node", "reserve_upward", ("sym_reserve_up",), "sym reserve_upward"),
        ("node", "reserve_active", ("sym_reserve_down",), "sym reserve_active down"),
        ("node", "reserve_downward", ("sym_reserve_down",), "sym reserve_downward"),
        ("node", "demand", ("sym_reserve_up",), "sym reserve demand up"),
        ("node", "demand", ("sym_reserve_down",), "sym reserve demand down"),
        ("unit__to_node", "reserve_procurement_cost",
         ("gas_plant", "sym_reserve_up"), "sym reserve cost up"),
        ("unit__to_node", "reserve_procurement_cost",
         ("gas_plant", "sym_reserve_down"), "sym reserve cost down"),

        # === From process_stochastic_structure ===
        ("stochastic_structure__stochastic_scenario", "weight_relative_to_parents",
         ("stochastic", "high"), "stochastic weight high"),
        ("stochastic_structure__stochastic_scenario", "weight_relative_to_parents",
         ("stochastic", "low"), "stochastic weight low"),

        # === From process_forecasts ===
        ("node__to_unit", "vom_cost", ("elec_node", "wind_farm"), "commodity_price_forecasts scenario map"),
        ("unit__to_node", "vom_cost", ("gas_plant", "elec_node"), "operational_cost_forecasts scenario map"),
        ("node", "storage_state_fix", ("storage_node",), "storage_state_fix_forecasts ts scenario map"),

        # === From rolling_jump / rolling_horizon ===
        ("model", "roll_forward", ("sp1",), "rolling_jump to roll_forward"),
        ("model", "window_duration", ("sp1",), "rolling_horizon to window_duration"),
        ("temporal_block", "block_start", ("sp1_tb0",), "tb0 block_start"),
        ("temporal_block", "block_end", ("sp1_tb0",), "tb0 block_end"),
        ("temporal_block", "block_start", ("sp1_tb1",), "tb1 block_start"),
        ("temporal_block", "block_end", ("sp1_tb1",), "tb1 block_end"),
        ("temporal_block", "resolution", ("sp1_investments",), "investment temporal block resolution"),

        # === Set-based temporal block override ===
        ("temporal_block", "resolution", ("stoch_set_sp1_tb0",), "set override tb0 resolution"),
        ("temporal_block", "block_start", ("stoch_set_sp1_tb0",), "set override tb0 block_start"),
        ("temporal_block", "block_end", ("stoch_set_sp1_tb0",), "set override tb0 block_end"),
        ("temporal_block", "resolution", ("stoch_set_sp1_tb1",), "set override tb1 resolution"),
        ("temporal_block", "block_start", ("stoch_set_sp1_tb1",), "set override tb1 block_start"),
        ("temporal_block", "block_end", ("stoch_set_sp1_tb1",), "set override tb1 block_end"),
        ("temporal_block", "resolution", ("stoch_set_sp1_investments",), "set override inv tb resolution"),
        ("node__temporal_block", None, ("elec_node", "stoch_set_sp1_tb0"), "set node temporal_block tb0"),
        ("node__temporal_block", None, ("elec_node", "stoch_set_sp1_tb1"), "set node temporal_block tb1"),
        ("units_on__temporal_block", None, ("gas_plant", "stoch_set_sp1_tb0"), "set unit temporal_block tb0"),
        ("units_on__temporal_block", None, ("gas_plant", "stoch_set_sp1_tb1"), "set unit temporal_block tb1"),
        ("node__investment_temporal_block", None, ("elec_node", "stoch_set_sp1_investments"), "set node inv temporal_block"),
        ("unit__investment_temporal_block", None, ("gas_plant", "stoch_set_sp1_investments"), "set unit inv temporal_block"),
        ("connection__investment_temporal_block", None, ("power_line", "stoch_set_sp1_investments"), "set conn inv temporal_block"),

        # === Node penalty defaults ===
        ("node", "balance_penalty", ("storage_node",), "default node penalty on storage_node"),
        ("node", "balance_penalty", ("fuel_node",), "default node penalty on fuel_node"),
        ("node", "balance_penalty", ("demand_node",), "default node penalty on demand_node"),
    ]

    found = []
    not_found = []
    errors = []

    with DatabaseMapping(TARGET_URL) as db:
        entity_classes = [ec["name"] for ec in db.get_entity_class_items()]
        print(f"\nEntity classes in target: {entity_classes}")

        print("\nEntities in target:")
        for ec in entity_classes:
            entities = db.get_entity_items(entity_class_name=ec)
            if entities:
                names = [e["entity_byname"] for e in entities]
                print(f"  {ec}: {names}")

        print("\nParameter values in target:")
        for pv in db.get_parameter_value_items():
            print(
                f"  {pv['entity_class_name']}.{pv['parameter_definition_name']} "
                f"on {pv['entity_byname']} [{pv['type']}]"
            )

        for entity_class, param_name, entity_byname, desc in expected:
            try:
                if entity_class not in entity_classes:
                    not_found.append(
                        (entity_class, param_name, entity_byname, desc,
                         f"Entity class '{entity_class}' does not exist")
                    )
                    continue

                if param_name is None:
                    # Entity existence check only
                    ent = db.get_entity_item(
                        entity_class_name=entity_class,
                        entity_byname=entity_byname,
                    )
                    if ent:
                        found.append((entity_class, param_name, entity_byname, desc))
                    else:
                        not_found.append(
                            (entity_class, param_name, entity_byname, desc,
                             "Entity not found")
                        )
                elif entity_byname is not None:
                    pv = db.get_parameter_value_item(
                        entity_class_name=entity_class,
                        parameter_definition_name=param_name,
                        entity_byname=entity_byname,
                        alternative_name="Base",
                    )
                    if pv:
                        found.append((entity_class, param_name, entity_byname, desc))
                    else:
                        all_pvs = db.get_parameter_value_items(
                            entity_class_name=entity_class,
                            parameter_definition_name=param_name,
                        )
                        matching = [p for p in all_pvs if p["entity_byname"] == entity_byname]
                        if matching:
                            found.append(
                                (entity_class, param_name, entity_byname,
                                 desc + f" (alt: {matching[0]['alternative_name']})")
                            )
                        else:
                            not_found.append(
                                (entity_class, param_name, entity_byname, desc,
                                 "Parameter value not found")
                            )
                else:
                    pvs = db.get_parameter_value_items(
                        entity_class_name=entity_class,
                        parameter_definition_name=param_name,
                    )
                    if pvs:
                        found.append(
                            (entity_class, param_name, pvs[0]["entity_byname"], desc)
                        )
                    else:
                        not_found.append(
                            (entity_class, param_name, None, desc,
                             "No parameter values found for any entity")
                        )
            except Exception as e:
                errors.append(
                    (entity_class, param_name, entity_byname, desc, str(e))
                )

    print(f"\n{'='*70}")
    print(f"VERIFICATION RESULTS")
    print(f"{'='*70}")

    print(f"\n[OK] FOUND ({len(found)}):")
    for ec, pn, ebn, desc in found:
        print(f"  [OK] {ec}.{pn} on {ebn} - {desc}")

    print(f"\n[MISS] NOT FOUND ({len(not_found)}):")
    for ec, pn, ebn, desc, reason in not_found:
        print(f"  [MISS] {ec}.{pn} on {ebn} - {desc}: {reason}")

    print(f"\n[ERR] ERRORS ({len(errors)}):")
    for ec, pn, ebn, desc, err in errors:
        print(f"  [ERR] {ec}.{pn} on {ebn} - {desc}: {err}")

    total = len(found) + len(not_found) + len(errors)
    print(f"\nSummary: {len(found)}/{total} found, {len(not_found)} missing, {len(errors)} errors")

    return not_found, errors


def main():
    print("=" * 70)
    print("INES-to-SpineOpt Transformation Test")
    print("=" * 70)

    print("\n[1] Creating source database from ines-spec-template.json...")
    try:
        create_db_from_template(SOURCE_URL, INES_TEMPLATE)
        print("  Source database created.")
    except Exception as e:
        print(f"  ERROR creating source DB: {e}")
        traceback.print_exc()
        return

    print("\n[2] Creating target database from spineopt_template.json...")
    try:
        create_db_from_template(TARGET_URL, SPINEOPT_TEMPLATE)
        print("  Target database created.")
    except Exception as e:
        print(f"  ERROR creating target DB: {e}")
        traceback.print_exc()
        return

    print("\n[3] Populating source database with test data...")
    try:
        populate_source_db()
    except Exception as e:
        print(f"  ERROR populating source DB: {e}")
        traceback.print_exc()
        return

    print("\n[4] Running ines_to_spineopt.py transformation...")
    success = run_transformation()
    if not success:
        print("  WARNING: Transformation returned non-zero exit code")

    print("\n[5] Verifying transformation results...")
    try:
        not_found, errors = verify_results()
    except Exception as e:
        print(f"  ERROR during verification: {e}")
        traceback.print_exc()
        return

    if not_found or errors:
        print(f"\n*** {len(not_found)} MISSING, {len(errors)} ERRORS ***", flush=True)
    else:
        print("\n*** ALL CHECKS PASSED ***", flush=True)


if __name__ == "__main__":
    main()
