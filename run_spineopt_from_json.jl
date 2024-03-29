import SpineOpt
import JuMP
import JSON

path = @__DIR__
input = ARGS[1]#joinpath(path, "input_spineopt.json")#
output = ARGS[2]#joinpath(path, "output_spineopt.json")#

input_data = JSON.parsefile(input)

t0 = time()
m = SpineOpt.run_spineopt(input_data, nothing)
t1 = time()

#=
m = run_spineopt(
    ARGS...;
    upgrade=true,
    mip_solver=nothing,
    lp_solver=nothing,
    add_user_variables=m -> nothing,
    add_constraints=m -> nothing,
    update_constraints=m -> nothing,
    log_level=3,
    optimize=true,
    update_names=false,
    alternative="",
    write_as_roll=0,
    use_direct_model=false,
    filters=Dict("tool" => "object_activity_control"),
    log_file_path=nothing,
    resume_file_path=nothing
)
=#

#SpineOpt.write_model_file(m; file_name=output)
outputdata = Dict(
    "tool" => "SpineOpt",
    "time" => t1-t0,
    "objective" => SpineOpt.objective_value(m),
    "#variables" => JuMP.num_variables(m),
    "#constraints" => JuMP.num_constraints(m; count_variable_in_set_constraints = false)
)
open(output, "w") do f
    JSON.print(f, outputdata, 4)
end