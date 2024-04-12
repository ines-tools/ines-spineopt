import SpineOpt
import SpineInterface
import JuMP
import JSON

input = ARGS[1]
output = ARGS[2]
otheroutput = "sqlite://"

if split(input, ".")[end] == "json"
	input = JSON.parsefile(input)
end

t0 = time()
m = SpineOpt.run_spineopt(input,otheroutput)
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

# directly store in ines-certify format instead of making another conversion (part of the SpineOpt data that we need is not stored in the spineopt database anyway)

#SpineOpt.write_model_file(m; file_name=output)

if split(output, ".")[end] == "json"
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
else
    outputdata = Dict(
        "entities" => [
            [
                "tool",
                "SpineOpt",
                nothing
            ]
        ],
        "parameter_values" => [
            [
                "tool",
                "SpineOpt",
                "time",
                t1-t0,
                "SpineOpt"
            ],
            [
                "tool",
                "SpineOpt",
                "objective value",
                SpineOpt.objective_value(m),
                "SpineOpt"
            ],
            [
                "tool",
                "SpineOpt",
                "number of variables",
                JuMP.num_variables(m),
                "SpineOpt"
            ],
            [
                "tool",
                "SpineOpt",
                "number of constraints",
                JuMP.num_constraints(m; count_variable_in_set_constraints = false),
                "SpineOpt"
            ]
        ],
        "alternatives" => [
            [
                "SpineOpt",
                ""
            ]
        ]
    )
    SpineInterface.import_data(output,outputdata,"import data")
end