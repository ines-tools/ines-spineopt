# ines-spineopt
Translation between the ines specification and the SpineOpt structure

The SpineOpt template in this repository is the snapshot that is used for the mapping process. This file may be behind on the template in the [SpineOpt repository](https://github.com/Spine-tools/SpineOpt.jl).

## development
The direction that the developments is headed:
+ the files outside of the folders (i.e. run_spineopt_in_spinetools.jl and convert_ines_spineopt.py) are to be deleted.
+ The files currently remain as there is still some useful code in there that needs to be migrated to the scripts that follow the same format as the other conversion scripts.
+ ines_to_spineopt.py and spineopt_to_ines.py are the most important files to further develop.
+ That development consists mostly of filling in the yaml files and perhaps make a specific function for a specific conversion.