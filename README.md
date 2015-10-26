# SFrame Split

This is a small script to split the SFrame xml files and send the jobs to the naf with qsub.
There are some open issues and possible extensions.

##How to use
-> Take the ConfigParse & ConfigSGE part from the example file and add it to your xml file. It has to stay commented otherwise sframe would read it and not work

-> go to the directory where you run sframe from 

-> For convenience chmod a+x sframe_split.py and export PATH=$PATH:/DirToScript/parallel/ 

-> sframe_split.py [options] File.xml

-> This creates the xml files and the needed sh files

-> For more have a look at the help


## Issues 
-> hadd with files that have no events raise warning that are persistent, but otherwise should work.

-> Code needs facelift

-> Documantation is missing

-> Only Split by File is tested other methods are implemented but at the moment not supported. E.g. other methods need to put in the number of events instead of -1. But anyway the should be way slower.

-> optparser deprecated should be changed to argparser
