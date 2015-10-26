# SFrame Split

This is a small script to split the SFrame xml files and send the jobs to the naf with qsub.
There are some open issues and possible extensions.

##Install
-> git clone this repo anywhere :)

-> For convenience you might have to chmod a+x sframe_split.py 

##How to use
-> Setup sframe & cmssw for a 2.7 python version 

-> export PATH=$PATH:/DirToScript/parallel/ 

-> Take the ConfigParse & ConfigSGE part from the example file and add it to your xml file. It has to stay commented otherwise sframe would read it and not work

-> go to the directory where you run sframe from. This is not requiered by sframe_split but if you use relative paths, it will not possible to resolve them correctly and errors will be thrown

-> sframe_split.py [options] File.xml

-> This creates the xml files and the needed sh files

-> For more have a look at the help


## Issues 
-> hadd with files that have no events raise warning that are persistent, but otherwise work as far as known.

-> very few safty & sanity checks

-> Code needs facelift

-> Documantation is missing

-> Only Split by File is tested other methods are implemented but at the moment not supported. E.g. for other methods the user need to put in the number of events int the xml file instead of -1. But anyway the should be way slower.

-> optparser deprecated should be changed to argparser
