# SFrame Split

This is a small script to split the SFrame xml files and send the jobs to the naf with qsub.
There are some open issues and possible extensions.

##How to use
-> Take the ConfigParse & ConfigSGE part from the example file and add it to your xml file. It has to stay commented otherwise sframe would read it and not work

-> go to the directory where you run sframe from 

-> dir/sframe_split.py [options] File.xml

-> This creates the xml files and the needed sh files

-> for more have a look at the help

## Issues 
-> hadd with files that have no events raise warning that are persistent.

-> SFrame creates jobTemp directories, don't erase them while the jobs is running, otherwise the job fails. After the jobs ends succusfully they are erased anyway.

-> Script should be made executable from any dir, at the moment it only works if invoked with full path name

-> Code needs facelift

-> Documantation is missing

-> Only Split by File is tested other methods are implemented but at the moment not supported. E.g. other methods need to put in the number of events instead of -1. But anyway the should be way slower.
