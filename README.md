# SFrame Split

This is a small script to split the SFrame xml files and send the jobs to the naf with qsub.
There are some open issues and possible extensions.

##How to use

-> dir/sframe_split.py [options] File.xml

-> This creates the xml files and the needed sh files

-> for more have a look at the help

## Issues 

-> SFrame creates jobTemp directories, don't erase them, otherwise the job fails.

-> Script should be made executable from any dir, at the moment it only works if invoked with full path name

-> Code needs facelift

-> Documantation is missing
