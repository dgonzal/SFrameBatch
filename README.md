# SFrame Batch

This is a small script to split the SFrame xml files and send the jobs to the naf with qsub.
There are some open issues and possible extensions.

##Install
-> git clone this repo anywhere :)

-> For convenience you might have to chmod a+x sframe_split.py 

##How to use
-> SFrame creates jobTemp_* directories where you start this script. Take care to have enough disk space.

-> Setup sframe & cmssw for a 2.7 python version 

-> export PATH=$PATH:/DirToScript/SFrameBatch/ 

-> Take the ConfigParse & ConfigSGE part from the example file and add it to your xml file. It has to stay commented at the beginnign of the file otherwise sframe would read it and not work.

-> Go to the directory where you run sframe from. This is not requiered by sframe_split but if you use relative paths, it will not be possible to resolve them correctly and errors will be thrown.

-> sframe_batch.py [options] File.xml

-> sframe_batch.py File.xml creates the xml files, the needed sh files and tells you how many jobs you are going to submit

-> To submit the jobs use the -s option. Pls make sure that you don't submit too many jobs 

-> For more have a look at the help: sframe_batch.py --help

-> Split by file and by events is in. For split by events all the files need to be opend.

## Issues 
-> very few safty & sanity checks

-> Code needs facelift

-> Documantation is missing

-> optparser deprecated should be changed to argparser

## Extensions & improvements

-> http://stackoverflow.com/questions/26104116/qstat-and-long-job-names retrieve names and extendet support for resubmission if job is fails 

-> gui e.g. see https://github.com/wardi/urwid, it is shipped with cmssw

-> Use https://raw.githubusercontent.com/schmitts/fhadd/master/fhadd.py 
