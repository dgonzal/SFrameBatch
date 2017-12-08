#!/usr/bin/env python

import sys
# the mock-0.3.1 dir contains testcase.py, testutils.py & mock.py




#simple script that runs several sframe batch jobs and creates everything you might need
if __name__ == "__main__":
    debug = False
    remove = True #remove directories with old results

    #put your local sfram_batch dir in search path
    sys.path.append('/nfs/dust/cms/user/gonvaq/SFrameBatch/')
    #import the main function
    from sframe_batch import SFrameBatchMain

    #what you want to do, could also be done in parallel but then monitoring gets more difficult
    variations_variables = ['PU_variation']#'SF_muonID','BTag_variation',
    variations = ['up','down']
    xmlfile = "Sel.xml" # EleSel.xml
    for var in variations_variables:
        for value in variations:
            command_string = "-slac "+xmlfile+" -w workdir."+var+"_"+value+" -o ./"+var+"_"+value+" --ReplaceUserItem "+var+","+value

            if debug :print command_string.split(" ")
            else:
                #try:
                SFrameBatchMain(command_string.split(" "))
                """
                except:
                    print "SFrameBatch did crash during running:"
                    print command_string 
                    sys.exit(1)
                """
