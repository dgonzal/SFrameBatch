#!/usr/bin/env python
# -*- coding: utf-8 -*-

from subprocess import call
import multiprocessing

def run_missing_files(missing_files, worker=1):
    f = open(missing_files,'r')
    missing_xml = []
    for line in f:
        missing_xml.append(line.split(" ")[-1])

    pool = multiprocessing.Pool(processes=int(worker))
    result = pool.map_async(sframe_call,missing_xml)
    pool.close()
    pool.join()
    print 'missing xml files', len(missing_xml)
    while result._number_left>0:
            sys.stdout.write("\033[F")
            missing = round(float(result._number_left)*float(result._chunksize)/float(len(missing_xml))*100)
            if(missing > 100):
                missing =100
            print "Missing [%]", missing
            time.sleep(10)


    
    print 'done with',missing_files

    
def sframe_call(xml):
   call(['sframe_main '+xml], shell=True)


if __name__ == "__main__":
    run_missing_files(sys.argv[1])
