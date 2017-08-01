import parsl;
from parsl import *
import sys

workers=ThreadPoolExecutor(max_workers=5)
dfk=DataFlowKernel(workers)

@App('bash', dfk)
def processCcd(visitnumber,ccdnumber, stdout='std.out', stderr='std.err'):	
	cmd_line='''cd ..
	. lsst-env.sh
	cd lsstsw
	setup -v pipe_tasks
	setup -v obs_subaru
	processCcd.py repo --calib repo --output repo --doraise --id visit={0} ccd={1}'''

@App('bash', dfk)
def makeSkyMap(stdout='std.out', stderr='std.err'):
	cmd_line='''cd ..
	. lsst-env.sh
	cd lsstsw
	setup -v pipe_tasks
	setup -v obs_subaru
	makeSkyMap.py repo -C skymapConfig.py --output repo --doraise'''

@App('bash', dfk)
def makeCoaddTempExp(visitnumber1, ccd1, ccd2, stdout='std.out', stderr='std.err'):
	cmd_line='''cd ..
	. lsst-env.sh
        cd lsstsw
        setup -v pipe_tasks
        setup -v obs_subaru
        makeCoaddTempExp.py repo --output repo --doraise --no-versions --id patch=8,6 tract=0 filter=HSC-R -c doApplyUberCal=False --selectId visit={0} ccd={1} --selectId visit={0} ccd={2} --clobber-config'''

futures_level1={};
futures_level1[1]= processCcd(903334, 16);
futures_level1[2]= processCcd(903334, 23);
futures_level1[3]= processCcd(903336, 17);
futures_level1[4]= processCcd(903336, 24);
futures_level1[5]= makeSkyMap();

for i in futures_level1:
	print(futures_level1[i].result())

futures_level2={};
futures_level2[1]= makeCoaddTempExp(903336, 17, 24);
futures_level2[2]= makeCoaddTempExp(903334, 16, 23);

for i in futures_level2:
	print(futures_level2[i].result())
