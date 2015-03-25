import sys
import os
from itertools import islice

outputname = sys.argv[2]
points = int(sys.argv[3])
parts = int(sys.argv[4])
buffered = int(sys.argv[5])

buf = []
partno = 0
perfile = int(points/parts)
with open(sys.argv[1]) as f:
    while True:
        data = list(islice(f, perfile))
	data = buf + data
	buf = data[-buffered:]
	print len(data)
        if len(buf) == len(data):
            break
	outfp  = open(outputname + "{0:03d}".format(partno), "w")
	tobewritten = " ".join(data)
	outfp.write(tobewritten.replace("\n", ""))
	outfp.close()
	partno +=1

