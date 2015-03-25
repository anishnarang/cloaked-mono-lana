import socket
import sys
import re
s = socket.socket()
s.bind((sys.argv[1],8008))
s.listen(20)
bsf = 1e20 

while True:
    sc, address = s.accept()
    print address     
    inputval = sc.recv(1024)
    print inputval
    if re.match("^\d+?\.?\d+?$", inputval.strip()) is not None:
		inputval = float(inputval)
		print inputval
		if(float(inputval) < bsf ) :
			bsf = float(inputval) 
		sc.send(str(bsf))
    else:
	sc.send(str(bsf)) 
    sc.close()
s.close()
