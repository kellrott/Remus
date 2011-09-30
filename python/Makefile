
all : 
	thrift -gen py ../core/remus.thrift
	cp -r gen-py/remus/net remus/

clean :
	rm -rf remus/thrift
