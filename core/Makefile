
gen-thrift : gen-java


gen-java : remusNet.thrift
	thrift -gen java remusNet.thrift
	cp -r gen-java/org src/main/java/

clean :
	rm -rf gen-java
