
gen-thrift : gen-java


gen-java : remus.thrift
	thrift -gen java remus.thrift
	cp -r gen-java/org src/main/java/

clean :
	rm -rf gen-java
