
all : 
	mvn package
	mvn dependency:copy-dependencies

eclipse : 
	mvn eclipse:eclipse

clean :
	mvn clean
	rm -rf target