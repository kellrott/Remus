
all : 
	mkdir -p lib
	mvn install -DskipTests

eclipse : 
	mvn eclipse:eclipse

clean :
	mvn clean
	rm -rf lib
