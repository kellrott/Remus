
all : 
	mvn package -DskipTests
	mvn dependency:copy-dependencies
	cp -n */target/dependency/*.jar */target/*.jar lib/

eclipse : 
	mvn eclipse:eclipse

clean :
	mvn clean
	rm -rf target
