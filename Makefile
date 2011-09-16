
all : 
	mkdir -p lib
	mvn install -DskipTests
	mvn dependency:copy-dependencies
	find */target/dependency/*.jar */target/*.jar -exec cp \{\} ./lib \;

eclipse : 
	mvn eclipse:eclipse

clean :
	mvn clean
	rm -rf lib
