
all:
	mvn dependency:copy-dependencies
	java -Djava.ext.dirs=target/dependency/ org.antlr.Tool -o src/main/java/org/remus/tools/antlr RemusCli.g
