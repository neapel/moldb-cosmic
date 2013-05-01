all :
	mvn assembly:assembly

clean :
	mvn clean

run :
	java -jar target/cosmic-0-jar-with-dependencies.jar
