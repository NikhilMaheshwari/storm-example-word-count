# storm-example-word-count

Storm Setup Local MacOS - 

Steps for local installation 
1. Install zookeeper – brew install zookeeper 
2. Install storm – brew install storm 
 
3. Further config from link https://www.tutorialspoint.com/apache_storm/apache_storm_installation.htm 
 
Steps to start zookeeper 
1. ZkServer start 
2. Storm nimbus 
3. Storm supervisor 
4. Storm UI 
5. Open localhost:8080

Running the code locally - 
1. mvn clean
2. mvn package
3. storm jar target/storm-example-app-1.0-SNAPSHOT.jar com.example.Main
