# MultiWayLoader
This library provides multiple way to do table loads.
* JDBC to JDBC.
    * For moving data from multiple DBs.
    * If you want to copy within the same DB, please use a normal SQL.
* JDBC to file.
    * Copy data from DB to a file, for example the supported one: ORC.

# How to build
```
# Test code is not added yet at the moment.
mvn clean install -DskipTests=true
```

# Examples
* Check the ./examples folder.
* There are 2 examples:
    * JdbcToFile
    * JdbcToJdbc
* Change ./examples/pom.xml for the correct class for exec.
* Run:
    ```
    mvn package -DskipTests=true
    ```