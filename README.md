# CsaEventProcessor build and test

create a new dir and clone repository
```
mkdir test001
cd test001
git clone https://github.com/prof-eugene/CsaEventProcessor.git ./
```

normal build
```
./gradlew build
```

test run: converts src/test/resources/input.log input file and saves processed output into ./new_testdb1/new_output.csv file
```
./gradlew run --args="-i src/test/resources/input.log  -o new_output.csv -wd jdbc:hsqldb:file:new_testdb1/sampledb;shutdown=true"
```

check output file
```
cat ./new_testdb1/new_output.csv
```
```
a,5,APPLICATION_LOG,12345,true
b,3,,,false
c,8,,,true
```

build standalone jar 
```
./gradlew standaloneJar
```

run with custom-settings, like JVM maximum heap size or other options. Avaliable application-level settings are below
```
java -Xmx8192m -jar build/libs/CsaEventProcessorStandalone-1.0-SNAPSHOT.jar -i src/test/resources/input.log  -o new_output.csv -wd jdbc:hsqldb:file:new_testdb1/sampledb;shutdown=true 
```

avaliable options

```
usage: CsaEventProcessor
 -at,--alert-threshold <arg>        alert output events if their duration
                                    is longer (default 4 ms)
 -i,--input <arg>                   input file name
 -o,--output <arg>                  output file name
 -st,--single-thread                use single-thread processing (reduces
                                    memory load)
 -wd,--working-database-URL <arg>   HSQLDB working database URI (example
                                    "jdbc:hsqldb:file:/tmp/test1/sampledb;
                                    shutdown=true")
```

If OutOfMemory occurs, then there are two suggestions:
1. Increase maximum heap size with -Xmx8192m argument to java executable
2. Use single-thread processing by passing -st argument to the application executable
