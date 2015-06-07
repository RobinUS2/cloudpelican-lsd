# cloudpelican-lsd
Log stream dump tool (uses Rsyslog, Kafka and Apache storm)

### Getting Started in local mode ###
```
git clone git@github.com:RobinUS2/cloudpelican-lsd.git
cd cloudpelican-lsd/storm
mvn clean install
java -jar java -jar ~/.m2/repository/nl/us2/cloudpelican/storm-processor/1.0-SNAPSHOT/storm-proceor-1.0-SNAPSHOT-jar-with-dependencies.jar -zookeeper=zookeeper1.domain.com:2181,zookeeper2.domain.com:2181,zookeeper3.domain.com:2181 -topic=my_logging -grep=this
```

