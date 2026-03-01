# Music Plays demo for Pravega Streaming Storage

This demo is intended to be similar to the Kafka Music demo.  There is a generator app that generates random song play events, a processing app that tracks windowed statistics (like song counts, artist counts, player counts, etc.), and a small REST service that provides access to these statistics.

## Running the generator

To run the generator, simply build the shadowJar (`./gradlew shadowJar`) and follow the usage instructions:

```
usage: SongPlayGenerator [-c <controller-uri>] [-e <elasticsearch-uri>]
       [-h] [-k] [--max-xput <events-per-second>] [--min-xput
       <events-per-second>] [-r] [-s <pravega-stream>] [-w] [-x
       <pravega-scope>] [--xput-interval <seconds>]
 -c,--controller <controller-uri>    Service endpoint of the Pravega
                                     controller
 -e,--es-url <elasticsearch-uri>     Elastic Search URL
 -h,--help                           Print this help text
 -k,--use-keycloak                   This enables Keycloak authentication
                                     for use with Streaming Data Platform.
                                     You must have a valid keycloak.json
                                     file in your home directory
    --max-xput <events-per-second>   Maximum throughput (events per
                                     second) to write. Throughput will
                                     vary randomly between min and max.
                                     Default is 20
    --min-xput <events-per-second>   Minimum throughput (events per
                                     second) to write. Throughput will
                                     vary randomly between min and max.
                                     Default is 1
 -r,--reader                         This will read the stream and send
                                     the data to Elastic Search
 -s,--stream <pravega-stream>        The Pravega stream name
 -w,--writer                         This will generate random song plays
                                     and writes them to a Pravega stream
 -x,--scope <pravega-scope>          The Pravega scope
    --xput-interval <seconds>        Time in seconds between changes to
                                     the throughput rate. Throughput will
                                     remain constant for this duration.
                                     Default is 20
```

(You can generate these instructions by running the jar with the `-h` or `--help` option) 
