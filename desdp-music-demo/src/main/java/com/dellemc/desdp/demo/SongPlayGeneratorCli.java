package com.dellemc.desdp.demo;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class SongPlayGeneratorCli {
    private static final Logger log = LoggerFactory.getLogger(SongPlayGeneratorCli.class);

    static Options options() {
        Options options = new Options();

        options.addOption(Option.builder("c").longOpt("controller").desc("Service endpoint of the Pravega controller")
                .hasArg().argName("controller-uri").build());
        options.addOption(Option.builder("x").longOpt("scope").desc("The Pravega scope")
                .hasArg().argName("pravega-scope").build());
        options.addOption(Option.builder("s").longOpt("stream").desc("The Pravega stream name")
                .hasArg().argName("pravega-stream").build());
        options.addOption(Option.builder("k").longOpt("use-keycloak").desc("This enables Keycloak authentication for use with Streaming Data Platform. You must have a valid keycloak.json file in your home directory")
                .build());

        options.addOption(Option.builder("e").longOpt("es-url").desc("Elastic Search URL")
                .hasArg().argName("elasticsearch-uri").build());

        options.addOption(Option.builder("r").longOpt("reader").desc("This will read the stream and send the data to Elastic Search")
                .build());
        options.addOption(Option.builder("w").longOpt("writer").desc("This will generate random song plays and writes them to a Pravega stream")
                .build());

        options.addOption(Option.builder().longOpt("min-xput").desc("Minimum throughput (events per second) to write. Throughput will vary randomly between min and max. Default is " + SongPlayGenerator.DEFAULT_MIN_XPUT)
                .hasArg().argName("events-per-second").build());
        options.addOption(Option.builder().longOpt("max-xput").desc("Maximum throughput (events per second) to write. Throughput will vary randomly between min and max. Default is " + SongPlayGenerator.DEFAULT_MAX_XPUT)
                .hasArg().argName("events-per-second").build());
        options.addOption(Option.builder().longOpt("xput-interval").desc("Time in seconds between changes to the throughput rate. Throughput will remain constant for this duration. Default is " + SongPlayGenerator.DEFAULT_XPUT_INTERVAL)
                .hasArg().argName("seconds").build());
        options.addOption(Option.builder("h").longOpt("help").desc("Print this help text").build());
        return options;
    }

    static SongPlayGenerator.Config parseConfigWriter(CommandLine commandLine) {
        SongPlayGenerator.Config config = new SongPlayGenerator.Config();

        if (commandLine.hasOption("min-xput"))
            config.setMinXput(Integer.parseInt(commandLine.getOptionValue("min-xput")));
        if (commandLine.hasOption("max-xput"))
            config.setMaxXput(Integer.parseInt(commandLine.getOptionValue("max-xput")));
        if (commandLine.hasOption("xput-interval"))
            config.setXputInterval(Integer.parseInt(commandLine.getOptionValue("xput-interval")));

        config.setControllerEndpoint(commandLine.getOptionValue('c'));
        config.setScope(commandLine.getOptionValue('x'));
        config.setStream(commandLine.getOptionValue('s'));
        config.setUseKeycloak(commandLine.hasOption('k'));

        return config;
    }

    static SongPlayReader.Config parseConfigReader(CommandLine commandLine) {
        SongPlayReader.Config config = new SongPlayReader.Config();

        if (commandLine.hasOption("es-url")){
            URI url = URI.create(commandLine.getOptionValue("es-url"));
            //log.info(url.getHost());
            //log.info(String.valueOf(url.getPort()));
            //log.info(url.getScheme());
            config.setSchemeES(url.getScheme());
            config.setIpES(url.getHost());
            config.setPortES(url.getPort());
        }

        config.setControllerEndpoint(commandLine.getOptionValue('c'));
        config.setScope(commandLine.getOptionValue('x'));
        config.setStream(commandLine.getOptionValue('s'));
        config.setUseKeycloak(commandLine.hasOption('k'));

        return config;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp(SongPlayGenerator.class.getSimpleName(), options(), true);
            System.out.println();
            System.exit(0);
        }
        CommandLine commandLine = new DefaultParser().parse(options(), args);
        // help text
        if (commandLine.hasOption('h')) {
            System.out.println("\n" + SongPlayGenerator.class.getSimpleName() + " - generates random song plays and writes them to a Pravega stream\n");
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp(SongPlayGenerator.class.getSimpleName(), options(), true);
            System.out.println();
        } else {
            if(commandLine.hasOption('w') ) {
                SongPlayGenerator.Config config = parseConfigWriter(commandLine);
                //parseConfigWriter(commandLine);
                log.info("parsed options:{}", config);
                SongPlayGenerator writer = new SongPlayGenerator(config);
                writer.run();
            }
            else if (commandLine.hasOption('r')){
                SongPlayReader.Config config = parseConfigReader(commandLine);
                //parseConfigReader(commandLine);
                log.info("parsed options:{}", config);
                SongPlayReader reader = new SongPlayReader(config);
                reader.run(config);
            }

        }
    }

}
