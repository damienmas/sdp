package com.dellemc.oe.ingest;
/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */

import com.dellemc.oe.serialization.JsonNodeSerializer;
import com.dellemc.oe.util.CommonParams;
import com.dellemc.oe.util.Constants;
import com.dellemc.oe.util.DataGenerator;
import com.dellemc.oe.util.Utils;
import com.dellemc.oe.util.PravegaUtil;
import com.dellemc.oe.util.PravegaAppConfiguration;
//import io.pravega.example.flinkprocessor.AbstractJob;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * A simple example app that uses a Pravega Writer to write to a given scope and stream.
 */
public class JSONWriterDM {
    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(JSONWriterDM.class);

//    public final String scope;
//    public final String streamName;
//    public final String dataFile;
//    public final URI controllerURI;


    /*    public JSONWriter(String scope, String streamName, URI controllerURI,String dataFile) {
            this.scope = scope;
            this.streamName = streamName;
            this.dataFile = dataFile;
            this.controllerURI = controllerURI;
        }
    */
    private final PravegaAppConfiguration config;

    public JSONWriterDM(PravegaAppConfiguration appConfiguration) {
        config = appConfiguration;
    }
    public PravegaAppConfiguration getConfig() {
        return config;
    }



    public static void main(String[] args) {
        PravegaAppConfiguration config = new PravegaAppConfiguration(args);
        LOG.info("@@@@@@@@@@@@@@@@ config: {}", config);
        JSONWriterDM writer = new JSONWriterDM(config);
        writer.run();
    }

    public void run() {
        ObjectNode message = null;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*
        env.setParallelism(getConfig().getParallelism());
        if (!getConfig().isEnableOperatorChaining()) {
            env.disableOperatorChaining();
        }
        StreamExecutionEnvironment env = initializeFlinkStreaming();
        */
        LOG.info(" @@@@@@@@@@@@@@@@ Creating stream");
        PravegaUtil.createStream(getConfig().getClientConfig(), getConfig().getOutputStreamConfig());



        //PravegaUtil.createStream(getConfig().getClientConfig(), getConfig().getInputStreamConfig());

/*                ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();
                LOG.info(" @@@@@@@@@@@@@@@@ ClientConfig created");
                LOG.info(" @@@@@@@@@@@@@@@@ Creating stream " + scope + "/" + streamName + " on " + controllerURI);
                //  create stream
                boolean  streamCreated = Utils.createStream(scope, streamName, controllerURI);

                LOG.info(" @@@@@@@@@@@@@@@@ STREAM  =  "+ streamName + "  CREATED = "+ streamCreated);
                // Create EventStreamClientFactory
                try( EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

                // Create  Pravega event writer
                    EventStreamWriter<JsonNode> writer = clientFactory.createEventWriter(
                        streamName,
                        new JsonNodeSerializer(),
                        EventWriterConfig.builder().build())) {
                    //  Convert CSV  data to JSON
                    String data = DataGenerator.convertCsvToJson(dataFile);
                    // Deserialize the JSON message.
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode jsonArray = objectMapper.readTree(data);
                    if (jsonArray.isArray()) {
                        for (JsonNode node : jsonArray) {
                            message = (ObjectNode) node;
                            LOG.info("@@@@@@@@@@@@@ DATA  @@@@@@@@@@@@@  "+message.toString());
                            final CompletableFuture writeFuture = writer.writeEvent(routingKey, message);
                            writeFuture.get();
                            Thread.sleep(10000);
                        }

                    }

        }
        catch (Exception e) {
            LOG.error("@@@@@@@@@@@@@ ERROR  @@@@@@@@@@@@@  "+e.getMessage());
        }*/
    }
}
