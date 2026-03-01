/*
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package com.dellemc.desdp.demo;


import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/*
 *  This flink application demonstrates the JSON Data reading
 */
public class SongPlayReader {
    private static final Logger log = LoggerFactory.getLogger(SongPlayReader.class);

    public static final String ELASTIC_SEARCH_IP = "127.0.0.1";
    public static final int ELASTIC_SEARCH_PORT = 9200;
    public static final String ELASTIC_SEARCH_SCHEME = "http";

    // Logger initialization

    private Config config;
    private AtomicBoolean running = new AtomicBoolean();

    public SongPlayReader(Config config) {
        this.config = config;

    }

    public static void run(Config config) {

        //running.set(true);

        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withControllerURI(URI.create(config.getControllerEndpoint()))
                .withDefaultScope(config.getScope())
                .withHostnameValidation(false);

        try {

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // create the Pravega source to read a stream of text
            FlinkPravegaReader<String> flinkPravegaReader = FlinkPravegaReader.<String>builder()
                    .withPravegaConfig(pravegaConfig)
                    .forStream(config.getStream())
                    .withDeserializationSchema(new UTF8StringDeserializationSchema())
                    .build();

            /*DataStream<String> events = env
                    .addSource(flinkPravegaReader)
                    .name("events");*/

            //DataStream<Tuple2<String, Integer>> events = env
            DataStream<Tuple2<String, Integer>> events = env
                    .addSource(flinkPravegaReader)
                    .name(config.getStream())
                    .map(new RowSplitter())
                    .flatMap(new ArtistCount())
                    .keyBy(0)
                    .sum(1);
                    //.flatMap(new Result());

            // create an output sink to print to stdout for verification
            events.printToErr();

            List<HttpHost> httpHosts = new ArrayList<>();
            httpHosts.add(new HttpHost("10.247.191.240", 9200, "http"));


            // use a ElasticsearchSink.Builder to create an ElasticsearchSink
            ElasticsearchSink.Builder<Tuple2<String, Integer>> esSinkBuilder = new ElasticsearchSink.Builder<Tuple2<String, Integer>>(
                    httpHosts,
                    new ElasticsearchSinkFunction<Tuple2<String, Integer>>() {
                        public IndexRequest createIndexRequest(Tuple2<String, Integer> element) {
                            Map<String, String> json = new HashMap<>();
                            json.put("artist", element.f0);
                            json.put("count", element.f1.toString());
                            return Requests.indexRequest()
                                    .index("music-demo")
                                    .type("top-artist")
                                    .source(json);
                        }

                        @Override
                        public void process(Tuple2<String, Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
                            indexer.add(createIndexRequest(element));
                        }
                    }
            );
            esSinkBuilder.setBulkFlushMaxActions(1);
            esSinkBuilder.setRestClientFactory(
                    restClientBuilder -> {
                        restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                            @Override
                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {

                                // elasticsearch username and password
                                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "secret"));

                                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            }
                        });
                    }
            );


            //ElasticsearchSink<String> elasticSink = newElasticSearchSink();
            events.addSink(esSinkBuilder.build());  //.name("Write to ElasticSearch")

            // execute within the Flink environment
            env.execute("JSON Reader");

            log.info("########## JSON READER END #############");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class RowSplitter implements
            MapFunction<String, Tuple3<String, String, String>> {

        public Tuple3<String, String, String> map(String row)
                throws Exception {

            JSONDataCustom obj = new ObjectMapper().readValue(row, JSONDataCustom.class);

            return new Tuple3<String, String, String>(
                    obj.playerId,
                    obj.song,
                    obj.artist
            );
        }
    }

    public static class ArtistCount implements
            FlatMapFunction<Tuple3<String, String, String>, Tuple2<String, Integer>> {
            //FlatMapFunction<Tuple3<String, String, String>, String> {

        public void flatMap(Tuple3<String, String, String> list, Collector<Tuple2<String, Integer>> out)
                throws Exception {

                out.collect(new Tuple2<String, Integer> (list.f2, 1));
               // out.collect(new String (list.f2.toString() + ", 1"));

        }
    }

    public static class Result implements FlatMapFunction<Tuple2<String, Integer>, String> {
        public void flatMap(Tuple2<String, Integer> list, Collector<String> str)
                throws Exception {
            str.collect(new String("{\"artist\": \""+ list.f0.toString() + "\", \"count\": \"" + list.f1 + "\"}"  ));
        }
    }

    static class Config extends Utils.Config{
        String ipES = ELASTIC_SEARCH_IP;
        int portES = ELASTIC_SEARCH_PORT;
        String schemeES= ELASTIC_SEARCH_SCHEME;

        public Config() {
        }

        public Config(String controllerEndpoint, String scope, String stream, boolean useKeycloak, String ipES, int portES, String schemeES) {
            //if (minXput > maxXput)
            //    throw new IllegalArgumentException("max xput must be greater than or equal to min xput");
            setControllerEndpoint(controllerEndpoint);
            setScope(scope);
            setStream(stream);
            setUseKeycloak(useKeycloak);

            setIpES(ipES);
            setPortES(portES);
            setSchemeES(schemeES);
        }

        public String getIpES() {
            return ipES;
        }

        public void setIpES(String ipES) {
            InetAddressValidator validator = new InetAddressValidator();
            if (!validator.isValidInet4Address(ipES)) throw new IllegalArgumentException("ipes must be a valid IPV4 address");
                this.ipES = ipES;
        }

        public int getPortES() {
            return portES;
        }

        public void setPortES(int portES) {
            if (portES < 1 || portES > 65535 ) throw new IllegalArgumentException("portes must be a valid integer between 1 and 65535");
            this.portES = portES;
        }

        public String getSchemeES() {
            return schemeES;
        }

        public void setSchemeES(String schemeES) {
            if (!"http".equals(schemeES) && !"https".equals(schemeES)) throw new IllegalArgumentException("schemees must be http or https");
            this.schemeES = schemeES;
        }

        @Override
        public String toString() {
            return "Config{" +
                    "controllerEndpoint='" + controllerEndpoint + '\'' +
                    ", scope='" + scope + '\'' +
                    ", stream='" + stream + '\'' +
                    ", useKeycloak=" + useKeycloak +
                    ", schemeES=" + schemeES +
                    ", ipES=" + ipES +
                    ", portES=" + portES +
                    '}';
        }
    }
}
