package com.dellemc.oe.readers;

import java.net.URI;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.dellemc.oe.util.Constants;
import com.dellemc.oe.util.Utils;
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.UTF8StringSerializer;

import com.dellemc.oe.util.CommonParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EventReader {

    private static Logger LOG = LoggerFactory.getLogger(EventReader.class);
    public final String scope;
    public final String streamName;
    public final URI controllerURI;

    public EventReader(String scope, String streamName, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
    }

    public void run(String routingKey, String message) {
        StreamInfo streamInfo;
        try(StreamManager streamManager = StreamManager.create(controllerURI)) {
            streamInfo = streamManager.getStreamInfo(scope, streamName);
        }


        //final boolean scopeIsNew = streamManager.createScope(scope);
        //StreamConfiguration streamConfig = StreamConfiguration.builder()
        //        .scalingPolicy(ScalingPolicy.fixed(1))
        //        .build();
        //final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);

        final String readerGroup = UUID.randomUUID().toString().replace("-", "");
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName),
                        streamInfo.getTailStreamCut(),
                        //streamInfo.getHeadStreamCut(),
                        StreamCut.UNBOUNDED)
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }

        try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
             EventStreamReader<String> reader = clientFactory.createReader("reader",
                     readerGroup,
                     new UTF8StringSerializer(),
                     ReaderConfig.builder().build())) {
            System.out.format("Reading all the events from %s/%s%n", scope, streamName);
            EventRead<String> event = null;
            for (;;) {
                //do {
                try {
                    event = reader.readNextEvent(1000);
                    if (event.getEvent() != null) {
                        System.out.format("Read event '%s'%n", event.getEvent());
                    }
                } catch (ReinitializationRequiredException e) {
                    //There are certain circumstances where the reader needs to be reinitialized
                    e.printStackTrace();
                }
                //} while (event.getEvent() != null);
            }
            //System.out.format("No more events from %s/%s%n", scope, streamName);
        }
    }

    public static void main (String[]args){
        CommonParams.init(args);
        final String scope = CommonParams.getParam(Constants.SCOPE);
        final String streamName = CommonParams.getParam(Constants.STREAM_NAME);
        final String routingKey = CommonParams.getParam(Constants.ROUTING_KEY_ATTRIBUTE_NAME);
        final URI controllerURI = URI.create(CommonParams.getParam(Constants.CONTROLLER_URI));
        final String message = CommonParams.getParam(Constants.MESSAGE);

        EventReader ew = new EventReader(scope, streamName, controllerURI);
        ew.run(routingKey, message);
    }
}
