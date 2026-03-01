package com.dellemc.desdp.demo;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.keycloak.client.PravegaKeycloakCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class Utils {
    private static Logger log = LoggerFactory.getLogger(SongPlayGenerator.class);
    private static Config config;
    //private Config config;

    
    static ClientConfig createClientConfig(Config myconfig) {
        config = myconfig;
        ClientConfig.ClientConfigBuilder builder = ClientConfig.builder();
        builder.controllerURI(URI.create(config.getControllerEndpoint()));

        // Keycloak means we are using Streaming Data Platform
        if (config.isUseKeycloak()) {
            builder.credentials(new PravegaKeycloakCredentials());
        }

        return builder.build();
    }

    static void createStream(ClientConfig clientConfig) {
        try (StreamManager streamManager = StreamManager.create(clientConfig)) {

            // create the scope
            if (!config.isUseKeycloak()) // can't create a scope in SDP
                streamManager.createScope(config.getScope());

            // create the stream
            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.byEventRate(3, 2, 1))
                    .build();
            streamManager.createStream(config.getScope(), config.getStream(), streamConfiguration);
        }
    }



    static class Config {

        String controllerEndpoint;
        String scope;
        String stream;
        boolean useKeycloak;

        public Config() {
        }

        public Config(String controllerEndpoint, String scope, String stream, boolean useKeycloak) {
            setControllerEndpoint(controllerEndpoint);
            setScope(scope);
            setStream(stream);
            setUseKeycloak(useKeycloak);

        }

        public String getControllerEndpoint() {
            return controllerEndpoint;
        }

        public void setControllerEndpoint(String controllerEndpoint) {
            if (controllerEndpoint == null || controllerEndpoint.trim().length() == 0)
                throw new IllegalArgumentException("controller endpoint is required");
            this.controllerEndpoint = controllerEndpoint;
        }

        public String getScope() {
            return scope;
        }

        public void setScope(String scope) {
            if (scope == null || scope.trim().length() == 0) throw new IllegalArgumentException("scope is required");
            this.scope = scope;
        }

        public String getStream() {
            return stream;
        }

        public void setStream(String stream) {
            if (stream == null || stream.trim().length() == 0) throw new IllegalArgumentException("stream is required");
            this.stream = stream;
        }

        public boolean isUseKeycloak() {
            return useKeycloak;
        }

        public void setUseKeycloak(boolean useKeycloak) {
            this.useKeycloak = useKeycloak;
        }

        @Override
        public String toString() {
            return "Config{" +
                    "controllerEndpoint='" + controllerEndpoint + '\'' +
                    ", scope='" + scope + '\'' +
                    ", stream='" + stream + '\'' +
                    ", useKeycloak=" + useKeycloak +
                    '}';
        }
    }
}
