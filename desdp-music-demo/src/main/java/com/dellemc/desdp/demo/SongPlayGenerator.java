package com.dellemc.desdp.demo;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SongPlayGenerator {
    private static Logger log = LoggerFactory.getLogger(SongPlayGenerator.class);

    private static final String SONG_MAP_RESOURCE = "/songs.lst";
    public static final int DEFAULT_MIN_XPUT = 1; // per second
    public static final int DEFAULT_MAX_XPUT = 20; // per second
    public static final int DEFAULT_XPUT_INTERVAL = 20; // seconds

    private static List<String> _songList;
    private static Map<String, String> _artistMap;
    //private static Utils.Config pravegaConfig;

    static Map<String, String> getArtistMap() {
        if (_artistMap == null) {
            synchronized (SongPlayGenerator.class) {
                if (_artistMap == null) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(SongPlayGenerator.class.getResourceAsStream(SONG_MAP_RESOURCE)));
                    _artistMap = reader.lines().collect(Collectors.toMap(s -> s.split("::")[0], s -> s.split("::")[1]));
                }
            }
        }
        return _artistMap;
    }

    static List<String> getSongList() {
        if (_songList == null) {
            synchronized (SongPlayGenerator.class) {
                if (_songList == null) {
                    _songList = new ArrayList<>(getArtistMap().keySet());
                }
            }
        }
        return _songList;
    }

    private Config config;
    private Random random;
    private AtomicInteger currentXput = new AtomicInteger();
    private long lastIntervalChangeTime;
    private AtomicBoolean running = new AtomicBoolean();

    public SongPlayGenerator(Config config) {
        this.config = config;
        this.random = new Random();
        verifyXput(); // last change time will be 0 here, so this will set an initial xput
    }
    public void run() {
        running.set(true);

        // create stream
        ClientConfig clientConfig = Utils.createClientConfig(config);
        Utils.createStream(clientConfig);

        // create client factory and event writer
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(config.getScope(), clientConfig);
             EventStreamWriter<String> writer = clientFactory.createEventWriter(
                     config.getStream(), new UTF8StringSerializer(), EventWriterConfig.builder().build())) {

            // loop until stopped
            while (running.get()) {

                // use the player ID as the routing key (guarantees order for each player)
                String playerId = generatePlayerId();
                String message = generatePlayMessage(playerId);
                log.info("Writing message (key: {}, message: {}) to stream {} / {}",
                        playerId, message, config.getScope(), config.getStream());
                writer.writeEvent(playerId, message);
                verifyXput();
                throttle();
            }
        }
    }

    public void stop() {
        running.set(false);
    }

    String generatePlayerId() {
        // just generate a random integer between 1 and 10,000
        return "" + (random.nextInt(10000) + 1);
    }

    String generatePlayMessage(String playerId) {

        // pull a random song from the song list
        String song = getSongList().get(random.nextInt(getSongList().size()));

        // get the artist
        String artist = getArtistMap().get(song);
        return "{" +
                "\"playerId\": \"" + playerId + "\"," +
                "\"song\": \"" + song + "\"," +
                "\"artist\": \"" + artist + "\"" +
                "}";
    }

    void verifyXput() {
        if (System.currentTimeMillis() - lastIntervalChangeTime > config.getXputInterval() * 1000) {

            // time to change up the xput
            currentXput.set(randomizeXput());
            lastIntervalChangeTime = System.currentTimeMillis();
        }
    }

    int randomizeXput() {
        if (config.getMaxXput() > config.getMinXput())
            return random.nextInt(config.getMaxXput() - config.getMinXput()) + config.getMinXput();
        else return config.getMinXput();
    }

    void throttle() {
        long sleepTime = 1000 / currentXput.get();
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            log.warn("interrupted while throttling", e);
        }
    }

    public int getCurrentXput() {
        return currentXput.get();
    }

    static class Config extends Utils.Config{
        int minXput = DEFAULT_MIN_XPUT;
        int maxXput = DEFAULT_MAX_XPUT;
        int xputInterval = DEFAULT_XPUT_INTERVAL;

        public Config() {
        }

        public Config(String controllerEndpoint, String scope, String stream, boolean useKeycloak, int minXput, int maxXput, int xputInterval) {
            if (minXput > maxXput) {
                throw new IllegalArgumentException("max xput must be greater than or equal to min xput");
            }
            setControllerEndpoint(controllerEndpoint);
            setScope(scope);
            setStream(stream);
            setUseKeycloak(useKeycloak);

            setMinXput(minXput);
            setMaxXput(maxXput);
            setXputInterval(xputInterval);
        }

        public int getMinXput() {
            return minXput;
        }

        public void setMinXput(int minXput) {
            if (minXput <= 0) throw new IllegalArgumentException("min xput must be greater than 0");
            this.minXput = minXput;
        }

        public int getMaxXput() {
            return maxXput;
        }

        public void setMaxXput(int maxXput) {
            if (minXput > maxXput)
                throw new IllegalArgumentException("max xput must be greater than or equal to min xput");
            this.maxXput = maxXput;
        }

        public int getXputInterval() {
            return xputInterval;
        }

        public void setXputInterval(int xputInterval) {
            if (xputInterval <= 0) throw new IllegalArgumentException("xput interval must be greater than 0");
            this.xputInterval = xputInterval;
        }

        @Override
        public String toString() {
            return "Config{" +
                    "controllerEndpoint='" + controllerEndpoint + '\'' +
                    ", scope='" + scope + '\'' +
                    ", stream='" + stream + '\'' +
                    ", useKeycloak=" + useKeycloak +
                    ", minXput=" + minXput +
                    ", maxXput=" + maxXput +
                    ", xputInterval=" + xputInterval +
                    '}';
        }
    }
}
