package com.dataartisans.flinktraining.exercises.datastream_java.datatypes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CoreEvent implements Comparable<CoreEvent>, Serializable {
    public CoreEvent() {
        this.properties = new HashMap<String, String>();
    }

    public static CoreEvent fromReader(BufferedReader reader) throws IOException {
        CoreEvent event = new CoreEvent();
        String[] split;
        String line = reader.readLine();
        while(line != null && !line.equals("===")) {
            split = line.split(": ");
            if(split.length != 2) {
                if(line.equals("===")) {
                    break;
                } else {
                    line = reader.readLine();
                    continue;
                }
            }
            event.addProperty(split[0], split[1]);
            line = reader.readLine();
        }

        return event;
    }

    @Override
    public int compareTo(CoreEvent o) {
        return 0;
    }

    public void addProperty(String key, String value) {
        properties.put(key, value);
    }

    public Instant getEventTime() {
        if (!properties.containsKey("eventTime")) {
            return null;
        }

        return Instant.parse(properties.get("eventTime"));
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }


    private Map<String,String> properties;
}
