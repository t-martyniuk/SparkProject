package football;

import lombok.Getter;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.stereotype.Component;
import scala.util.parsing.combinator.testing.Str;

import java.util.HashMap;
import java.util.Set;



@Getter
public class Event {

    private int code;
    private String from;
    private String to;
    private long eventTime;
    private String stadion;

    /**
     * Maps lines of the file to the HashMaps of name of the field and value of the field.
     * @param line line of the textfile.
     * @return HashMap where keys are names of the fields and values are corresponding values.
     */
    public static HashMap <String, String> parseData(String line) {
        HashMap<String, String> entries = new HashMap<>();
        String data[] = line.split(";");
        for (String entry: data) {
            String[] entryData = entry.split("=");
            entries.put(entryData[0], entryData[1]);
        }
        return entries;
    }


    public Event(HashMap<String, String> entries) {
        code = Integer.parseInt(entries.get("code"));
        from = entries.get("from");
        to = entries.get("to");
        String[] data = entries.get("eventTime").split(":");
        int minutes = Integer.parseInt(data[0]);
        int seconds = Integer.parseInt(data[1]);
        eventTime = minutes * 60 + seconds;
        stadion = entries.get("stadion");
    }
}


