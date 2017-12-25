package football.conf;

import lombok.Getter;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import scala.Int;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.*;

@Component
@Getter
@PropertySource("classpath:football_columns.properties")
public class UserConfig implements Serializable {

    public static final Integer[] TWO_PLAYERS_CODES = {3,4};
    private Set<String> columnNames;
    private Map<Integer, String> actionCodes;
    private Map<String, String> playerCountries;

    @Value("${columnNames}")
    private void setColumnNames(String[] columnNames) {
        this.columnNames = new HashSet<String>(Arrays.asList(columnNames));
    }

    @SneakyThrows
    private Properties readProperties(String filename) {
        Properties properties = new Properties();
        properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(filename));
        return properties;
    }

    @PostConstruct
    private void readActionCode() {
        Properties properties = readProperties("codes.properties");
        Enumeration e = properties.propertyNames();
        actionCodes = new HashMap<>();

        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            Integer code = Integer.parseInt(key);
            actionCodes.put(code, properties.getProperty(key));
        }
    }

    @PostConstruct
    private void readPlayersCountry() {
        Properties properties = readProperties("teams.properties");
        Enumeration e = properties.propertyNames();
        playerCountries = new HashMap<>();

        while (e.hasMoreElements()) {
            String country = (String) e.nextElement();
            String team = properties.getProperty(country);
            String[] players = team.split(",");
            for(String player: players) {
                playerCountries.put(player,country);
            }
        }
    }
}
