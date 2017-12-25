package football.functions;

import football.autowired_broadcast.AutowiredBroadcast;
import football.conf.UserConfig;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.UDF1;

@RegisterUDF
public class PlayerToCountryConverter implements UDF1<String, String> {
    @AutowiredBroadcast
    private Broadcast<UserConfig> broadcast;

    /**
     * Matches the player to his country.
     * @param player player of some team.
     * @return country(team) name of the player.
     * @throws Exception
     */
    @Override
    public String call(String player) throws Exception {
        return broadcast.value().getPlayerCountries().get(player);
    }
}
