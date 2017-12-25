package football.functions;

import football.autowired_broadcast.AutowiredBroadcast;
import football.conf.UserConfig;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.UDF1;

@RegisterUDF
public class PlayerToCountryConverter implements UDF1<String, String> {
    @AutowiredBroadcast
    private Broadcast<UserConfig> broadcast;


    @Override
    public String call(String player) throws Exception {
        return broadcast.value().getPlayerCountries().get(player);
    }
}
