package football.functions;

import football.conf.UserConfig;
import football.autowired_broadcast.AutowiredBroadcast;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.UDF1;

@RegisterUDF
public class CodeToDescriptionConverter implements UDF1<Integer, String> {
    @AutowiredBroadcast
    private Broadcast<UserConfig> broadcast;

    @Override
    public String call(Integer code) throws Exception {
        return broadcast.value().getActionCodes().get(code);
    }
}
