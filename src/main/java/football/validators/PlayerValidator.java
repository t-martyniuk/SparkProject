package football.validators;

import football.autowired_broadcast.AutowiredBroadcast;
import football.conf.UserConfig;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.springframework.stereotype.Service;

import static football.DataFrameBuilder.FROM;
import static football.DataFrameBuilder.TO;

@Service
public class PlayerValidator implements Validator {
    @AutowiredBroadcast
    private Broadcast<UserConfig> broadcast;

    /**
     * Leaves only rows with existing players.
     * @param df DataFrame that is under validation.
     * @return Refreshed dataframe.
     */
    @Override
    public DataFrame validate(DataFrame df) {
        df = df.filter(df.col(FROM).isin(broadcast.value().getPlayerCountries().keySet().stream().toArray(String[]::new)));
        df = df.filter(df.col(TO).isin(broadcast.value().getPlayerCountries().keySet().stream().toArray(String[]::new)));
        return df;
    }
}
