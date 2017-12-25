package football.validators;


import football.autowired_broadcast.AutowiredBroadcast;
import football.conf.UserConfig;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.springframework.stereotype.Service;

import static football.DataFrameBuilder.CODE;

@Service
public class CodeValidator implements Validator {
    @AutowiredBroadcast
    private Broadcast<UserConfig> broadcast;

    /**
     * Leaves only those rows where the action is made by correct number of players.
     * @param df DataFrame that is under validation.
     * @return Refreshed dataframe.
     */
    @Override
    public DataFrame validate(DataFrame df) {
        df = df.filter(df.col(CODE).isin(broadcast.value().TWO_PLAYERS_CODES));
        return df;
    }
}
