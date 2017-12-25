package football.validators;


import org.apache.spark.sql.DataFrame;
import org.springframework.stereotype.Service;

import static football.DataFrameBuilder.EVENT_TIME;

@Service
public class TimeValidator implements Validator {

    private static final int GAME_LENGTH = 90*60;

    /**
     * Leaves only those rows that have correct time.
     * @param df DataFrame that is under validation.
     * @return Refreshed dataframe.
     */
    @Override
    public DataFrame validate(DataFrame df) {
        df = df.filter(df.col(EVENT_TIME).leq(GAME_LENGTH));
        return df;
    }
}

