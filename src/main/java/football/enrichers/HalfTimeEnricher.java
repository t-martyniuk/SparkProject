package football.enrichers;

import football.aspects.ShowDataFrameInTheBeginning;
import org.apache.spark.sql.DataFrame;
import org.springframework.stereotype.Service;

import static football.DataFrameBuilder.EVENT_TIME;
import static org.apache.spark.sql.functions.ceil;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

@Service
public class HalfTimeEnricher implements Enricher {
    public static final String HALF_TIME = "halfTime";
    private static final int HALF_TIME_LENGTH = 45*60;

    /**
     *Adds first or second halftime column.
     * @param df Dataframe that the column is added to.
     * @return Refreshed dataframe.
     */
    @ShowDataFrameInTheBeginning
    @Override
    public DataFrame enrich(DataFrame df) {
        df = df.withColumn(HALF_TIME, ceil(col(EVENT_TIME).divide(lit(HALF_TIME_LENGTH))));
        return df;

    }
}
