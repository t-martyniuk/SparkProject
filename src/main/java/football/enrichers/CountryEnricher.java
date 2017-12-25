package football.enrichers;

import football.ShowDataFrameInTheEnd;
import football.functions.PlayerToCountryConverter;
import org.apache.spark.sql.DataFrame;
import org.springframework.stereotype.Service;

import static football.DataFrameBuilder.FROM;
import static football.DataFrameBuilder.TO;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

@Service
public class CountryEnricher implements Enricher {
    public static final String COUNTRY_OF_FROM = "countryOfFrom";
    public static final String COUNTRY_OF_TO = "countryOfTo";

    /**
     *Adds countries of the players.
     * @param df Dataframe that the column is added to.
     * @return Refreshed dataframe.
     */
    @ShowDataFrameInTheEnd
    @Override
    public DataFrame enrich(DataFrame df) {
        df = df.withColumn(COUNTRY_OF_FROM,
                callUDF(PlayerToCountryConverter.class.getName(), col(FROM)));

        df = df.withColumn(COUNTRY_OF_TO, callUDF(PlayerToCountryConverter.class.getName(), col(TO)));
        return df;
    }
}
