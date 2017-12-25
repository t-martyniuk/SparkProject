package football;

import football.autowired_broadcast.AutowiredBroadcast;
import football.conf.UserConfig;
import football.enrichers.Enricher;
import football.functions.CodeToDescriptionConverter;
import football.functions.PlayerToCountryConverter;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.apache.spark.sql.functions.*;
import static football.DataFrameBuilder.*;

@Service
public class DataFrameEnricher {
    @Autowired
    private List<Enricher> enrichers;

    /**
     * Adds columns to the DataFrame due to the enrichers' rules.
     * @param df DataFrame that the columns are added to.
     * @return refreshed DataFrame with new columns.
     */
    public DataFrame enrichData(DataFrame df) {
        for(Enricher enricher: enrichers){
            df = enricher.enrich(df);
        }
        return df;
    }
}