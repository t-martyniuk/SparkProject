package football.enrichers;

import football.functions.CodeToDescriptionConverter;
import org.apache.spark.sql.DataFrame;
import org.springframework.stereotype.Service;

import static football.DataFrameBuilder.CODE;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

@Service
public class CodeDescriptionEnricher implements Enricher {
    public static final String CODE_DESC = "codeDescription";

    /**
     * Adds the description of the action.
     * @param df Dataframe that the column is added to.
     * @return Refreshed dataframe.
     */
    @Override
    public DataFrame enrich(DataFrame df) {
        df = df.withColumn(CODE_DESC, callUDF(CodeToDescriptionConverter.class.getName(), col(CODE)));
        return df;

    }
}
