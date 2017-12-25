package football;

import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.xml.crypto.Data;
import java.io.Serializable;

import static org.apache.spark.sql.functions.col;

@Service
public class BusinessLogic  {
    @Autowired
    private DataFrameBuilder dataFrameBuilder;

    @Autowired
    private DataFrameEnricher dataFrameEnricher;

    @Autowired
    private DataFrameValidator dataFrameValidator;

    /**
     * Validates and enriches the DataFrame downloaded from the file.
     * @param filename Name of the file.
     * @return Refreshed dataframe.
     */
    public DataFrame doWork(String filename){
        DataFrame df = dataFrameBuilder.load(filename);
        df = dataFrameValidator.retrieveValid(df);
        df = dataFrameEnricher.enrichData(df);
        return df;
    }
}
