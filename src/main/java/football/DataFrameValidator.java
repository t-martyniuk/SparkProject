package football;

import football.autowired_broadcast.AutowiredBroadcast;
import football.conf.UserConfig;
import football.validators.Validator;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

import static football.DataFrameBuilder.*;

@Service
public class DataFrameValidator {

    @Autowired
    private List<Validator> validators;

    /**
     * Deletes non-valid rows from the DataFrame due to the validators' rules.
     * @param df DataFrame that the rows are deleted from.
     * @return refreshed DataFrame with valid rows.
     */
    public DataFrame retrieveValid(DataFrame df) {
        for(Validator validator: validators){
            df = validator.validate(df);
        }
        return df;
    }
}
