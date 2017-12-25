package football;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

@Service
public class DataFrameBuilder {
    public static final String CODE = "code";
    public static final String FROM = "from";
    public static final String TO = "to";
    public static final String EVENT_TIME = "eventTime";
    public static final String STADION = "stadion";

    @Autowired
    private JavaSparkContext sc;

    @Autowired
    private SQLContext sqlContext;

    /**
     * Loads the DataFrame from the text file.
     * @param filename Name of the file.
     * @return loaded DataFrame.
     */
    public DataFrame load(String filename) {
        JavaRDD<String> rdd = sc.textFile(filename);
        JavaRDD<Row> rowJavaRDD = rdd.
                filter(line -> !line.equals("")).
                map(line -> Event.parseData(line)).
                map(entries -> new Event(entries)).
                map(event -> RowFactory.create(event.getCode(), event.getFrom(), event.getTo(), event.getEventTime(), event.getStadion()));
        return sqlContext.createDataFrame(rowJavaRDD, createSchema());
    }


    private static StructType createSchema() {
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(CODE, DataTypes.IntegerType, false),
                DataTypes.createStructField(FROM, DataTypes.StringType, false),
                DataTypes.createStructField(TO, DataTypes.StringType, false),
                DataTypes.createStructField(EVENT_TIME, DataTypes.LongType, false),
                DataTypes.createStructField(STADION, DataTypes.StringType, false),

        });
    }
}