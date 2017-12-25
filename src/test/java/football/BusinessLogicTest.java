package football;

import com.oracle.webservices.internal.api.message.PropertySet;
import com.sun.org.apache.bcel.internal.classfile.Code;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;

import static football.DataFrameBuilder.*;
import static football.DataFrameEnricher.*;
import static football.conf.Const.DEV;
import static football.enrichers.CodeDescriptionEnricher.CODE_DESC;
import static football.enrichers.CountryEnricher.COUNTRY_OF_FROM;
import static football.enrichers.CountryEnricher.COUNTRY_OF_TO;
import static football.enrichers.HalfTimeEnricher.HALF_TIME;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = Conf.class)
@ActiveProfiles(DEV)
public class BusinessLogicTest {
    @Autowired
    private DataFrameBuilder dataFrameBuilder;

    @Autowired
    private DataFrameEnricher dataFrameEnricher;

    @Autowired
    private DataFrameValidator dataFrameValidator;

    private Row dataRow;

    @PostConstruct
    public void initialize() {
        DataFrame recievedDf = dataFrameBuilder.load("data/rawData.txt");
        recievedDf = dataFrameValidator.retrieveValid(recievedDf);
        recievedDf = dataFrameEnricher.enrichData(recievedDf);
        dataRow = recievedDf.collect()[0];
    }

    @Test
    public void testDataCode() {
        Assert.assertEquals(3, (int) dataRow.getAs(CODE));
    }

    @Test
    public void testDataFrom() {
        Assert.assertEquals("Denys Harmash", dataRow.getAs(FROM));
    }

    @Test
    public void testDataTo() {
        Assert.assertEquals("Roman Shirokov", dataRow.getAs(TO));
    }

    @Test
    public void testDataEventTime() {
        Assert.assertEquals(2689, (long) dataRow.getAs(EVENT_TIME));
    }

    @Test
    public void testDataStadion() {
        Assert.assertEquals("sill", dataRow.getAs(STADION));
    }

    @Test
    public void testDataHalfTime() {
        Assert.assertEquals(1, (long) dataRow.getAs(HALF_TIME));
    }

    @Test
    public void testDataCountryOfFrom() {
        Assert.assertEquals("Ukraine", dataRow.getAs(COUNTRY_OF_FROM));
    }

    @Test
    public void testDataCountryOfTo() {
        Assert.assertEquals("Russia", dataRow.getAs(COUNTRY_OF_TO));
    }

    @Test
    public void testDataCodeDescription() {
        Assert.assertEquals("pass sent", dataRow.getAs(CODE_DESC));
    }
}