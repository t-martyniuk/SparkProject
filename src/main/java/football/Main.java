package football;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Main {
    public static void main(String[] args) {
        System.setProperty("spring.profiles.active", "Dev");
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Conf.class);
        BusinessLogic businessLogic = context.getBean(BusinessLogic.class);
        businessLogic.doWork("data/rawData.txt");
    }
}
