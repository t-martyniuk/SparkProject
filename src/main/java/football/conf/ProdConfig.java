package football.conf;

import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import static football.conf.Const.DEV;
import static football.conf.Const.PROD;

@Profile(PROD)
@Configuration
public class ProdConfig {
    @Bean
    public SparkConf sparkConf(){
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("football");
        return sparkConf;
    }
}
