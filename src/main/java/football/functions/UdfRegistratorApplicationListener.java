package football.functions;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Component
public class UdfRegistratorApplicationListener implements ApplicationListener<ContextRefreshedEvent> {
    @Autowired
    private ApplicationContext context;

    @Autowired
    private SQLContext sqlContext;

    private Map<Class, DataType> typeMap;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        Collection<Object> udfObjects = context.getBeansWithAnnotation(RegisterUDF.class).values();
        for (Object udfObject : udfObjects) {

            Class actualClass = udfObject.getClass();
            ParameterizedType type = (ParameterizedType) actualClass.getGenericInterfaces()[0];
            Type[] parameters = type.getActualTypeArguments();
            Class parameter = (Class) parameters[parameters.length - 1];

            sqlContext.udf().register(udfObject.getClass().getName(),
                    (UDF1<?, ?>) udfObject, typeMap.get(parameter));
        }
    }

    @PostConstruct
    private void initTypeMap() {
        typeMap = new HashMap<>();
        typeMap.put(String.class, DataTypes.StringType);
        typeMap.put(Integer.class, DataTypes.IntegerType);
        typeMap.put(Double.class, DataTypes.DoubleType);
    }
}
