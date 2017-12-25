package football.aspects;

import org.apache.spark.sql.DataFrame;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;

import static football.conf.Const.DEV;

@Component
@Aspect
@Profile(DEV)
public class ShowDataFrameExecution {
    @AfterReturning(
            value = "@annotation(football.aspects.ShowDataFrameInTheEnd)",
            returning = "df")
    public void showAfter(JoinPoint joinPoint, DataFrame df) {
        String methodName = joinPoint.getSignature().getName();
        System.out.println("===== Showing DataFrame after " + methodName + " ====");
        df.show();
        System.out.println("==== Stop showing DataFrame ====");
    }

    @Before(value = "@annotation(football.aspects.ShowDataFrameInTheEnd) && args(df)", argNames = "df")
    public void showBefore(DataFrame df) {
        System.out.println("===== Showing DataFrame before ====");
        df.show();
        System.out.println("==== Stop showing DataFrame ====");
    }
}


