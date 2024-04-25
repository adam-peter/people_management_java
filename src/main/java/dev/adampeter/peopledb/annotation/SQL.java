package dev.adampeter.peopledb.annotation;

import dev.adampeter.peopledb.model.CrudOperation;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@Repeatable(MultiSQL.class)
public @interface SQL {
    String value();
    CrudOperation operationType();
//    int age() default 30; - can create more annotation attributes by creating more methods
//    String name() default "John";
}
