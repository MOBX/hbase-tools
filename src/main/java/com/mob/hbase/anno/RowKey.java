/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.hbase.anno;

import java.lang.annotation.*;

/**
 * 行key标记
 * 
 * @author zxc Aug 28, 2015 11:46:11 AM
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ java.lang.annotation.ElementType.FIELD })
public @interface RowKey {

}
