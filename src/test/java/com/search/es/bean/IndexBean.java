package com.search.es.bean;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author zqz
 * @version 1.0
 * @date 2020-04-04 00:02
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class IndexBean extends DocumentBaseFields {

    private String id;

    private String content1;

}
