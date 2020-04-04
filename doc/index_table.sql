create TABLE `index_cursor` (
  `auto_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `table_name` varchar(50) DEFAULT NULL COMMENT '数据表名称',
  `processed` varchar(50) DEFAULT NULL COMMENT '已处理的id',
  `task_type` int(11) DEFAULT NULL COMMENT '任务类型',
  `index_type` int(11) DEFAULT NULL COMMENT '索引类别',
  `index_flag` varchar(20) DEFAULT NULL COMMENT '索引标记',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`auto_id`),
  UNIQUE KEY `index_cursor_idx` (`table_name`,`task_type`,`index_type`,`index_flag`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='索引数据标记';

create TABLE `index_task` (
  `auto_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `table_name` varchar(50) DEFAULT NULL COMMENT '数据表名称',
  `task_status` int(11) DEFAULT NULL COMMENT '任务状态，1：处理中，2：处理成功，3：处理失败',
  `task_num` int(11) DEFAULT NULL COMMENT '任务数据量',
  `start_term` varchar(20) DEFAULT NULL COMMENT '起始条件',
  `end_term` varchar(20) DEFAULT NULL COMMENT '结束条件',
  `task_no` varchar(30) DEFAULT NULL COMMENT '任务id',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  `client_info` varchar(1000) DEFAULT NULL COMMENT '处理该任务的客户端信息',
  `task_type` int(11) DEFAULT NULL COMMENT '任务类型',
  `index_type` int(11) DEFAULT NULL COMMENT '索引方式类别',
  `id_list` varchar(2000) DEFAULT NULL COMMENT '该任务对应的id列表',
  `illegal_data` varchar(2000) DEFAULT NULL COMMENT '格式非法id',
  `index_flag` varchar(20) DEFAULT NULL COMMENT '索引标记',
  `fail_info` varchar(2000) DEFAULT NULL COMMENT '处理失败提示信息',
  PRIMARY KEY (`auto_id`),
  UNIQUE KEY `index_task_idx_task` (`task_no`),
  KEY `index_task_idx_table` (`table_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='索引任务记录';

create TABLE `index_update` (
  `auto_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `table_name` varchar(50) DEFAULT NULL COMMENT '表名',
  `data_id` bigint(20) DEFAULT NULL COMMENT '表主键',
  `insert_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON update CURRENT_TIMESTAMP,
  PRIMARY KEY (`auto_id`),
  UNIQUE KEY `idx_uniq` (`table_name`,`data_id`),
  KEY `idx_table` (`table_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='待更新到索引数据表';

create TABLE `index_delete` (
  `auto_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `table_name` varchar(50) DEFAULT NULL COMMENT '表名',
  `data_id` bigint(20) DEFAULT NULL COMMENT '表主键',
  `insert_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON update CURRENT_TIMESTAMP,
  PRIMARY KEY (`auto_id`),
  UNIQUE KEY `idx_uniq` (`table_name`,`data_id`),
  KEY `idx_table` (`table_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='待删除表到索引数据表';