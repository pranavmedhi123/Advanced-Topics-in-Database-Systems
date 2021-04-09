drop table Connected_nodes;

create table Connected_nodes (
  my_nodes int,
  my_connected_nodes int)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table Connected_nodes;


INSERT OVERWRITE table Connected_nodes SELECT my_nodes ,  COUNT(my_connected_nodes)  FROM Connected_nodes GROUP BY my_nodes;
INSERT OVERWRITE table Connected_nodes SELECT my_connected_nodes ,  COUNT(my_nodes)  FROM Connected_nodes GROUP BY my_connected_nodes;

SELECT *
FROM Connected_nodes;