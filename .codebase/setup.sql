
CREATE STORAGE VOLUME minio_volume
TYPE = S3
LOCATIONS = ("s3://bucket1/starrocks_warehouse/")
PROPERTIES
(
    "enabled" = "true",
    "aws.s3.region" = "cn-beijing",
    "aws.s3.endpoint" = "http://127.0.0.1:9000",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "minio_access_key",
    "aws.s3.secret_key" = "minio_secret_key"
);

SET minio_volume AS DEFAULT STORAGE VOLUME;

create database demo;

use demo;


CREATE TABLE `tb_all_primitivetype_write_duplicate` (
  `rowId` int(11) NULL COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_largeint` LARGEINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
); 

CREATE TABLE `tb_all_primitivetype_write_unique` (
  `rowId` int(11) NULL COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_largeint` LARGEINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP 
UNIQUE KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
); 

CREATE TABLE `tb_all_primitivetype_write_primary` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_largeint` LARGEINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
); 

CREATE TABLE `tb_all_primitivetype_write_aggregate` (
  `rowId` int(11) NULL COMMENT "",
  `c_boolean` BOOLEAN REPLACE NULL COMMENT "",
  `c_tinyint` TINYINT MAX NULL COMMENT "",
  `c_smallint` SMALLINT MAX NULL COMMENT "",
  `c_int` int(11) SUM NULL COMMENT "",
  `c_bigint` BIGINT SUM NULL COMMENT "",
  `c_largeint` LARGEINT SUM NULL COMMENT "",
  `c_float` FLOAT SUM NULL COMMENT "",
  `c_double` DOUBLE SUM NULL COMMENT "",
  `c_date` DATE MAX NULL COMMENT "",
  `c_datetime` DATETIME MAX NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) SUM NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  SUM NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  SUM NULL COMMENT "",
  `c_char` CHAR(128)  MAX NULL COMMENT "",
  `c_varchar` VARCHAR(512)  MAX NULL COMMENT ""
) ENGINE=OLAP 
AGGREGATE KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
); 

-- read test
CREATE TABLE `tb_all_primitivetype_read_duplicate` (
  `rowId` int(11) NULL COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_largeint` LARGEINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
); 


insert into tb_all_primitivetype_read_duplicate 
values
(0, true, 127, 32767, 2147483647, 9223372036854775807, 99999999999999999999999999999999999999, -0, -0, "1900-01-01", "1800-11-20 12:40:39", 9999999.57, 999999999999999.568, 9999999999999999999999999999999999.5679, "c_char:name0", "c_varchar:name0"),
(1, false, -128, -32768, -2147483648, -9223372036854775808, -99999999999999999999999999999999999999, 123.45679, 23456.78901234, "4096-12-31", "4096-11-30 11:22:33", -9999999.57, -999999999999999.568, -9999999999999999999999999999999999.5679, "c_char:name1", "c_varchar:name1"),
(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ),
(3, false, 3, 30, 300, 3000, 30000, 370.37036, 70370.36703702, "2026-10-31", "2026-12-30 22:33:44", 37036.70, 370370367037.704, 37037036703703703670370.7037, "c_char:name3", "c_varchar:name3")
;


insert into tb_all_primitivetype_read_duplicate 
values 
(4, true, -4, -40, -400, -4000, -40000, -493.82715, -93827.15604936, "2019-10-31", "2019-12-30 22:33:44", -49382.27, -493827156050.272, -49382715604938271560494.2716, "c_char:name4", "c_varchar:name4")
;


CREATE TABLE `tb_all_primitivetype_read_unique` (
  `rowId` int(11) NULL COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_largeint` LARGEINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP 
UNIQUE KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
); 


insert into tb_all_primitivetype_read_unique 
values
(0, true, 127, 32767, 2147483647, 9223372036854775807, 99999999999999999999999999999999999999, -0, -0, "1900-01-01", "1800-11-20 12:40:39", 9999999.57, 999999999999999.568, 9999999999999999999999999999999999.5679, "c_char:name0", "c_varchar:name0"),
(1, false, -128, -32768, -2147483648, -9223372036854775808, -99999999999999999999999999999999999999, 123.45679, 23456.78901234, "4096-12-31", "4096-11-30 11:22:33", -9999999.57, -999999999999999.568, -9999999999999999999999999999999999.5679, "c_char:name1", "c_varchar:name1"),
(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ),
(3, false, 3, 30, 300, 3000, 30000, 370.37036, 70370.36703702, "2026-10-31", "2026-12-30 22:33:44", 37036.70, 370370367037.704, 37037036703703703670370.7037, "c_char:name3", "c_varchar:name3"),
(4, true, 123, 123, 123, 123, 123, 123, 123, "2000-1-1", "2000-1-1 1:1:1", 123, 123, 123, "123", "123")
;

insert into tb_all_primitivetype_read_unique 
values 
(4, true, -4, -40, -400, -4000, -40000, -493.82715, -93827.15604936, "2019-10-31", "2019-12-30 22:33:44", -49382.27, -493827156050.272, -49382715604938271560494.2716, "c_char:name4", "c_varchar:name4")
;



CREATE TABLE `tb_all_primitivetype_read_primary` (
  `rowId` int(11)  COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_largeint` LARGEINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
); 


insert into tb_all_primitivetype_read_primary 
values
(0, true, 127, 32767, 2147483647, 9223372036854775807, 99999999999999999999999999999999999999, -0, -0, "1900-01-01", "1800-11-20 12:40:39", 9999999.57, 999999999999999.568, 9999999999999999999999999999999999.5679, "c_char:name0", "c_varchar:name0"),
(1, false, -128, -32768, -2147483648, -9223372036854775808, -99999999999999999999999999999999999999, 123.45679, 23456.78901234, "4096-12-31", "4096-11-30 11:22:33", -9999999.57, -999999999999999.568, -9999999999999999999999999999999999.5679, "c_char:name1", "c_varchar:name1"),
(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ),
(3, false, 3, 30, 300, 3000, 30000, 370.37036, 70370.36703702, "2026-10-31", "2026-12-30 22:33:44", 37036.70, 370370367037.704, 37037036703703703670370.7037, "c_char:name3", "c_varchar:name3"),
(4, true, 123, 123, 123, 123, 123, 123, 123, "2000-1-1", "2000-1-1 1:1:1", 123, 123, 123, "123", "123")
;

insert into tb_all_primitivetype_read_primary 
values 
(4, true, 123, -40, 123, -4000, -40000, -493.82715, -93827.15604936, "2019-10-31", "2019-12-30 22:33:44", -49382.27, -493827156050.272, -49382715604938271560494.2716, "c_char:name4", "c_varchar:name4")
;

update tb_all_primitivetype_read_primary set c_tinyint = -4, c_int = -400 where rowId = 4;


DROP TABLE IF EXISTS tb_all_primitivetype_read_aggregate;
CREATE TABLE `tb_all_primitivetype_read_aggregate` (
  `rowId` int(11) NULL COMMENT "",
  `c_boolean` BOOLEAN REPLACE NULL COMMENT "",
  `c_tinyint` TINYINT MAX NULL COMMENT "",
  `c_smallint` SMALLINT MAX NULL COMMENT "",
  `c_int` int(11) SUM NULL COMMENT "",
  `c_bigint` BIGINT SUM NULL COMMENT "",
  `c_largeint` LARGEINT SUM NULL COMMENT "",
  `c_float` FLOAT SUM NULL COMMENT "",
  `c_double` DOUBLE SUM NULL COMMENT "",
  `c_date` DATE MAX NULL COMMENT "",
  `c_datetime` DATETIME MAX NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) MAX NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  MAX NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  SUM NULL COMMENT "",
  `c_char` CHAR(128)  MAX NULL COMMENT "",
  `c_varchar` VARCHAR(512)  MAX NULL COMMENT ""
) ENGINE=OLAP 
AGGREGATE KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
); 

insert into tb_all_primitivetype_read_aggregate 
values
(0, true, 127, 32767, 2147483647, 9223372036854775807, 99999999999999999999999999999999999999, -0, -0, "1900-01-01", "1800-11-20 12:40:39", 9999999.57, 999999999999999.568, 9999999999999999999999999999999999.5679, "c_char:name0", "c_varchar:name0"),
(1, false, -128, -32768, -2147483648, -9223372036854775808, -99999999999999999999999999999999999999, 123.45679, 23456.78901234, "4096-12-31", "4096-11-30 11:22:33", -9999999.57, -999999999999999.568, -9999999999999999999999999999999999.5679, "c_char:name1", "c_varchar:name1"),
(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ),
(3, false, 3, 30, 300, 3000, 30000, 370.37036, 70370.36703702, "2026-10-31", "2026-12-30 22:33:44", 37036.70, 370370367037.704, 37037036703703703670370.7037, "c_char:name3", "c_varchar:name3")
;

insert into tb_all_primitivetype_read_aggregate 
values 
(4, true, -100, -100, 0, 0, 0, 0, 0, "2000-10-31", "2000-12-30 22:33:44", -9999999, -999999999999999, 0, "a", "a")
;

insert into tb_all_primitivetype_read_aggregate 
values 
(4, true, -4, -40, -400, -4000, -40000, -493.82715, -93827.15604936, "2019-10-31", "2019-12-30 22:33:44", -49382.27, -493827156050.272, -49382715604938271560494.2716, "c_char:name4", "c_varchar:name4")
;




CREATE TABLE `tb_all_primitivetype_read_two_key_duplicate` (
  `rowId` int(11) NULL COMMENT "",
  `rowId2` int(11) NULL COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_largeint` LARGEINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(rowId, rowId2)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId, rowId2) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
); 


insert into tb_all_primitivetype_read_two_key_duplicate 
values
(0, 0, true, 127, 32767, 2147483647, 9223372036854775807, 99999999999999999999999999999999999999, -0, -0, "1900-01-01", "1800-11-20 12:40:39", 9999999.57, 999999999999999.568, 9999999999999999999999999999999999.5679, "c_char:name0", "c_varchar:name0"),
(1, 1, false, -128, -32768, -2147483648, -9223372036854775808, -99999999999999999999999999999999999999, 123.45679, 23456.78901234, "4096-12-31", "4096-11-30 11:22:33", -9999999.57, -999999999999999.568, -9999999999999999999999999999999999.5679, "c_char:name1", "c_varchar:name1"),
(2, 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ),
(3, 3, false, 3, 30, 300, 3000, 30000, 370.37036, 70370.36703702, "2026-10-31", "2026-12-30 22:33:44", 37036.70, 370370367037.704, 37037036703703703670370.7037, "c_char:name3", "c_varchar:name3")
;


insert into tb_all_primitivetype_read_two_key_duplicate 
values 
(4, 4, true, -4, -40, -400, -4000, -40000, -493.82715, -93827.15604936, "2019-10-31", "2019-12-30 22:33:44", -49382.27, -493827156050.272, -49382715604938271560494.2716, "c_char:name4", "c_varchar:name4")
;


CREATE TABLE `tb_all_primitivetype_read_two_key_unique` (
  `rowId` int(11) NULL COMMENT "",
  `rowId2` int(11) NULL COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_largeint` LARGEINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP 
UNIQUE KEY(rowId, rowId2)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId, rowId2) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
); 


insert into tb_all_primitivetype_read_two_key_unique 
values
(0, 0, true, 127, 32767, 2147483647, 9223372036854775807, 99999999999999999999999999999999999999, -0, -0, "1900-01-01", "1800-11-20 12:40:39", 9999999.57, 999999999999999.568, 9999999999999999999999999999999999.5679, "c_char:name0", "c_varchar:name0"),
(1, 1, false, -128, -32768, -2147483648, -9223372036854775808, -99999999999999999999999999999999999999, 123.45679, 23456.78901234, "4096-12-31", "4096-11-30 11:22:33", -9999999.57, -999999999999999.568, -9999999999999999999999999999999999.5679, "c_char:name1", "c_varchar:name1"),
(2, 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ),
(3, 3, false, 3, 30, 300, 3000, 30000, 370.37036, 70370.36703702, "2026-10-31", "2026-12-30 22:33:44", 37036.70, 370370367037.704, 37037036703703703670370.7037, "c_char:name3", "c_varchar:name3"),
(4, 4, true, 123, 123, 123, 123, 123, 123, 123, "2000-1-1", "2000-1-1 1:1:1", 123, 123, 123, "123", "123")
;

insert into tb_all_primitivetype_read_two_key_unique 
values 
(4, 4, true, -4, -40, -400, -4000, -40000, -493.82715, -93827.15604936, "2019-10-31", "2019-12-30 22:33:44", -49382.27, -493827156050.272, -49382715604938271560494.2716, "c_char:name4", "c_varchar:name4")
;



CREATE TABLE `tb_all_primitivetype_read_two_key_primary` (
  `rowId` int(11)  COMMENT "",
  `rowId2` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_largeint` LARGEINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(rowId, rowId2)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId, rowId2) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
); 


insert into tb_all_primitivetype_read_two_key_primary 
values
(0, 0, true, 127, 32767, 2147483647, 9223372036854775807, 99999999999999999999999999999999999999, -0, -0, "1900-01-01", "1800-11-20 12:40:39", 9999999.57, 999999999999999.568, 9999999999999999999999999999999999.5679, "c_char:name0", "c_varchar:name0"),
(1, 1, false, -128, -32768, -2147483648, -9223372036854775808, -99999999999999999999999999999999999999, 123.45679, 23456.78901234, "4096-12-31", "4096-11-30 11:22:33", -9999999.57, -999999999999999.568, -9999999999999999999999999999999999.5679, "c_char:name1", "c_varchar:name1"),
(2, 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ),
(3, 3, false, 3, 30, 300, 3000, 30000, 370.37036, 70370.36703702, "2026-10-31", "2026-12-30 22:33:44", 37036.70, 370370367037.704, 37037036703703703670370.7037, "c_char:name3", "c_varchar:name3"),
(4, 4, true, 123, 123, 123, 123, 123, 123, 123, "2000-1-1", "2000-1-1 1:1:1", 123, 123, 123, "123", "123")
;

insert into tb_all_primitivetype_read_two_key_primary 
values 
(4, 4, true, 123, -40, 123, -4000, -40000, -493.82715, -93827.15604936, "2019-10-31", "2019-12-30 22:33:44", -49382.27, -493827156050.272, -49382715604938271560494.2716, "c_char:name4", "c_varchar:name4")
;

update tb_all_primitivetype_read_two_key_primary set c_tinyint = -4, c_int = -400 where rowId = 4;


DROP TABLE IF EXISTS tb_all_primitivetype_read_two_key_aggregate;
CREATE TABLE `tb_all_primitivetype_read_two_key_aggregate` (
  `rowId` int(11) NULL COMMENT "",
  `rowId2` int(11) NULL COMMENT "",
  `c_boolean` BOOLEAN REPLACE NULL COMMENT "",
  `c_tinyint` TINYINT MAX NULL COMMENT "",
  `c_smallint` SMALLINT MAX NULL COMMENT "",
  `c_int` int(11) SUM NULL COMMENT "",
  `c_bigint` BIGINT SUM NULL COMMENT "",
  `c_largeint` LARGEINT SUM NULL COMMENT "",
  `c_float` FLOAT SUM NULL COMMENT "",
  `c_double` DOUBLE SUM NULL COMMENT "",
  `c_date` DATE MAX NULL COMMENT "",
  `c_datetime` DATETIME MAX NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) MAX NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  MAX NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  SUM NULL COMMENT "",
  `c_char` CHAR(128)  MAX NULL COMMENT "",
  `c_varchar` VARCHAR(512)  MAX NULL COMMENT ""
) ENGINE=OLAP 
AGGREGATE KEY(rowId, rowId2)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId, rowId2) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
); 

insert into tb_all_primitivetype_read_two_key_aggregate 
values
(0, 0, true, 127, 32767, 2147483647, 9223372036854775807, 99999999999999999999999999999999999999, -0, -0, "1900-01-01", "1800-11-20 12:40:39", 9999999.57, 999999999999999.568, 9999999999999999999999999999999999.5679, "c_char:name0", "c_varchar:name0"),
(1, 1, false, -128, -32768, -2147483648, -9223372036854775808, -99999999999999999999999999999999999999, 123.45679, 23456.78901234, "4096-12-31", "4096-11-30 11:22:33", -9999999.57, -999999999999999.568, -9999999999999999999999999999999999.5679, "c_char:name1", "c_varchar:name1"),
(2, 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ),
(3, 3, false, 3, 30, 300, 3000, 30000, 370.37036, 70370.36703702, "2026-10-31", "2026-12-30 22:33:44", 37036.70, 370370367037.704, 37037036703703703670370.7037, "c_char:name3", "c_varchar:name3")
;

insert into tb_all_primitivetype_read_two_key_aggregate 
values 
(4, 4, true, -100, -100, 0, 0, 0, 0, 0, "2000-10-31", "2000-12-30 22:33:44", -9999999, -999999999999999, 0, "a", "a")
;

insert into tb_all_primitivetype_read_two_key_aggregate 
values 
(4, 4, true, -4, -40, -400, -4000, -40000, -493.82715, -93827.15604936, "2019-10-31", "2019-12-30 22:33:44", -49382.27, -493827156050.272, -49382715604938271560494.2716, "c_char:name4", "c_varchar:name4")
;


CREATE TABLE `tb_json_two_key_unique` (
  `rowId` int(11) NULL COMMENT "",
  `rowId2` int(11) NULL COMMENT "",
  `c_varchar` varchar(512) NULL COMMENT "",
  `c_long_varchar` varchar(1048576) NULL COMMENT "",
  `c_json` json NULL COMMENT ""
) ENGINE=OLAP
UNIQUE KEY(`rowId`, `rowId2`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`rowId`, `rowId2`) BUCKETS 3
PROPERTIES (
"replication_num" = "1",
"datacache.enable" = "true",
"storage_volume" = "minio_volume",
"enable_async_write_back" = "false",
"enable_persistent_index" = "false",
"compression" = "LZ4"
);


CREATE TABLE `tb_json_two_key_primary` (
  `rowId` int(11)  COMMENT "",
  `rowId2` int(11)  COMMENT "",
  `c_varchar` varchar(512) NULL COMMENT "",
  `c_long_varchar` varchar(1048576) NULL COMMENT "",
  `c_json` json NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`rowId`, `rowId2`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`rowId`, `rowId2`) BUCKETS 3
PROPERTIES (
"replication_num" = "1",
"datacache.enable" = "true",
"storage_volume" = "minio_volume",
"enable_async_write_back" = "false",
"enable_persistent_index" = "false",
"compression" = "LZ4"
);


CREATE TABLE `tb_binary_two_key_duplicate` (
  `rowId` int(11) NULL COMMENT "",
  `rowId2` int(11) NULL COMMENT "",
  `c_varbinary` varbinary NULL COMMENT "",
  `c_binary` binary NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`rowId`, `rowId2`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`rowId`, `rowId2`) BUCKETS 3 
PROPERTIES (
"replication_num" = "1"
); 

CREATE TABLE `tb_binary_two_key_unique` (
  `rowId` int(11) NULL COMMENT "",
  `rowId2` int(11) NULL COMMENT "",
  `c_varbinary` varbinary NULL COMMENT "",
  `c_binary` binary NULL COMMENT ""
) ENGINE=OLAP 
UNIQUE KEY(`rowId`, `rowId2`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`rowId`, `rowId2`) BUCKETS 3 
PROPERTIES (
"replication_num" = "1"
); 

CREATE TABLE `tb_binary_two_key_primary` (
  `rowId` int(11)  COMMENT "",
  `rowId2` int(11)  COMMENT "",
  `c_varbinary` varbinary NULL COMMENT "",
  `c_binary` binary NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`rowId`, `rowId2`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`rowId`, `rowId2`) BUCKETS 3 
PROPERTIES (
"replication_num" = "1"
); 

CREATE TABLE `tb_part_read_two_key_unique` (
  `rowId` varchar(65535) NOT NULL COMMENT "",
  `part_int` int NOT NULL COMMENT "",
  `part_date` date NOT NULL COMMENT "",
  `rowId2` bigint NOT NULL COMMENT "",
  `c_boolean` boolean NULL COMMENT "",
  `c_tinyint` tinyint(4) NULL COMMENT "",
  `c_smallint` smallint(6) NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` bigint(20) NULL COMMENT "",
  `c_largeint` largeint(40) NULL COMMENT "",
  `c_float` float NULL COMMENT "",
  `c_double` double NULL COMMENT "",
  `c_date` date NULL COMMENT "",
  `c_datetime` datetime NULL COMMENT "",
  `c_decimal32` decimal(9, 2) NULL COMMENT "",
  `c_decimal64` decimal(18, 3) NULL COMMENT "",
  `c_decimal128` decimal(38, 4) NULL COMMENT "",
  `c_char` char(128) NULL COMMENT "",
  `c_varchar` varchar(512) NULL COMMENT ""
) ENGINE=OLAP 
UNIQUE KEY( `rowId`, part_int, part_date,  `rowId2`)
COMMENT "OLAP"
PARTITION BY (part_int, part_date)
DISTRIBUTED BY HASH(`rowId`, `rowId2`) BUCKETS 3 
PROPERTIES (
"replication_num" = "1"
);


insert into tb_part_read_two_key_unique 
values
( 0, 1, "2024-05-01",  0, true, 127, 32767, 2147483647, 9223372036854775807, 99999999999999999999999999999999999999, -0, -0, "1900-01-01", "1800-11-20 12:40:39", 9999999.57, 999999999999999.568, 9999999999999999999999999999999999.5679, "c_char:name0", "c_varchar:name0"),
( 1, 2, "2024-06-01",  1, false, -128, -32768, -2147483648, -9223372036854775808, -99999999999999999999999999999999999999, 123.45679, 23456.78901234, "4096-12-31", "4096-11-30 11:22:33", -9999999.57, -999999999999999.568, -9999999999999999999999999999999999.5679, "c_char:name1", "c_varchar:name1"),
( 2, 2, "2024-06-02",  2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ),
( 3, 2, "2024-06-03",  3, false, 3, 30, 300, 3000, 30000, 370.37036, 70370.36703702, "2026-10-31", "2026-12-30 22:33:44", 37036.70, 370370367037.704, 37037036703703703670370.7037, "c_char:name3", "c_varchar:name3"),
( 4, 2, "2024-06-04",  4, true, 123, 123, 123, 123, 123, 123, 123, "2000-1-1", "2000-1-1 1:1:1", 123, 123, 123, "123", "123")
;

insert into tb_part_read_two_key_unique 
values 
(4, 2, "2024-06-04", 4, true, -4, -40, -400, -4000, -40000, -493.82715, -93827.15604936, "2019-10-31", "2019-12-30 22:33:44", -49382.27, -493827156050.272, -49382715604938271560494.2716, "c_char:name4", "c_varchar:name4")
;




CREATE TABLE `tb_part_read_part_key_first` (
  `part_date` varchar(65535) NOT NULL COMMENT "",
  `part_int` int NOT NULL COMMENT "",
  `rowId` varchar(65535) NOT NULL COMMENT "",
  `rowId2` bigint NOT NULL COMMENT "",
  `c_boolean` boolean NULL COMMENT "",
  `c_tinyint` tinyint(4) NULL COMMENT "",
  `c_smallint` smallint(6) NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` bigint(20) NULL COMMENT "",
  `c_largeint` largeint(40) NULL COMMENT "",
  `c_float` float NULL COMMENT "",
  `c_double` double NULL COMMENT "",
  `c_date` date NULL COMMENT "",
  `c_datetime` datetime NULL COMMENT "",
  `c_decimal32` decimal(9, 2) NULL COMMENT "",
  `c_decimal64` decimal(18, 3) NULL COMMENT "",
  `c_decimal128` decimal(38, 4) NULL COMMENT "",
  `c_char` char(128) NULL COMMENT "",
  `c_varchar` varchar(512) NULL COMMENT ""
) ENGINE=OLAP 
UNIQUE KEY(  part_date, part_int,`rowId`,  `rowId2`)
COMMENT "OLAP"
PARTITION BY (part_date , part_int)
DISTRIBUTED BY HASH(`rowId`, `rowId2`) BUCKETS 3 
PROPERTIES (
"replication_num" = "1"
);


insert into tb_part_read_part_key_first 
values
( "2024-05-01", 1,  0,  0, true, 127, 32767, 2147483647, 9223372036854775807, 99999999999999999999999999999999999999, -0, -0, "1900-01-01", "1800-11-20 12:40:39", 9999999.57, 999999999999999.568, 9999999999999999999999999999999999.5679, "c_char:name0", "c_varchar:name0"),
( "2024-06-01", 2,  1,  1, false, -128, -32768, -2147483648, -9223372036854775808, -99999999999999999999999999999999999999, 123.45679, 23456.78901234, "4096-12-31", "4096-11-30 11:22:33", -9999999.57, -999999999999999.568, -9999999999999999999999999999999999.5679, "c_char:name1", "c_varchar:name1"),
( "2024-06-02", 2,  2,  2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ),
( "2024-06-03", 2,  3,  3, false, 3, 30, 300, 3000, 30000, 370.37036, 70370.36703702, "2026-10-31", "2026-12-30 22:33:44", 37036.70, 370370367037.704, 37037036703703703670370.7037, "c_char:name3", "c_varchar:name3"),
( "2024-06-04", 2,  4,  4, true, 123, 123, 123, 123, 123, 123, 123, "2000-1-1", "2000-1-1 1:1:1", 123, 123, 123, "123", "123")
;



 CREATE TABLE `tb_bitmap_hll_two_key_primary` (
  `rowId` int(11) NOT NULL COMMENT "",
  `rowId2` int(11) NOT NULL COMMENT "",
  `c_bitmap` bitmap NULL COMMENT "",
  `c_hll` HLL NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`rowId`, `rowId2`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`rowId`, `rowId2`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

insert into tb_bitmap_hll_two_key_primary values
(0, 0, to_bitmap(0), HLL_EMPTY()),
(1, 1, to_bitmap(1000), HLL_HASH(1000)),
(2, 2, NULL, NULL),
(3, 3, to_bitmap(3000), HLL_HASH(3000))
;



 CREATE TABLE `tb_bitmap_hll_two_key_unique` (
  `rowId` int(11) NOT NULL COMMENT "",
  `rowId2` int(11) NOT NULL COMMENT "",
  `c_bitmap` bitmap NULL COMMENT "",
  `c_hll` HLL NULL COMMENT ""
) ENGINE=OLAP 
UNIQUE KEY(`rowId`, `rowId2`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`rowId`, `rowId2`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);


insert into tb_bitmap_hll_two_key_unique values
(0, 0, to_bitmap(0), HLL_EMPTY()),
(1, 1, to_bitmap(1000), HLL_HASH(1000)),
(2, 2, NULL, NULL),
(3, 3, to_bitmap(3000), HLL_HASH(3000))
;




 CREATE TABLE `tb_bitmap_hll_two_key_aggregate` (
  `rowId` int(11) NOT NULL COMMENT "",
  `rowId2` int(11) NOT NULL COMMENT "",
  `c_bitmap` bitmap BITMAP_UNION NULL COMMENT "",
  `c_hll` HLL hll_union NULL COMMENT ""
) ENGINE=OLAP 
AGGREGATE KEY(`rowId`, `rowId2`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`rowId`, `rowId2`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

insert into tb_bitmap_hll_two_key_aggregate values
(0, 0, to_bitmap(0), HLL_EMPTY()),
(1, 1, to_bitmap(1000), HLL_HASH(1000)),
(2, 2, NULL, NULL),
(3, 3, to_bitmap(3000), HLL_HASH(3000))
;

CREATE TABLE tb_read_map_array_struct(
  `rowId` int(11) NOT NULL COMMENT "",
  `rowId2` int(11) NOT NULL COMMENT "",
  `c_map` MAP<INT, VARCHAR(1024)> NULL COMMENT "",
  `c_array` ARRAY<INT> NULL COMMENT "",
  `c_struct` STRUCT<a INT, b INT, c VARCHAR(1024), d DATE> NULL COMMENT ""
)
PRIMARY KEY(`rowId`, `rowId2`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`rowId`, `rowId2`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1"
);


insert into tb_read_map_array_struct values 
  (0, 0, map<INT, VARCHAR(1024)>{}, [],
   named_struct('a', 2147483647,'b', 2147483647, 'c', 'c:name0,d:1', 'd', '1900-01-01')
  ),
  (1, 1, map<INT, VARCHAR(1024)>{100:'mapvalue:100'},  [100], 
   named_struct('a',-2147483648,'b', -2147483648, 'c', 'c:name1,d:1', 'd', '4096-12-31')
  ),
  (2, 2, NULL, NULL, NULL),
  (3, 3, map<INT, VARCHAR(1024)>{300:'mapvalue:300',301:'mapvalue:301',302:'mapvalue:302'}, [300,301,302],
   named_struct('a', 301, 'b', 301,'c', 'c:name3,d:1', 'd', '2027-10-31')
  )
  ;


CREATE TABLE tb_map_array_struct(
  `rowId` int(11) NOT NULL COMMENT "",
  `rowId2` int(11) NOT NULL COMMENT "",
  `c_map` MAP<INT, VARCHAR(1024)> NULL COMMENT "",
  `c_array` ARRAY<INT> NULL COMMENT "",
  `c_struct` STRUCT<a INT, b INT, c VARCHAR(1024), d DATE> NULL COMMENT "",
  `c_struct2` STRUCT<a INT, b STRUCT<c INT, d INT>, e MAP<INT, VARCHAR(1024)>, f ARRAY<INT>> NULL COMMENT ""
)
PRIMARY KEY(`rowId`, `rowId2`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`rowId`, `rowId2`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1"
);
