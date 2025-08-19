set hive.exec.mode.local.auto=True;
create database if not exists tms;
use tms;
SET hive.exec.parallel=true; -- 允许并行执行UNION等操作
SET hive.exec.parallel.thread.number=2; -- 并行线程数
SET hive.exec.dynamic.partition.mode=nonstrict;


