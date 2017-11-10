Project:find top-k subroots by title
功能:根据商品的title找到top-k个最可能所属的商品类别
方法:根据已有的商品title及其类别,采用TF-IDF计算每个类别中的词语权重.
对于新的商品title,首先对其分词,然后关键词在各个类别的权重,计算属于每个类别的得分,返回top-k个得分最高类别.
TF:subroot中包含该关键词的title数量/subroot中所有的title数量
IDF:log(所有的subroot数量/(包含该关键词的subroot数量+1))
注:商品title的分词采用jieba的全索引模式分词,并使用自己的词库.

【Step1】:根据taobao的subroot_name找到DCG的subroot_name
set mapreduce.map.memory.mb=2048;
set mapreduce.map.java.opts=-Xmx1600m;
DROP TABLE tmp_kgs_newrootsubroot_title_set;
CREATE TABLE tmp_kgs_newrootsubroot_title_set AS
SELECT 
t2.newroot AS root_name,
t2.newsubroot AS subroot_name,
t1.title_set
FROM
    (SELECT *
    FROM idl_limao_cid_dim
    WHERE ds='2017-06-26'
    ) t1
LEFT JOIN config_newcid_dim t2
ON t1.root_name=t2.root_name 
AND t1.subroot_name=t2.subroot_name
WHERE t2.root_name IS NOT NULL
AND   t2.subroot_name IS NOT NULL;

【Step2】:将title_set展开,找到每个title_id对应的'root_name&subroot_name'
DROP TABLE tmp_kgs_newrootsubroot_title_id;
CREATE TABLE tmp_kgs_newrootsubroot_title_id AS
SELECT
DISTINCT CONCAT_WS('&',root_name,subroot_name) AS rootsubroot,
title_id
FROM tmp_kgs_newrootsubroot_title_set
LATERAL VIEW explode(title_set) mytable AS title_id
WHERE root_name IS NOT NULL 
AND subroot_name IS NOT NULL;

【Step3】:找到每个title_id对应的jieba分词
DROP TABLE tmp_kgs_rootsubroot_token;
CREATE TABLE tmp_kgs_rootsubroot_token AS
SELECT
DISTINCT t1.rootsubroot,
t1.title_id,
t2.jieba_token
FROM 
    (SELECT
    rootsubroot,title_id
    FROM tmp_kgs_newrootsubroot_title_id
    ) t1
LEFT JOIN
    (SELECT 
    title_id,jieba_token
    FROM idl_title_split_agg
    WHERE ds='2017-10-20'
    ) t2
ON t1.title_id=t2.title_id
WHERE t2.title_id IS NOT NULL;

【Step4】:抽取300000条数据作为样本,采用TF-IDF训练每个别类中词语的权重
insert overwrite local directory '/home/kangguosheng/tmp'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '\073'
MAP KEYS TERMINATED BY '\072'
STORED AS TEXTFILE
select rootsubroot,jieba_token
from tmp_kgs_rootsubroot_token
order by rand()
limit 300000;

python_code:subroot_keyword_tfidf.py
output:
keywords文件--所有的关键词
subroots文件--subroot及其关键词的数量(不去重)
subroot_keyword_tfidf--subroot中每个keyword的tf*idf值
load data local inpath '/home/kangguosheng/filetransfer/subroot_keyword_tfidf' 
overwrite into table tmp_kgs_subroot_keyword_tfidf partition(ds='2017-11-10');

-- 将subroot_keyword_tfidf导入hive表
DROP table  tmp_kgs_subroot_keyword_tfidf;
CREATE TABLE if not exists tmp_kgs_subroot_keyword_tfidf
(
subroot_name STRING,
keyword      STRING,
tfidf        FLOAT
) 
comment "subroot_keyword_tfidf"
PARTITIONED BY (ds STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
COLLECTION ITEMS TERMINATED BY '\073'
MAP KEYS TERMINATED BY '\072'
STORED AS TEXTFILE;

【Step5】:抽取100000条数据作为测试样本,测试分类的准确度
sql_code:tmp_kgs_title_subroot_token_test.sql
evaluation:
top-3总体预测精度:0.913632226900421
top-5总体预测精度:0.9556160054404896

【Step6】:查找测试失败的样本数据,并进行观察
sql_code:tmp_kgs_title_subroot_test_failure_data.sql

【Step7】:实现基于商品title,返回top-k商品类别的Python程序接口
文件夹:find_subroots

★★经验总结★★:先采用小数据测试程序的准确性是很重要的,一方面保证程序能跑通,另一方面尽量保证逻辑正确.否则,一开始就在大数据集上运行程序,可能运行了半天,最后程序却中断了;或者程序逻辑有问题,导致程序白跑了,结果无效,这样就会耽误很多不必要的时间.



