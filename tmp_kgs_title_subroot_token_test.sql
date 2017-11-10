--抽取100000测试title
DROP TABLE tmp_kgs_title_subroot_token_test;
CREATE TABLE tmp_kgs_title_subroot_token_test AS
select 
title_id,
rootsubroot AS subroot_name,
jieba_token
from tmp_kgs_rootsubroot_token
order by rand()
limit 100000;

--计算title在每个subroot_name下的得分
set mapreduce.map.memory.mb=2048;
set mapreduce.map.java.opts=-Xmx1600m;
DROP TABLE tmp_kgs_title_subroot_test_score;
CREATE TABLE tmp_kgs_title_subroot_test_score AS
SELECT
t1.title_id,
t2.subroot_name,
SUM(t2.tfidf) AS tfidf_score
FROM
    (SELECT
    title_id,
    subroot_name,
    keyword
    FROM tmp_kgs_title_subroot_token_test
    LATERAL VIEW explode(jieba_token) mytable AS keyword
    ) t1
LEFT JOIN 
    (SELECT *
    FROM tmp_kgs_subroot_keyword_tfidf
    WHERE ds="2017-11-10"
    ) t2
ON t1.keyword=t2.keyword
WHERE t2.keyword IS NOT NULL
GROUP BY t1.title_id,t2.subroot_name;

--预测每个title所在subroot_name
set mapreduce.map.memory.mb=2048;
set mapreduce.map.java.opts=-Xmx1600m;
DROP TABLE tmp_kgs_title_subroot_test_predict_3;
CREATE TABLE tmp_kgs_title_subroot_test_predict_3 AS
SELECT 
title_id,
subroot_name AS predict_subroot_name,
tfidf_score  AS max_tfidf_score
FROM
    (SELECT
    title_id,
    subroot_name,
    tfidf_score,
    row_number()over(Partition BY title_id ORDER BY tfidf_score DESC) AS ranks
    FROM tmp_kgs_title_subroot_test_score
    ) t1
WHERE ranks<=3;

--统计每个subroot_name下的预测精度
set mapreduce.map.memory.mb=2048;
set mapreduce.map.java.opts=-Xmx1600m;
DROP TABLE tmp_kgs_title_subroot_test_accuracy_3;
CREATE TABLE tmp_kgs_title_subroot_test_accuracy_3 AS
SELECT
subroot_name,
COUNT(DISTINCT title_id) AS subroot_num,
SUM(CASE WHEN subroot_name=predict_subroot_name THEN 1 ELSE 0 END) AS accuracy_num,
SUM(CASE WHEN subroot_name=predict_subroot_name THEN 1 ELSE 0 END)/COUNT(DISTINCT title_id) AS accuracy
FROM
    (SELECT
    t1.title_id,
    t1.subroot_name,
    t2.predict_subroot_name
    FROM tmp_kgs_title_subroot_token_test t1
    LEFT JOIN tmp_kgs_title_subroot_test_predict_3 t2
    ON t1.title_id=t2.title_id
    WHERE t2.title_id IS NOT NULL
    ) t3
GROUP BY t3.subroot_name;

-- 总体预测精度:0.913632226900421
SELECT
SUM(accuracy_num)/SUM(subroot_num) AS total_accuracy
FROM tmp_kgs_title_subroot_test_accuracy_3;
-- 总体预测精度:0.9556160054404896
SELECT
SUM(accuracy_num)/SUM(subroot_num) AS total_accuracy
FROM tmp_kgs_title_subroot_test_accuracy_5;

-- 导出每个类别的预测精度
set mapreduce.job.reduces=1;
insert overwrite local directory '/home/kangguosheng/tmp'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '\073'
MAP KEYS TERMINATED BY '\072'
STORED AS TEXTFILE
SELECT *
FROM tmp_kgs_title_subroot_test_accuracy_5
ORDER BY accuracy DESC;
---------------------------------------------------------------------------
--预测每个title所在subroot_name
set mapreduce.map.memory.mb=2048;
set mapreduce.map.java.opts=-Xmx1600m;
DROP TABLE tmp_kgs_title_subroot_test_predict_5;
CREATE TABLE tmp_kgs_title_subroot_test_predict_5 AS
SELECT 
title_id,
subroot_name AS predict_subroot_name,
tfidf_score  AS max_tfidf_score
FROM
    (SELECT
    title_id,
    subroot_name,
    tfidf_score,
    row_number()over(Partition BY title_id ORDER BY tfidf_score DESC) AS ranks
    FROM tmp_kgs_title_subroot_test_score
    ) t1
WHERE ranks<=5;

--统计每个subroot_name下的预测精度
set mapreduce.map.memory.mb=2048;
set mapreduce.map.java.opts=-Xmx1600m;
DROP TABLE tmp_kgs_title_subroot_test_accuracy_5;
CREATE TABLE tmp_kgs_title_subroot_test_accuracy_5 AS
SELECT
subroot_name,
COUNT(DISTINCT title_id) AS subroot_num,
SUM(CASE WHEN subroot_name=predict_subroot_name THEN 1 ELSE 0 END) AS accuracy_num,
SUM(CASE WHEN subroot_name=predict_subroot_name THEN 1 ELSE 0 END)/COUNT(DISTINCT title_id) AS accuracy
FROM
    (SELECT
    t1.title_id,
    t1.subroot_name,
    t2.predict_subroot_name
    FROM tmp_kgs_title_subroot_token_test t1
    LEFT JOIN tmp_kgs_title_subroot_test_predict_5 t2
    ON t1.title_id=t2.title_id
    WHERE t2.title_id IS NOT NULL
    ) t3
GROUP BY t3.subroot_name;