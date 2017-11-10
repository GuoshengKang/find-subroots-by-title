--统计每个subroot_name下的预测精度
-- 91355条分类准确的title_id
set mapreduce.map.memory.mb=2048;
set mapreduce.map.java.opts=-Xmx1600m;
DROP TABLE tmp_kgs_title_subroot_test_success;
CREATE TABLE tmp_kgs_title_subroot_test_success AS
SELECT
t1.title_id,
t1.subroot_name,
t2.predict_subroot_name
FROM tmp_kgs_title_subroot_token_test t1
LEFT JOIN tmp_kgs_title_subroot_test_predict_3 t2
ON t1.title_id=t2.title_id AND t1.subroot_name=t2.predict_subroot_name
WHERE t2.title_id IS NOT NULL;

-- 8644条分类不准确的title_id
DROP TABLE tmp_kgs_title_subroot_test_failure;
CREATE TABLE tmp_kgs_title_subroot_test_failure AS
SELECT
t1.title_id,
t1.subroot_name,
collect_set(t2.predict_subroot_name) AS predict_subroot_names,
t1.jieba_token
FROM 
    (SELECT
    s1.title_id,
    s1.subroot_name,
    s1.jieba_token
    FROM tmp_kgs_title_subroot_token_test s1
    LEFT JOIN tmp_kgs_title_subroot_test_success s2
    ON s1.title_id=s2.title_id
    WHERE s2.title_id IS NULL
    ) t1
LEFT JOIN tmp_kgs_title_subroot_test_predict_3 t2
ON t1.title_id=t2.title_id
GROUP BY t1.title_id,t1.subroot_name,t1.jieba_token;