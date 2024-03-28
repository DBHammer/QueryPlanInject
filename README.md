# QueryPlanInject

> ##### 本项目旨在提取主流 DBMS 的查询计划，包括 Join Order 和 Join Operator 的类型，并生成对应 DBMS 的 Query Hint

## 效果

目前已实现的功能是提取 PostgreSQL，TiDB 和 OceanBase 的查询计划，包括：

- Join Order：以 list 形式先后顺序排列
- Physical Operator：目前只提取 Join 的算子，主要包含两个连接的表，预估的基数，真实的基数（由于是 explain 获取，暂时这一项由预估的基数赋值），连接物理类型

然后生成 Query Hint 输入给 OceanBase，执行查询获悉 Cost 和 Latency，比较其是否满足偏序以及是否有加速比性能提升



## TODO

1. 我暂时没有验证是否可行，在 test 里增加 junit单元测试，写大概 10-20 个样例测试注入 Hint 是否起效果，因为未来可能一下跑几百个，需要保证逻辑严格正确
2. 目前只写了其他数据库注入到 OceanBase 里的 Query Hint 注入，其他数据库的执行逻辑没有写，且 Query Hint 是否完全合理没有确认
3.  Main 函数里满足偏序关系以后的每个算子的基数 Q-error 没有统计
4. 目前Physical Operator包含两个连接的表有个比较大的问题是，从查询计划里不太容易提取出到底是哪两个表连接，比如对于 OceanBase 来说，只能知道 d 和 c 先 join 了，但是 b 是和 d 有连接条件还是与c 有连接条件，查询计划一般不知道，现在的写法是按照新加入的表默认和列表里的前一个表进行连接。所以这里需要补充逻辑最好从 Artemis 本身 SQL 的语义结构里直接提取关系，比如 SELECT * from t1, t2 where t1.id = t2.id, 这样就知道 t1 和  t2 有连接条件了。



## 参考链接

1. pg_hint_plan: https://github.com/ossc-db/pg_hint_plan/blob/master/docs/hint_list.md
2. oceanbase query hint: https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000000035691
3. tidb query hint:https://docs.pingcap.com/zh/tidb/stable/optimizer-hints

