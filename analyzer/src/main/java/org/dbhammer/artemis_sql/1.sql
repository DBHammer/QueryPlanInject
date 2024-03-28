select count(*) as result
from table_1,
     table_2,
     table_3,
     table_4,
     table_5,
     table_6,
     table_7
where table_3.fk_3 = table_2.primaryKey
  and table_3.fk_0 = table_5.primaryKey
  and table_2.fk_6 = table_4.primaryKey
  and table_4.fk_4 = table_6.primaryKey
  and table_5.fk_2 = table_7.primaryKey
  and table_6.fk_5 = table_1.primaryKey
  and table_3.col_2 between 805152.8892859903693152 and 1371080.6539595037674712
  and table_2.col_1 not in (605236, 474084, 503070, 554259)
  and table_5.col_6 > 663759.0426306412552409
  and table_4.col_9 in (665403, 666955, 676750, 681870, 678555, 668917, 671826, 670146)
  and table_6.col_6 >= 677279.9921799572130953
  and table_7.col_7 < 677377.376977851837018556
  and table_1.col_7 not in (677362, 677375, 677368, 677363);