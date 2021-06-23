-- Hive 开窗函数

-- 普通的聚合函数聚合的行集是组,开窗函数聚合的行集是窗口。因此,普通的聚合函数每组(Group by)只返回一个值，而开窗--- 可为窗口中的每行都返回一个值。简单理解，就是对查询的结果多出一列，这一列可以是聚合值，也可以是排序值。
-- 开窗函数一般分为两类,聚合开窗函数和排序开窗函数。

--测试数据

-- 建表
create table student_scores(
id int,
studentId int,
language int,
math int,
english int,
classId string,
departmentId string
);
-- 写入数据
insert into table student_scores values
  (1,111,68,69,90,'class1','department1'),
  (2,112,73,80,96,'class1','department1'),
  (3,113,90,74,75,'class1','department1'),
  (4,114,89,94,93,'class1','department1'),
  (5,115,99,93,89,'class1','department1'),
  (6,121,96,74,79,'class2','department1'),
  (7,122,89,86,85,'class2','department1'),
  (8,123,70,78,61,'class2','department1'),
  (9,124,76,70,76,'class2','department1'),
  (10,211,89,93,60,'class1','department2'),
  (11,212,76,83,75,'class1','department2'),
  (12,213,71,94,90,'class1','department2'),
  (13,214,94,94,66,'class1','department2'),
  (14,215,84,82,73,'class1','department2'),
  (15,216,85,74,93,'class1','department2'),
  (16,221,77,99,61,'class2','department2'),
  (17,222,80,78,96,'class2','department2'),
  (18,223,79,74,96,'class2','department2'),
  (19,224,75,80,78,'class2','department2'),
  (20,225,82,85,63,'class2','department2');

  --聚合开窗函数
  --count开窗函数

  -- count 开窗函数

  select studentId,math,departmentId,classId,
  -- 以符合条件的所有行作为窗口
  count(math) over() as count1,
   -- 以按classId分组的所有行作为窗口
  count(math) over(partition by classId) as count2,
   -- 以按classId分组、按math排序的所有行作为窗口
  count(math) over(partition by classId order by math) as count3,
   -- 以按classId分组、按math排序、按 当前行+往前1行+往后2行的行作为窗口
  count(math) over(partition by classId order by math rows between 1 preceding and 2 following) as count4
  from student_scores where departmentId='department1';

  --结果
  studentid   math    departmentid    classid count1  count2  count3  count4
  111         69      department1     class1  9       5       1       3
  113         74      department1     class1  9       5       2       4
  112         80      department1     class1  9       5       3       4
  115         93      department1     class1  9       5       4       3
  114         94      department1     class1  9       5       5       2
  124         70      department1     class2  9       4       1       3
  121         74      department1     class2  9       4       2       4
  123         78      department1     class2  9       4       3       3
  122         86      department1     class2  9       4       4       2

  --结果解释:
  --studentid=115,count1为所有的行数9,count2为分区class1中的行数5,count3为分区class1中math值<=93的行数4,
  --count4为分区class1中math值向前+1行向后+2行(实际只有1行)的总行数3。

  sum开窗函数
  -- sum开窗函数

  select studentId,math,departmentId,classId,
  -- 以符合条件的所有行作为窗口
  sum(math) over() as sum1,
  -- 以按classId分组的所有行作为窗口
  sum(math) over(partition by classId) as sum2,
   -- 以按classId分组、按math排序后、按到当前行(含当前行)的所有行作为窗口
  sum(math) over(partition by classId order by math) as sum3,
   -- 以按classId分组、按math排序后、按当前行+往前1行+往后2行的行作为窗口
  sum(math) over(partition by classId order by math rows between 1 preceding and 2 following) as sum4
  from student_scores where departmentId='department1';

  结果
  studentid   math    departmentid    classid sum1    sum2    sum3    sum4
  111         69      department1     class1  718     410     69      223
  113         74      department1     class1  718     410     143     316
  112         80      department1     class1  718     410     223     341
  115         93      department1     class1  718     410     316     267
  114         94      department1     class1  718     410     410     187
  124         70      department1     class2  718     308     70      222
  121         74      department1     class2  718     308     144     308
  123         78      department1     class2  718     308     222     238
  122         86      department1     class2  718     308     308     164

  结果解释:
      同count开窗函数