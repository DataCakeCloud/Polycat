# 阶段一演示

演示如下特性：
1. 查看Catalog和Table时间线、版本历史
2. 数据恢复到指定版本
3. 恢复误删表格
4. 数据分支和隔离

## 查看Catalog和Table时间线，查看多版本
1. 创建catalog, database, table，以及准备样例数据
```sql
create catalog main;
use catalog main;
show catalogs;
desc catalog main;
create database tpch location './target/tpch';
use database tpch;
show databases;
desc database tpch;
create table region (r_name string, r_regionkey int) stored as carbon;
show tables;
desc table region;
desc table extended region;
insert into region values ('Africa', 0), ('America', 1), ('Asia', 2), ('Europe', 3), ('Middle East', 4);
select * from region;
insert into region values ('Africa', 0), ('America', 1), ('Asia', 2), ('Europe', 3), ('Middle East', 4);
insert into region values ('Africa', 0), ('America', 1), ('Asia', 2), ('Europe', 3), ('Middle East', 4);
insert into region values ('Africa', 0), ('America', 1), ('Asia', 2), ('Europe', 3), ('Middle East', 4);
select * from region;
```

2. 查看版本历史
```sql
show history for catalog main;
show history for table region;
```



## 数据恢复

3. 恢复表格到指定的版本
```sql
restore table region version as of 'version_number';
show history for table region;
select * from region;

restore table region version as of 'version_number';
select * from region;
```

4. 恢复误删表格
```sql 
drop table region;
show tables;
show all tables;
undrop table region;

show tables;
show all tables;
show history for table region;
```



## 数据多分支

5. 创建分支
```
create table nation (n_name string, n_nationkey int, n_regionkey int) stored as carbon;
insert into nation values('China', 0, 2 ), ('Russia', 1,3), ('United States', 2, 1), ('United Kingdom', 3,3), ('France', 4,3), ('Germany', 5, 3), ('India', 6, 2), ('Japan', 7, 2);

select * from nation;
show tables;

create branch dev on main;
show branches;
```

6. 使用分支实现数据隔离
```sql
use branch dev;
use database tpch;

select * from nation;
insert into nation values('China', 0, 2 ), ('Russia', 1,3), ('United States', 2, 1), ('United Kingdom', 3,3), ('France', 4,3), ('Germany', 5, 3), ('India', 6, 2), ('Japan', 7, 2);

insert into nation values('China', 0, 2 ), ('Russia', 1,3), ('United States', 2, 1), ('United Kingdom', 3,3), ('France', 4,3), ('Germany', 5, 3), ('India', 6, 2), ('Japan', 7, 2);

select * from nation;
select * from main.tpch.nation;

show history for table nation;
show history for table main.tpch.nation;

show history for catalog dev;
show history for catalog main;
```
