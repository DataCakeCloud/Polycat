##Hive元数据迁移
  1. 使用MySQL作为主库，LakeMetaStore作为从库，实时将MySQL的数据复制到LakeMetaStore作为从库。
  2. 将Hive Metastore节点的数量减少到一个，以防止多个Metastore节点同时写入MySQL和LakeMetastore。
  3. 在应用程序的非高峰时段，从主数据库切换到辅助数据库。使用LakeMetaStore作为主数据库，并重新启动了Metastore

  1. Hive表元数据做静态迁移，迁移过程中不允许新增数据；
  2. 后台任务迁移，能够实时监控迁移的过程
  3. 需要确定元数据迁移的范围
  4. 需要启动后台job
  
  
  Limitations
There are some limitations to what the migration scripts can do currently:

Only databases, tables and partitions can be migrated. Other entities such as column statistics, privileges, roles, functions, and transactions cannot be migrated.

The script only supports a Hive metastore stored in a MySQL-compatible JDBC source. Other Hive metastore database engines such as PostgreSQL are not supported.

There is no isolation guarantee, which means that if Hive is doing concurrent modifications to the metastore while the migration job is running, inconsistent data can be introduced in AWS Glue Data Catalog.

There is no streaming support. Hive metastore migration is done as a batch job.

If there is a naming conflict with existing objects in the target Data Catalog, then the existing data is overwritten by the new data.


Prerequisites
Your account must have access to AWS Glue.

Your Hive metastore must reside in a MySQL database accessible to AWS Glue. Currently, AWS Glue is able to connect to the JDBC data sources in a VPC subnet, such as RDS, EMR local Hive metastore, or a self-managed database on EC2. 
If your Hive metastore is not directly accessible to AWS Glue, then you must use Amazon S3 as intermediate staging area for migration.