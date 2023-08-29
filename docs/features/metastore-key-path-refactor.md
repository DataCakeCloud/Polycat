1. # metastore key path refactor

   ## 目前元数据组织

   meta store的元数据划分为多个FDBRecordStore(简称store)，每个store里面存储所有租户和数据对象的元数据。

   可以认为目前的元数据组织是平铺型的，平铺了多个subpace，哪些subpace和租户数据对象的对应关系并不清晰。

   例如：TableSchema 表按照project Id + catalog Id + database Id + table Id 作为 primary key，隔离不同租户和数据对象的数据。

   编码到FDB中key的编码方式是：Table Schema\project Id\catalog Id\database Id\table Id （subspace: Table Schema）

   table这个数据对象划分了：TableReference，TableSchema，TablePartition，TableProperties，TableStorage。每个表又分别对应一张记录历史状态的历史状态表，TableHistory，TableSchemaHistory，TablePartitionHistory，TablePropertiesHistory，TableStorageHistory。

   我们系统中已经有的subspace表的详细介绍，可以参考：catalog-server-metadata-table-description.md。

   ##### 数据组织结构

      ```shell
     / (subspace)
        +- TableSchema (STRING=TableSchema)
        +- TableSchemaHistory (STRING=TableSchemaHistory)
        +- TableStorage (STRING=TableStorage)
        +- TableStorageHistory (STRING=TableStorageHistory)
        +- TableProperties (STRING=TableProperties)
        +- TablePropertiesHistory (STRING=TablePropertiesHistory)
        +- TablePartition (STRING=TablePartition)
        +- TablePartitionHistory (STRING=TablePartitionHistory)
        +- TableHistory (STRING=TableHistory)
        +- TableCommit (STRING=TableCommit)
        +- TableReference (STRING=TableReference)
   	 +- TableObjectName (STRING=TableObjectName)
   	 +- DroppedTableObjectName (STRING=DroppedTableObjectName)
   	 +- DatabaseRecord (STRING=DatabaseRecord)
        +- DatabaseHistory (STRING=DatabaseHistory)
        +- DatabaseObjectName (STRING=DatabaseObjectName)
        +- DroppedDatabaseObjectName (STRING=DroppedDatabaseObjectName)
        +- CatalogCommit (STRING=CatalogCommit)
        +- SubBranchRecord (STRING=SubBranchRecord)
        +- CatalogRecord (STRING=CatalogRecord)
        +- CatalogHistory (STRING=CatalogHistory)
        +- CatalogObjectName (STRING=CatalogObjectName)
        +- DroppedCatalogObjectName (STRING=DroppedCatalogObjectName)
      ```

   ### 平铺型数据的组织方式存在几个不足：

   1. **平铺型的元数据组织，不同store的表混杂在一起，难以理解表之间的关系。store中的数据也混杂在一起，不利于数据的管理**。

   2. **租户的数据和数据对象的数据分散在不同的subspace中，按照租户或者数据对象删除数据时，需要在不同的subspace中查询数据。**增加了维护成本，

      例如，增加一个subspace，需要给数据对象的删除流程添加对应的删除操作。相应的开发人员需要详细了解subspace的划分。

   3. **不支持范围快速删除某个数据对象或者租户的数据，drop 流程复杂，测试用例不能快速清理测试产生的元数据。**目前不支持范围删除，删除某个对象的subpace数据需要遍历出来，逐个删除。FDB的范围的时间复制度是log(n)，不能使用范围删除的情况下，复杂度变成了2n log(n)。增加了FDB的读写开销，影响性能。

   4. proto不支持UUID 类型，我们对象的ID，采用UUID作为key编码key时，只能转成String类型增加了key的长度。增加了存储开销。

   5. **proto的定义和对象实例强耦合，流程不能复用。**例如，我们的table属性proto定义包含了table的对象信息，catalog id，database id，导致父子分支之间不能直接引用，需要转换一次catalog id。table 信息里面包含了database id导致，无法用较少的代价做到，将table迁移到其他database。

   6. partition数据过于分散，不利于优化partition数据的遍历。

   ## 重构后元数据的组织

   重构后，我们希望按照租户和数据对象组织数据。

   例如同样是TableSchema 表，重构后的key组织方式是：\project Id\catalog Id\database Id\table Id\ Table Schema

   删除租户或者数据对象的所有数据，只要指定project id或者 \project Id\catalog Id\database Id\table Id 就可以删除这个数据对象下面的所有元数据。

   ##### 数据组织结构

   ```shell
   / (keySpace)
       +- userPrivilege (STRING=userPrivilege)
       +- objectNameMap (STRING=objectNameMap)
       +- accelerator (STRING=accelerator)
          +- AcceleratorObjectName (STRING=AcceleratorObjectName)
          +- AcceleratorTemplate (STRING=AcceleratorTemplate)
          +- AcceleratorRecord (STRING=AcceleratorRecord)
       +- backendTaskObject (STRING=backendTaskObject)
       +- tableUsageProfile (STRING=tableUsageProfile)
       +- projectId (STRING)
          +- Role (STRING=Role)
             +- RoleObjectName (STRING=RoleObjectName)
             +- DroppedRoleObjectName (STRING=DroppedRoleObjectName)
             +- RoleUser (STRING=RoleUser)
             +- RoleID (UUID)
                +- RoleReference (STRING=RoleReference)
                +- RolePrivilege (STRING=RolePrivilege)
          +- Share (STRING=Share)
             +- ShareObjectName (STRING=ShareObjectName)
             +- DroppedShareObjectName (STRING=DroppedShareObjectName)
             +- ShareID (UUID)
                +- ShareReference (STRING=ShareReference)
                +- ShareAccount (STRING=ShareAccount)
                +- SharePrivilege (STRING=SharePrivilege)
          +- Delegate (STRING=Delegate)
          +- DataLineage(STRING=DataLineage)
             +- TableDataLineageRecord(STRING=TableDataLineageRecord)
          +- Catalog (STRING=Catalog)
             +- CatalogObjectName (STRING=CatalogObjectName)
             +- DroppedCatalogObjectName (STRING=DroppedCatalogObjectName)
             +- catalogId (UUID)
                +- CatalogCommit (STRING=CatalogCommit)
                +- SubBranchRecord (STRING=SubBranchRecord)
                +- CatalogRecord (STRING=CatalogRecord)
                +- CatalogHistory (STRING=CatalogHistory)
                +- Database (STRING=Database)
                   +- DatabaseObjectName (STRING=DatabaseObjectName)
                   +- DroppedDatabaseObjectName (STRING=DroppedDatabaseObjectName)
                   +- databaseId (UUID)
                      +- DatabaseRecord (STRING=DatabaseRecord)
                      +- DatabaseHistory (STRING=DatabaseHistory)
                      +- View (STRING=View)
                         +- ViewObjectName (STRING=ViewObjectName)
                         +- ViewId (UUID)
                            +- ViewReference (STRING=ViewReference)
                      +- Table (STRING=Table)
                         +- TableObjectName (STRING=TableObjectName)
                         +- DroppedTableObjectName (STRING=DroppedTableObjectName)
                         +- tableId (UUID)
                            +- TableSchema (STRING=TableSchema)
                            +- TableSchemaHistory (STRING=TableSchemaHistory)
                            +- TableStorage (STRING=TableStorage)
                            +- TableStorageHistory (STRING=TableStorageHistory)
                            +- TableProperties (STRING=TableProperties)
                            +- TablePropertiesHistory (STRING=TablePropertiesHistory)
                            +- TablePartition (STRING=TablePartition)
                            +- TablePartitionHistory (STRING=TablePartitionHistory)
                            +- TableHistory (STRING=TableHistory)
                            +- TableCommit (STRING=TableCommit)
                            +- TableReference (STRING=TableReference)
   ```

   

   project Id对象之外的subspace不属于任何一个数据对象，可以认为是全局的subspace。例如：userPrivilege， accelerator，backendTaskObject等。

   这些subspace，没有划分到某个数据对象，原因是本身就是全局的，或者是因为有些查询逻辑需要跨多个数据对象（可以检视一下这些查询逻辑是否合理，再做修改）。

   ##### Proto文件变化
   
   ````protobuf
   // 修改前
   message TableObjectName {
     required string project_id = 1;
     required string catalog_id = 2;
     required string database_id = 3;
     required string name = 4;
     required string object_id = 5;
   }
   
   // 修改后
   message TableObjectName {
     required string name = 1;
     required string object_id = 2;
   }
   ````

   代码实现例子：
   
   ```java
    +- state (STRING)
        +- office_id (LONG)
           +- employees (STRING=E)
           |  +- employee_id (LONG)
           +- inventory (STRING=I)
           |  +- stock_id (LONG)
           +- sales (STRING=S)
              +- transaction_id (UUID)
              +- layaways (NULL)
   
   KeySpace keySpace = new KeySpace(
          new KeySpaceDirectory("state", KeyType.STRING)
              .addSubdirectory(new KeySpaceDirectory("office_id", KeyType.LONG)
                  .addSubdirectory(new KeySpaceDirectory("employees", KeyType.STRING, "E")
                      .addSubdirectory(new KeySpaceDirectory("employee_id", KeyType.LONG)))
                  .addSubdirectory(new KeySpaceDirectory("inventory", KeyType.STRING, "I")
                      .addSubdirectory(new KeySpaceDirectory("stock_id", KeyType.LONG)))
                  .addSubdirectory(new KeySpaceDirectory("sales", KeyType.STRING, "S")
                      .addSubdirectory(new KeySpaceDirectory("transaction_id", KeyType.UUID))
                      .addSubdirectory(new KeySpaceDirectory( "layaways", KeyType.NULL)))));
   
   Tuple transactionKey = keySpace.path("state", "CA")
         .add("office_id", 1234)
         .add("sales")
         .add("transaction_id", UUID.randomUUID())
         .toTuple(context);
   ```

   ### 按照数据对象组织的方式几个优势：

   1. **通过KeySpace的方式组织keypath，可以清晰的呈现整个metastore的subpace的组织层级。在代码层面数据的组织结构有良好的表达方式，减少对文档的依赖**。（新增文件：DirectoryStoreHelper.java 表示subspace的组织关系）

   2. **快速删除某个租户和数据对象的数据，通过 deleteStore或者kv的范围删除可以，快速删除某个租户或者数据对象内的所有数据，并且整个删除动作具有事务性**。（当前实现不能支持范围删除，需要逐个遍历读取，再逐个删除，增加了FDB的负担影响性能，例如我们现在数据量最大的历史表数据，主key都是uuid，无法实现范围删除）

   3. **实现快速遍历某个租户或者数据对象内的所有数据，因为key的组织方式都是采用了对象ID作为前缀，可以通过范围查询快速遍历所有数据。**

      这里体现的就是按照租户隔离数据，这其实是2种隔离方式，平铺型的隔离方式其实是共享schema，共享table，通过给每个记录增加租户信息来做隔离。重构后的隔离，相当于是独立table的方式。这2种方式从数据查询角度，并没有太大的区别，都可以快速定位到需要查找的数据。但是以下几个方面独立table的方式更具有优势：
   
      （1）数据安全性，共享table的方式只是通过限制查询条件，限制了查询范围，也就是说在出现漏洞时，通过改变查询条件就容易获取到其他租户的数据。
   
      （2）便于按照租户粒度做统计，独立table的方式，可以很容易的给出这个租户使用资源的统计信息。
   
      （3）权限模型和元数据存储的模型可以有对应关系，也就是对某个租户授权，可以对应到元数据存储的数据对象上。

      （4）便于缓存或者其他资源分配参数按照租户调整，租户的资源可以有独立的数据对象模型和其对应时，可以更方便的按照这个租户的SLA，定义优化参数。
   
   4. **key path支持UUID类型，ID类型的key的编码不用采用String类型，采用UUID类型，可以很大程度的减少key的存储长度。**
   
   5. key path的常量类型目录名称，可以指定编码的名称，在代码层面可以通过路径名称（全名），提供较好的阅读性。又可以通过编码名称，提供较小的空间映射。例如可以把TableSchema 映射为 long类型或者 String类型的简写。
      例如 “Ts”，减少存储空间。
   
      ```java
      KeySpaceDirectory("TableSchema", KeyType.STRING, "Ts")
      KeySpaceDirectory("TableSchema", KeyType.LONG, 1)
      ```
   
      说明：KeySpaceDirectory 名称上包含目录这个名称，但是和FDB的DirectoryLayer是不同的，DirectoryLayer提供了更灵活的映射方式，可以实现目录改名等特性，但是也会增加一层元数据映射层，会增加读取查询次数。
      
   6. 添加数据对象内的元数据表，可以不用感知删除数据对象的流程，减少维护成本。
   
   7. **每个store只关联，自己存储的proto定义，可以实现强类型的检查，避免存储错误。**（重构前的方案，是把所有proto 都设置到一个RecordMetaDataBuilder中，这样的问题是，我们在table
      schema的subspace中存储了一个TableProperties的数据，是不能检查出这种错误）

   8. **数据组织结构和数据proto解耦合：数据对象的Proto组织不依赖数据对象的位置，方便之后抽象出UVO对象可以插入到任意数据组织层级下。**
   
      数据对象信息从proto定义中移除之后，目前看到还有一个好处，分支的数据查询时，引用父分支的数据，查询到父分支的数据后，由于数据中保存了父分支
   
      的catalog 信息，查询到的所有父分支的信息，都要修改catalog信息，重新生成属于子分支的信息。解耦之后，这种操作可以去除，减少了内存和cpu开销。
   
   9. **减少索引数量，平铺型的管理，通常需要按照数据对象维度创建索引，如果还需要对属性项进行索引，必须创建2级索引。按照数据对象维度组织数据后，可以减少一层索引的定义。**
   
   10. partition数据集中存放，可以通过手段加速partition遍历。
   
   ### 按照数据对象组织方式的几个不足：
   
   1. **跨数据对象的二级索引不能使用**。例如：原来方案中，catalog的属性中记录了parent catalog id，我们可以通过按照parent catalog创建二级索引的方式检索catalog 下存在哪些sub-branch。这种方式跨越了多个数据对象，在按照数据对象的组织方式中，不能采用这种方式，只能通过在catalog中创建另外一张表的方式记录属于这个父分支的子分支信息。
      
   2. **FDBRecordStore 数量增加**
      原来的方式，系统中只存在固定数量的FDBRecordStore，当前按照数据对象的组织方式，FDBRecordStore数量会按照数据对象的数量成倍增加。例如原来table相关的元数据表有10个subpace，按照数据对象方式组织数据后，每个表都会创建10个subspace。
   
      通过查阅FDB的资料，build Store的开销很小，build Store仅仅会记录一些元数据的版本信息，用于检查proto结构的变化。
   
   3. **跨数据对象的引用需要减少**，特别是分支的parition表，子分支存在引用父分支索引的情况，未来在我们的compaction 方案中，也要修改这种情况。不同分支之间的数据需要隔离。
   
      （**这个点也是让我再整理一下跨多个数据对象的范围接口是否合理**，例如我们一个subspace中有多个数据对象的数据，我们设置了时间维度，有些接口是按照时间维度去检视所有的数据对象，这种接口真的有需要吗，权限怎么设置呢）
      
      
   
   ## 修改总结
   
   ```java
    RecordMetaData RECORD_METADATA = buildRecordMetaData();
    
    static FDBRecordStore getRecordStore(FDBRecordContext context, Subspace subspace) {
           return FDBRecordStore.newBuilder().setMetaDataProvider(RECORD_METADATA).setContext(context)
               .setSubspace(subspace).createOrOpen();
       }
   
     static RecordMetaData buildRecordMetaData() {
           RecordMetaDataBuilder metaDataBuilder =
               RecordMetaData.newBuilder().setRecords(CommitProto.getDescriptor());
           for (String[] table : StoreTables.getTablesWithPrimaryKey()) {
               String[] pk = new String[table.length - 1];
               System.arraycopy(table, 1, pk, 0, pk.length);
               setRecordType(metaDataBuilder, table[0], pk);
           }
           return metaDataBuilder.build();
       }
   ```
   
   我们构建RecordStore有2个组成部分：subspace，表示key的前缀， RECORD_METADATA，表示store中可以存储的proto对象，以及proto中primary key的定义，2级索引的定义。
   
   **重构修改方案：**
   
   1. **把proto中关于数据对象的信息（projectId，catalogId，databaseId， tableId）提取出来，放到subspace中。**
   2. **把proto中定义的字段中，把数据对象相关的信息删除。**
   3. **每个store只关联和他本身相关的proto信息（不要把所有proto放到一个metadata里面），好处是可以做类型检查，方便添加新的proto和独立升级。**
   
   之前的build store的方式是：
   
   ```java
      public static FDBRecordStore getTablePropertiesStore(FDBRecordContext context) {
           return getRecordStore(context, SubspaceHelper.TABLE_PROPERTIES);
       }
   ```
   
   修改后需要传入数据对象信息，构造subspace：
   
   ```java
   public static FDBRecordStore getTablePropertiesStore(FDBRecordContext context, TableIdent tableIdent) {
           KeySpacePath keySpacePath = getTableKeyPath(tableIdent);
           return getStore(context, keySpacePath.add(DirectoryStringMap.TABLE_PROPERTIES.getName()),
               StoreMetadata.TABLE_PROPERTIES.getRecordMetaData());
    }
   
    private static KeySpacePath getTableKeyPath(TableIdent tableIdent) {
           return root.path(DirectoryStringMap.PROJECT_ID.getName(), tableIdent.getProjectId())
               .add(DirectoryStringMap.CATALOG_OBJECT.getName())
               .add(DirectoryStringMap.CATALOG_ID.getName(), UUID.fromString(tableIdent.getCatalogId()))
               .add(DirectoryStringMap.DATABASE_OBJECT.getName())
               .add(DirectoryStringMap.DATABASE_ID.getName(), UUID.fromString(tableIdent.getDatabaseId()))
               .add(DirectoryStringMap.TABLE_OBJECT.getName())
               .add(DirectoryStringMap.TABLE_ID.getName(), UUID.fromString(tableIdent.getTableId()));
   } 
   ```

   **这次重构的主要修改点：**
   
   1. subpace按照数据对象组织，按照数据对象执行范围删除（fdb开销小，事务性操作），按照租户和数据对象隔离数据
   2. subspace只对应和这个subspace相关的proto结构，可以做类型检查，防止出错
   3. subspace在fdb里面持久化的对象结构和流程中参数使用的结构分离，便于看护盘上的持久化结构，参数的变更，不影响数据升级。分离还有一个好处就是每个对象的职责单一，便于理解。目前我们有些对象设置了很多optional字段，为了查询时带出更多的信息，如果传参对象和持久化对象都共用这个结构体，这个结构体功能不单一。
   
   ## UVO 对象的设计
   
   #### 简介
   
   ​	在数据湖对于结构化数据和半结构化数据，都能通过表的方式进行管理。但是对于非结构化数据，例如：文本信息、在AI场景中的数据集，由于缺少数据结构，难以通过表的形式管理这些数据。对于这些**非结构化数据也需要通过数据湖将数据管理起来的需求**，PolyCat提供了**Universal Versioned Object(UVO)** 数据结构专门**解决非结构化数据的管理问题**。
   
   ​	UVO是一种不关心数据结构而对数据进行管理的对象，方便非结构化数据在数据湖中进行管理，不会受到数据格式的限制。数据管理中，有些功能并不需要知道数据结构的细节，如：版本控制、数据血缘、数据画像。我们可以复用这些功能，将非结构化数据管理起来。
   
   | 类型           | 具体操作                                                 |
   | -------------- | -------------------------------------------------------- |
   | 对象操作       | 创建对象、删除对象、修改对象属性、列举对象、读取对象属性 |
   | 数据操作       | 追加写、覆盖写、合并写、修改、删除、列举对象内数据实体   |
   | 版本操作       | 查看版本历史、版本回退、逆删除、按版本查询               |
   | 分支操作       | 创建分支、删除分支、合并分支                             |
   | 授权和鉴权操作 | 对UVO对象在用户间、租户间授权                            |
   | 操作日志和统计 | 记录对UVO对象的管理操作和访问操作、形成画像              |
   
   #### 数据结构
   
   通过上面的这次重构，我们将proto中关于对象的信息都抽取出去，存储的proto结构中只有对象本身的信息，不再包含具体的数据对象位置和关系的记录。这样也更加容易实现UVO的抽象。
   
   我们只需要定义几个UVO对象公共的subpace的proto，proto并不会与具体的数据类型关联，方便不同的数据类型处理公共的逻辑。
   
   UVO对象可以做到，新增对象类型值，就可以存储这个对象的属性信息，支持分支，多版本，画像，统计等公共的功能。所有的元数据的存储逻辑都可以复用。目的是可以支撑未来更多种类的数据对象的元数据信息的存储。（例如：View，Model，Text，Graph，Feature等对象）
   
   我们先看看，UVO对象的公共subpace的组织关系和定义：
   
   ```shell
   +- Database (STRING=Database)
      +- DatabaseObjectName (STRING=DatabaseObjectName)
      +- DroppedDatabaseObjectName (STRING=DroppedDatabaseObjectName)
         +- databaseId (UUID)
            +- DatabaseRecord (STRING=DatabaseRecord)
            +- DatabaseHistory (STRING=DatabaseHistory)
            +- Table (STRING=Table)
               +- xxx 
               ...
            +- UVO (STRING=UVO)
               +- UniversalObjectName (STRING=UniveralObjectName)
               +- DroppedUniversalObjectName (STRING=DroppedUnversalObjectName)
               +- UniversalObjectUsageProfile (STRING=UniversalObjectUsageProfile)
               +- UniversalObjectId (UUID)      
                  +- Storage (STRING=torage)
                  +- StorageHistory (STRING=StorageHistory)
                  +- Properties (STRING=Properties)
                  +- PropertiesHistory (STRING=PropertiesHistory)  
                  +- ObjectBase (STRING=ObjectBase)
                  +- ObjectBaseHistory (STRING=ObjectBaseHistory)
                  +- Commit (STRING=Commit)
                  +- Reference (STRING=Reference)
                  +- DataLineage(STRING=DataLineage)
                  +- UDS (STRING=User defined subspace)
   ```
   
   **subspace 说明：**
   
   1. UVO属于database数据对象之下和Table属于同一级的数据对象
   
   2. 基础数据：
   
      1. UniversalObjectName：**名称和ID的对应关系**，名称Name由用户设置，ID由catalog server分配，subspace中需要增加type说明UniversalObject的类型。（type + name ---> Id）
   
         ```protobuf
         message UniversalObjectName {
           required string type = 1;
           required string name = 2;
           required string object_id = 3;
         }
         ```
   
      2. DroppedUniversalObjectName：**记录删除的名称和ID对应关系**
   
         ```protobuf
         message DroppedUniversalObjectName {
           required string type = 1;
           required string name = 2;
           required string object_id = 3;
           required int64 create_time = 4;
           required int64 dropped_time = 5;
           required ObjectDropType drop_type = 6;
         }
         ```
   
      3. UniversalObjectUsageProfile：**统计对象操作情况**，具体可以抽象为读写操作的统计，便于输出UniversalObject的使用频率的统计。
   
         ```protobuf
         message UsageProfile{
           required string object_id = 1;
           required int64  start_time = 2;
           required string op_type =3;
           required int64  count = 4;
          }
         ```
   
      4. UniversalObjectId 是某个UniversalObject的UUID值，这个层级之下记录关于这个UniversalObject的所有信息
   
      5. ObjectBase ：基础信息，包含名称、对象的 存储格式，存储位置，存储相关的权限信息、用户自定义的属性信息等。ObjectBaseHistory 是ObjectBase的历史修改记录，我们将对象的用户信息、存储属性、自定义属性汇集在一起。(**不频繁修改)**
   
         ```protobuf
         message ObjectBase {
            required int32 primary_key = 1;
            required string name = 2;
            required string user_id = 5;
            required string location = 2;
            optional string storage_location_type = 3;
            optional string file_format = 4; // 注释类型
            map<string, string> properties = 2;
         }
         ```
   
      8. Commit ： 对象的操作记录是UniversalObject 的所有修改事务的统计信息。用于支撑time travel，restore，undrop等操作。
   
         ```protobuf
         message Commit {
           required string event_id = 1;
           required string name = 2;
           required int64 create_time = 3;
           required int64 commit_time = 4;
           repeated Operation operation = 5;
           optional int64 dropped_time = 6;
           optional bytes version = 7;
         }
         ```
   
      9. Reference :**互斥操作使用**，并发操作通过写Reference记录使需要串行执行的操作，产生事务冲突，通过事务重试转换为串行处理。（操作接口可以增加隔离级别的参数，用于控制是否需要串行处理）。
   
         ```protobuf
         message Reference {
           required int32 primary_key = 1;
           required string name = 2;
           // String of system clock, updated when table meta or data is inserted/update,
           // used for transaction conflict detection in underlying store, Ex. FoundationDB
           optional string latest_meta = 3; 
           optional string latest_data = 4;
         }
         ```
   
      10. DataLineage :**血缘关系**。
   
          ```protobuf
          message DataLineage {
            required DataSourceType data_source_type = 5;
            required string data_source_content = 6;
          }
          ```
   
      9. DataIndex ： **存数据的索引信息**
   
         ```protobuf
         message DataIndex {
           required string event_id = 1;
           repeated partitionSet partitions = 2;
         }
         ```
   
         
   
   3. UDS （User defined subspace）是灵活的用户自定义的subpace存储，可以提供用户自定义的数据管理方法。
   
      生命周期方面，UDS和我们定义好的如storage信息一样，**支持多版本、分支特性**，且和固定分支绑定。在数据使用方面，UDS可以复用底下数据库的索引功能，**可以通过用户自定义的索引进行查询加速**。
   
      1. 提供管理Proto格式的REST接口，如： INSERT PROTO *protoName*( *protodescription* ) INTO *objectId*; DELETE PROTO *protoName* IN *ObjectId*
   
      2. 提供用户管理UDS数据的REST接口： 如  INSERT UDS INTO *protoName* *datas*;  
   
         ​																	  DELETE UDS IN *protoName* *primaryKey*;  
   
         ​																	  GET  UDS IN *protoName* *primaryKey*
   
      UDS同样可以作为基础能力提供给LMS使用，完成一些定制化得优化。
   
      难点：用于应对这个目前实现的难点是proto需要预编译，我们会存储用户的设置的，subspace名称，UDSProto.getDescriptor()，primaryIndex，secondaryIndex，versionIndex索引值。写入数据时，传入subspace 名称和对应的protobuf，就可以完成数据的存储，读取时也是传入subpace名称和primarykey，就可以完成数据的读取。
   
   4.  可以采用UDS的方式来支持数据的**PARTITION**打上特定的标记，使得用户可以按照Tag获取对应的PARTITION信息。用户的数据的属性通常不止一种，在不同的维度可以有不同的分类。可以按照用户的意图将PARTITION打上特定的标签。用户可以带着Tag获得拥有Tag标签的PARTITION信息。如：用户按照城市进行PARTITION分区，那么可以按照省份、发达程度、人口数量等维度打上多个Tag。**用户可以按照Tag统计这些Partition的信息**。
   
   ##### 遗留问题：
   
   1. 多版本，分支，权限的能力都是复用目前数据对象的能力。
   2. 需要设计的是Commit信息的表达不一定能抽象出公共的操作，可能只能支持用户comment描述信息。
   3. UVO对象如果支持多版本的能力，对于overwrite的数据和append的数据怎么做GC动作。或者GC动作是否应该交由用户来完成，LSM需要为GC操作提供什么程度的支持（假设：提供可供GC的partition列表，以及PARTITION之间的append关系）。
   4. Tag 目前支持的粒度是按照PARTITION来划分，首要目的是记录PARTITION之间的联系。但若要按照Tag统计数据画像等信息，需要结合数据画像的统计粒度，来确定打Tag的粒度。

