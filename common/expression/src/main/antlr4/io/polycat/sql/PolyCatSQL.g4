grammar PolyCatSQL;

singleStatement
    : statement ';'* EOF
    ;

standaloneExpression
    : expression EOF
    ;

standalonePathSpecification
    : pathSpecification EOF
    ;

statement
    : query
    | command
    ;

catalogStatement
    : catalogCommand ';'* EOF
    ;

catalogCommand
    : CREATE CATALOG (IF NOT EXISTS)? identifier (COMMENT string)?     #createCatalog
    | USE CATALOG identifier                                           #useCatalog
    | DROP CATALOG (IF EXISTS)? identifier (PURGE)?                    #dropCatalog
    | SHOW CATALOGS (LIKE pattern=string)?                             #showCatalogs
    | (DESC | DESCRIBE) CATALOG (EXTENDED)? identifier                 #descCatalog
    | SHOW HISTORY FOR TABLE tableName                                 #showTableHistory
    | RESTORE TABLE tableName VERSION AS OF string                     #restoreTableWithVer
    // Delegate
    | CREATE DELEGATE delegateName=identifier
        STORAGE_PROVIDER '=' provider=identifier
        PROVIDER_DOMAIN_NAME '=' domainName=identifier
        AGENCY_NAME '=' agencyName=identifier
        (ALLOWED_LOCATIONS '=' allowedLocations=locationList)?
        (BLOCKED_LOCATIONS '=' blockedLocations=locationList)?         #createDelegate
    | (DESC | DESCRIBE) DELEGATE delegateName=identifier               #descDelegate
    | SHOW DELEGATES (LIKE pattern=string)?                            #showDelegates
    | DROP DELEGATE delegateName=identifier                            #dropDelegate
    // copy into table
    | COPY INTO tableName FROM location=string (RECURSIVE)?
        (DELEGATE '=' delegateName=identifier)?
        FILE_FORMAT '=' source=identifier
        (WITH_HEADER '=' with_header=booleanValue)?
        (DELIMITER '=' delimiter=string)?                               #copyIntoTable
    //BRANCH
    | CREATE BRANCH (IF NOT EXISTS)? qualifiedName
        (ON qualifiedName (VERSION'(ID='string')')?)?                  #createBranch
    | MERGE BRANCH srcBranch (TO destBranch)?                          #mergeBranch
    | SHOW BRANCHES (ON identifier)?                                   #showBranches
    | USE BRANCH identifier                                            #useBranch
    //DATABASE
    | DESC DATABASE (EXTENDED)? databaseName                           #descDatabase
    | DROP DATABASE (IF EXISTS)? databaseName (PURGE)?                 #dropDatabase
    | SHOW (ALL)? DATABASES
        (IN catalogName)?
        (LIKE pattern=string)?
        (LIMIT '=' maximumToScan=INTEGER_VALUE)?                       #showDatabases
    | UNDROP DATABASE databaseName idProperty? (AS qualifiedName)?     #undropDatabase
    | USE DATABASE databaseName                                        #useDatabase
    | ALTER DATABASE databaseName SET DBPROPERTIES properties          #alterDbProperties
    | ALTER DATABASE databaseName SET LOCATION location=string         #alterDbLocation
    //USAGE PROFILE
    | DESC ACCESS STATS FOR TABLE tableName
        (FROM startTimestamp=string TO endTimestamp=string)?           #descAccessStatsForTable
    | SHOW opType=(READ | WRITE) ACCESS STATS FOR CATALOG identifier
        (FROM startTimestamp=string TO endTimestamp=string)?
        (LIMIT '=' maximumToScan=INTEGER_VALUE)? (DESC | ASC)?         #showAccessStatsForCatalog
    // DATA LINEAGE
    | SHOW DATA LINEAGE FOR TABLE tableName (lineageType=(UPSTREAM | DOWNSTREAM))?    #showDataLineageForTable
    //PRIVILEGE
    | GRANT ALL ON objectType qualifiedName TO principal               #grantAllObjectPrivilegeToRole
    | GRANT privilege ON objectType qualifiedName TO principal         #grantPrivilegeToRole
    | GRANT SELECT ON TABLE qualifiedName TO SHARE identifier          #grantPrivilegeToShare
    | GRANT ROLE role=identifier TO USER user=identifier               #grantRoleToUser
    | GRANT SHARE share=shareFullName TO USER user=identifier          #grantShareToUser
    | REVOKE ALL ON objectType qualifiedName FROM principal            #revokeAllObjectPrivilegeFromRole
    | REVOKE ALL FROM ROLE identifier                                  #revokeAllPrivilegeFromRole
    | REVOKE privilege ON objectType qualifiedName FROM principal      #revokePrivilegeFromRole
    | REVOKE SELECT ON TABLE qualifiedName FROM SHARE identifier       #revokePrivilegeFromShare
    | REVOKE ROLE role=identifier FROM USER user=identifier            #revokeRoleFromUser
    | REVOKE SHARE share=shareFullName FROM USER user=identifier       #revokeShareFromUser
    | SHOW GRANTS TO ROLE identifier                                   #showGrantsToRole
    // ROLE
    | CREATE ROLE (IF EXISTS)? identifier (COMMENT string)?            #createRole
    | DROP ROLE (IF EXISTS)? identifier                                #dropRole
    | DESC ROLE (EXTENDED)? identifier                                 #descRole
    | SHOW ROLES (LIKE pattern=string)?                                #showRoles
    //SHARE
    | ALTER SHARE identifier ADD ACCOUNTS EQ string                    #alterShareAddAccounts
    | ALTER SHARE identifier REMOVE ACCOUNTS EQ string                 #alterShareRemoveAccounts
    | CREATE SHARE (IF EXISTS)?  identifier (COMMENT string)?          #createShare
    | DROP SHARE (IF EXISTS)? identifier (PURGE)?                      #dropShare
    | DESC SHARE (EXTENDED)? identifier                                #descShare
    | SHOW SHARES (LIKE pattern=string)?                               #showShares
    //TABLE
    | ALTER TABLE (IF EXISTS)? tableName columnAction                  #alterColumn
    | ALTER TABLE from=tableName RENAME TO to=tableName                #renameTable
    | PURGE TABLE (IF EXISTS)? tableName idProperty?                   #purgeTable
    | ALTER TABLE tableName SET TBLPROPERTIES properties               #setTableProperties
    | ALTER TABLE tableName UNSET TBLPROPERTIES keys                   #unsetTableProperties
    | ALTER TABLE tableName ADD (IF NOT EXISTS)?
        partitionSpecLocation+                                         #addPartition
    | ALTER TABLE tableName DROP (IF EXISTS)?
        partitionSpec (',' partitionSpec)* PURGE?                      #dropPartition
    | SHOW PARTITIONS tableName
        (LIMIT '=' maximumToScan=INTEGER_VALUE)?                       #showTablePartitions
    | SHOW (ALL)? TABLES
         (IN databaseName)?
         (LIKE pattern=string)?
         (LIMIT '=' maximumToScan=INTEGER_VALUE)?                      #showTables
    | UNDROP TABLE tableName idProperty? (AS qualifiedName)?           #undropTable
    //History
    | SHOW HISTORY FOR CATALOG identifier                              #showCatalogHistory

    // Policy
    | CREATE SHARE (IF EXISTS)?  identifier ON catalogName (COMMENT string)? #createShareOnCatalog
    | GRANT privileges ON objectType qualifiedName TO principalInfo
        EFFECT EQ booleanValue
        (CONDITIONS conditions=properties)?
        (ROWFILTER rowFilter=booleanExpression)?
        (COLFILTER colFilter=columnFilter)?
        (COLMASK  colMask=dataMask)?
        (WITH_GRANT)?                                                       #grantPrivilegeToPrincipal
    | REVOKE privileges ON objectType qualifiedName FROM principalInfo
        EFFECT EQ booleanValue
        (CONDITIONS conditions=properties)?
        (ROWFILTER rowFilter=booleanExpression)?
        (COLFILTER colFilter=columnFilter)?
        (COLMASK  colMask=dataMask)?
        (WITH_GRANT)?                                                       #revokePrivilegeFromPrincipal
    | SHOW POLICIES OF principalInfo                                        #showPoliciesOfPrincipal
    | GRANT rsPrincipalInfo TO ugPrincipalInfo                              #grantRsPrincipalToUgPrincipal
    | REVOKE rsPrincipalInfo FROM ugPrincipalInfo                           #revokeRsPrincipalFromUgPrincipal
    ;

command
    // CATALOG
    : catalogCommand                                                   #catalogDefault
    // DATABASE
    | CREATE DATABASE (IF NOT EXISTS)? databaseName
        (COMMENT comment=string)?
        (LOCATION location=string)?
        (WITH DBPROPERTIES properties)?                                #createDatabase
    // TABLE
    | createTableHeader
        columnAliases?
        (TRANSACTION EQ booleanValue)?
        (PARTITIONED BY '(' sparkPartition (',' sparkPartition)* ')')?
        (COMMENT comment=string)?
        (STORED AS source=identifier)?
        (LOCATION location=string)?
        (TBLPROPERTIES properties)?
        AS queryNoWith                                                  #createTableAsSelect
    | createTableHeader
        '(' tableElement (',' tableElement)* ')'
         (COMMENT comment=string)?
         (PARTITIONED BY '(' partitionDefinition (',' partitionDefinition)* ')')?
         (TRANSACTION EQ booleanValue)?
         (STORED AS source=identifier)?
         (LOCATION location=string)?
         (TBLPROPERTIES properties)?                                   #createTable
    | CREATE TABLE (IF NOT EXISTS)? tableName
         LIKE table=tableName
         (STORED AS source=identifier)?
         (LOCATION location=string)?
         (TBLPROPERTIES properties)?                                   #createTableLike
    | DROP TABLE (IF EXISTS)? tableName (PURGE)?                       #dropTable
    | (DESC | DESCRIBE) TABLE (EXTENDED | FORMATTED)? tableName        #descTable
    | SHOW CREATE TABLE tableName                                      #showCreateTable
    | INSERT (INTO | OVERWRITE) tableName
         (PARTITION '(' columnElement (',' columnElement)* ')')?
          columnAliases? query                                         #insertInto
    | MAINTAIN TABLE tableName COMPACT                                 #maintainTableCompact
    // COLUMN
    | SHOW COLUMNS IN tableName                                        #showColumns
    // STREAM
    | CREATE STREAM streamName=qualifiedName
        INTO TABLE targetTableName=qualifiedName
        FROM (KAFKA | SOCKET)
        (STMPROPERTIES properties)?                                    #createStream
    | SHOW STREAMS (LIKE pattern=string)?                              #showStreams
    | (DESC | DESCRIBE) STREAM qualifiedName                           #descStream
    | START STREAM (IF EXISTS)? qualifiedName
        (OPTIONS properties)                                           #startStream
    | STOP STREAM (IF EXISTS)? qualifiedName                           #stopStream
    | DROP STREAM (IF EXISTS)? qualifiedName                           #dropStream
    // BRANCH
    // HISTORY
    | RESTORE CATALOG identifier TIMESTAMP AS OF string                #restoreCatalogWithTs
    | RESTORE CATALOG identifier VERSION AS OF string                  #restoreCatalogWithVer
    | SHOW HISTORY FOR DATABASE databaseName                           #showDatabaseHistory
    | RESTORE DATABASE databaseName TIMESTAMP AS OF string             #restoreDatabaseWithTs
    | RESTORE DATABASE databaseName VERSION AS OF string               #restoreDatabaseWithVer
    | RESTORE TABLE tableName TIMESTAMP AS OF string                   #restoreTableWithTs
    | UNDROP CATALOG identifier idProperty? (AS qualifiedName)?        #undropCatalog
    //SHARE
    // VIEW
    | CREATE (OR REPLACE)? VIEW qualifiedName AS query                 #createView
    | DROP VIEW (IF EXISTS)? qualifiedName                             #dropView
    // MATERIALIZED VIEW
    | CREATE MATERIALIZED VIEW materializedViewName
      (TBLPROPERTIES properties)? AS query                             #createMaterializedView
    | DROP MATERIALIZED VIEW (IF EXISTS)? materializedViewName         #dropMaterializedView
    | REFRESH MATERIALIZED VIEW materializedViewName                   #refreshMaterializedView
    | SHOW (ALL)? MATERIALIZED VIEW
         (ON tableName)?
         (LIMIT '=' maximumToScan=INTEGER_VALUE)?                      #showMaterializedView
    // ROLE
    | ALTER ROLE from=roleName RENAME TO to=roleName                   #renameRole
    | EXPLAIN IN ENGINE identifier statement                           #explain
    | START TRANSACTION (transactionMode (',' transactionMode)*)?      #startTransaction
    | COMMIT WORK?                                                     #commit
    | ROLLBACK WORK?                                                   #rollback
    | SET key=configEntry (EQ value=configEntry)?                      #setConfiguration
    | UNSET key=configEntry                                            #unsetConfiguration
    // Engine
    | ATTACH ENGINE identifier properties                              #attachEngine
    | DETACH ENGINE identifier                                         #detachEngine
    | SHOW ENGINES                                                     #showEngines
    // Accelerator
    | CREATE ACCELERATOR accName USING lib=identifier AS queryNoWith   #createAccelerator
    | SHOW ACCELERATORS                                                #showAccelerators
    | DROP ACCELERATOR accName                                         #dropAccelerator
    // USAGE PROFILE
    // DATA LINEAGE
    // TestBench
    | DATAGEN bench=identifier table=identifier scale=INTEGER_VALUE    #dataGen
    ;

configEntry
    : STRING                                                           #quotedConfig
    | QUOTED_IDENTIFIER                                                #doubleQuotedConfig
    | BACKQUOTED_IDENTIFIER                                            #backQuotedConfig
    | ~('\'' | '`' | '"' | '=' )+                                      #unquotedConfig
    ;

idProperty
    : '(ID='string')'
    ;

query
    :  with? queryNoWith
    ;

with
    : WITH RECURSIVE? namedQuery (',' namedQuery)*
    ;

columnElement
    : partitionColumn=identifier '=' valueExpression
    ;

tableElement
    : columnDefinition
    | likeClause
    ;

columnDefinition
    : identifier type (columnConstraint)? (COMMENT string)? (WITH properties)?
    ;

partitionDefinition
    : sparkPartition
    | hivePartition
    ;

hivePartition
    : identifier type (COMMENT string)?
    ;

sparkPartition
    : identifier
    ;

likeClause
    : LIKE qualifiedName (optionType=(INCLUDING | EXCLUDING) PROPERTIES)?
    ;

columnConstraint
    : (NOT NULL | DEFAULT (string | number | booleanValue))
    ;

properties
    : '(' property (',' property)* ')'
    ;

property
    : key=string EQ value=string
    ;

colums
    : identifier (',' identifier)*
    ;

columnFilter
    : colums ':' optionType=(INCLUDING | EXCLUDING)
    ;

dataMask
    : columnMask (';' columnMask)*
    ;

columnMask
    : colums  ':' optionType=(VOID | ENCRYPT)
    ;

keys
    : '(' string (',' string)* ')'
    ;

partitionSpecLocation
    : partitionSpec locationSpec?
    ;

partitionSpec
    : PARTITION '(' columnElement (',' columnElement)* ')'
    ;

locationSpec
    : LOCATION string
    ;

queryNoWith:
      queryTerm
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=(INTEGER_VALUE | ALL))?
    ;

queryTerm
    : queryPrimary                                                             #queryTermDefault
    | left=queryTerm operator=INTERSECT setQuantifier? right=queryTerm         #setOperation
    | left=queryTerm operator=(UNION | EXCEPT) setQuantifier? right=queryTerm  #setOperation
    ;

queryPrimary
    : querySpecification                   #queryPrimaryDefault
    | TABLE qualifiedName                  #table
    | VALUES expression (',' expression)*  #inlineTable
    | '(' queryNoWith  ')'                 #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
    ;

groupBy
    : setQuantifier? groupingElement (',' groupingElement)*
    ;

groupingElement
    : groupingSet                                            #singleGroupingSet
    | ROLLUP '(' (expression (',' expression)*)? ')'         #rollup
    | CUBE '(' (expression (',' expression)*)? ')'           #cube
    | GROUPING SETS '(' groupingSet (',' groupingSet)* ')'   #multipleGroupingSets
    ;

groupingSet
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

namedQuery
    : name=identifier (columnAliases)? AS '(' query ')'
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?  #selectSingle
    | qualifiedName '.' ASTERISK    #selectAll
    | ASTERISK                      #selectAll
    ;

relation
    : left=relation
      ( CROSS JOIN right=sampledRelation | joinType JOIN rightRelation=relation joinCriteria)  #joinRelation
    | sampledRelation                                                                          #relationDefault
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    ;

sampledRelation
    : aliasedRelation (
        TABLESAMPLE sampleType '(' percentage=expression ')'
      )?
    ;

sampleType
    : BERNOULLI
    | SYSTEM
    ;
jobType
    : DDL
    | DML
    | RETRIEVAL
    | OTHERS
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

locationList
    : '(' string? (',' string)* ')'
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

relationPrimary
    : shareTableName                                                  #sourceShareTableName
    | tableName                                                       #sourceTableName
    | '(' query ')'                                                   #subqueryRelation
    | UNNEST '(' expression (',' expression)* ')' (WITH ORDINALITY)?  #unnest
    | LATERAL '(' query ')'                                           #lateral
    | '(' relation ')'                                                #parenthesizedRelation
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : valueExpression predicate[$valueExpression.ctx]?             #predicated
    | NOT booleanExpression                                        #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;

// workaround for https://github.com/antlr/antlr4/issues/780
predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                            #comparison
    | comparisonOperator comparisonQuantifier '(' query ')'               #quantifiedComparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN '(' expression (',' expression)* ')'                        #inList
    | NOT? IN '(' query ')'                                               #inSubquery
    | NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)?  #like
    | IS NOT? NULL                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                         #distinctFrom
    ;

valueExpression
    : primaryExpression                                                                 #valueExpressionDefault
    | valueExpression AT timeZoneSpecifier                                              #atTimeZone
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression                                 #concatenation
    ;

primaryExpression
    : NULL                                                                                #nullLiteral
    | interval                                                                            #intervalLiteral
    | identifier string                                                                   #typeConstructor
    | number                                                                              #numericLiteral
    | booleanValue                                                                        #booleanLiteral
    | string                                                                              #stringLiteral
    | BINARY_LITERAL                                                                      #binaryLiteral
    | '?'                                                                                 #parameter
    | POSITION '(' valueExpression IN valueExpression ')'                                 #position
    | '(' expression (',' expression)+ ')'                                                #rowConstructor
    | ROW '(' expression (',' expression)* ')'                                            #rowConstructor
    | qualifiedName '(' ASTERISK ')' filter? over?                                        #functionCall
    | qualifiedName '(' (setQuantifier? expression (',' expression)*)?
        (ORDER BY sortItem (',' sortItem)*)? ')' filter? over?                            #functionCall
    | identifier '->' expression                                                          #lambda
    | '(' (identifier (',' identifier)*)? ')' '->' expression                             #lambda
    | '(' query ')'                                                                       #subqueryExpression
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS '(' query ')'                                                                #exists
    | CASE valueExpression whenClause+ (ELSE elseExpression=expression)? END              #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #searchedCase
    | CAST '(' expression AS type ')'                                                     #cast
    | TRY_CAST '(' expression AS type ')'                                                 #cast
    | ARRAY '[' (expression (',' expression)*)? ']'                                       #arrayConstructor
    | value=primaryExpression '[' index=valueExpression ']'                               #subscript
    | identifier                                                                          #columnReference
    | base=primaryExpression '.' fieldName=identifier                                     #dereference
    | name=CURRENT_DATE                                                                   #specialDateTimeFunction
    | name=CURRENT_TIME ('(' precision=INTEGER_VALUE ')')?                                #specialDateTimeFunction
    | name=CURRENT_TIMESTAMP ('(' precision=INTEGER_VALUE ')')?                           #specialDateTimeFunction
    | name=LOCALTIME ('(' precision=INTEGER_VALUE ')')?                                   #specialDateTimeFunction
    | name=LOCALTIMESTAMP ('(' precision=INTEGER_VALUE ')')?                              #specialDateTimeFunction
    | name=CURRENT_USER                                                                   #currentUser
    | name=CURRENT_PATH                                                                   #currentPath
    | SUBSTRING '(' valueExpression FROM valueExpression (FOR valueExpression)? ')'       #substring
    | NORMALIZE '(' valueExpression (',' normalForm)? ')'                                 #normalize
    | EXTRACT '(' identifier FROM valueExpression ')'                                     #extract
    | '(' expression ')'                                                                  #parenthesizedExpression
    | GROUPING '(' (qualifiedName (',' qualifiedName)*)? ')'                              #groupingOperation
    ;

string
    : STRING                                #basicStringLiteral
    | UNICODE_STRING (UESCAPE STRING)?      #unicodeStringLiteral
    ;

timeZoneSpecifier
    : TIME ZONE interval  #timeZoneInterval
    | TIME ZONE string    #timeZoneString
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

comparisonQuantifier
    : ALL | SOME | ANY
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL intervalValue from=intervalField (TO to=intervalField)?
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

intervalValue
    : (PLUS | MINUS)? INTEGER_VALUE
    | string
    ;

normalForm
    : NFD | NFC | NFKD | NFKC
    ;

type
    : ARRAY '<' type '>'
    | MAP '<' type ',' type '>'
    | STRUCT ('<' identifier ':' type (',' identifier ':' type)* '>')
    | ROW '(' identifier type (',' identifier type)* ')'
    | baseType ('(' typeParameter (',' typeParameter)* ')')?
    | INTERVAL from=intervalField TO to=intervalField
    ;

typeParameter
    : INTEGER_VALUE | type
    ;

baseType
    : TIME_WITH_TIME_ZONE
    | TIMESTAMP_WITH_TIME_ZONE
    | identifier
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

filter
    : FILTER '(' WHERE booleanExpression ')'
    ;

over
    : OVER '('
        (PARTITIONED BY partition+=expression (',' partition+=expression)*)?
        (ORDER BY sortItem (',' sortItem)*)?
        windowFrame?
      ')'
    ;

windowFrame
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING)  #boundedFrame // expression should be unsignedLiteral
    ;


explainOption
    : FORMAT value=(TEXT | GRAPHVIZ | JSON)                 #explainFormat
    | TYPE value=(LOGICAL | DISTRIBUTED | VALIDATE | IO)    #explainType
    ;

transactionMode
    : ISOLATION LEVEL levelOfIsolation    #isolationLevel
    | READ accessMode=(ONLY | WRITE)      #transactionAccessMode
    ;

levelOfIsolation
    : READ UNCOMMITTED                    #readUncommitted
    | READ COMMITTED                      #readCommitted
    | REPEATABLE READ                     #repeatableRead
    | SERIALIZABLE                        #serializable
    ;

callArgument
    : expression                    #positionalArgument
    | identifier '=>' expression    #namedArgument
    ;

pathElement
    : identifier '.' identifier     #qualifiedArgument
    | identifier                    #unqualifiedArgument
    ;

pathSpecification
    : pathElement (',' pathElement)*
    ;

privileges
    : privilege (',' privilege)*
    ;

privilege
    : CREATE BRANCH
    | ALTER
    | DELETE
    | DROP
    | SHOW
    | SELECT
    | INSERT
    | USE
    | DESC
    | DESCRIBE
    | RESTORE
    | UNDROP
    | CREATE
    | ALL
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

catalogName
    : catalog=identifier
    ;

databaseName
    : (catalog=identifier '.')? database=identifier
    ;

tableName
    : (databaseName '.')? table=identifier
    ;

materializedViewName
    : (databaseName '.')? materializedView=identifier
    ;

accName
    : tableName
    ;

tableId
    : (databaseName '.')? id=identifier
    ;

shareName
    : share=identifier
    ;

shareFullName
    : (shareproject=identifier) '.' (sharecatalog=identifier)
    ;

shareTableName
    : (shareproject=identifier) '.' (sharecatalog=identifier) '.' (sharedatabase=identifier) '.' (sharetable=identifier)
    ;

roleName
    : role=identifier
    ;

createTableHeader
    : CREATE EXTERNAL? TABLE (IF NOT EXISTS)? tableName
    ;

grantor
    : principal             #specifiedPrincipal
    | CURRENT_USER          #currentUserGrantor
    | CURRENT_ROLE          #currentRoleGrantor
    ;

principal
    : identifier            #unspecifiedPrincipal
    | USER identifier       #userPrincipal
    | ROLE identifier       #rolePrincipal
    ;

principalInfo
    : ugPrincipalInfo
    | rsPrincipalInfo
    ;

ugPrincipalInfo
    : USER principalSource identifier
    | GROUP principalSource identifier
    ;

rsPrincipalInfo
    : SHARE shareFullName
    | SHARE identifier
    | ROLE  identifier
    ;

principalSource
    : IAM     #IAMSource
    | SAML     #SAMSource
    | LDAP    #LDAPSource
    ;

objectType
    : CATALOG
    | DATABASE
    | TABLE
    | VIEW
    | SHARE
    | BRANCH
    ;

roles
    : identifier (',' identifier)*
    ;

columnAction
    : ADD (COLUMNS | COLUMN) '(' columnDefinition (',' columnDefinition)* ')'       #addColumns
    | REPLACE (COLUMNS | COLUMN) '(' columnDefinition (',' columnDefinition)* ')'   #replaceColumns
    | CHANGE (COLUMNS | COLUMN)? changeCol=identifier columnDefinition
            (',' changeCol=identifier columnDefinition)?                            #changeColumns
    | RENAME (COLUMNS | COLUMN) oldCol=identifier TO newCol=identifier
        (',' oldCol=identifier TO newCol=identifier)*                               #renameColumns
    | DROP (COLUMNS | COLUMN) identifier (',' identifier)*                          #dropColumns
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | QUOTED_IDENTIFIER      #quotedIdentifier
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
    ;

catlogName : identifier;
srcBranch : identifier;
destBranch : identifier;

number
    : DECIMAL_VALUE  #decimalLiteral
    | DOUBLE_VALUE   #doubleLiteral
    | INTEGER_VALUE  #integerLiteral
    ;

nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See SqlParser.exitNonReserved
    : ACCELERATOR | ADD | ADMIN | ALL | ANALYZE | ANY | ARRAY | ASC | AT | ACCESS
    | BERNOULLI
    | CALL | CASCADE | CATALOGS | CATALOG | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED | CURRENT | COLFILTER | COLMASK
    | DATA | DATE | DAY | DDL | DEFAULT | DESC | DESCRIBE | DISTRIBUTED | DML
    | ENGINE | ENGINES | EXCLUDING | EXPLAIN
    | FILTER | FIRST | FOLLOWING | FORMAT | FUNCTIONS
    | GRANT | GRANTED | GRANTS | GRAPHVIZ | GROUP
    | HOUR | HISTORY
    | ID | IF | INCLUDING | INPUT | INTERVAL | IO | ISOLATION | IAM
    | JOB | JSON
    | LAST | LATERAL | LEVEL | LIMIT | LINEAGE | LOGICAL | LDAP
    | MAP | MINUTE | MONTH | MATERIALIZED
    | NFC | NFD | NFKC | NFKD | NO | NONE | NULLIF | NULLS
    | ONLY | OPTION | ORDINALITY | OTHERS | OUTPUT | OVER
    | PARTITION | PARTITIONED | PARTITIONS | PATH | POSITION | PRECEDING | PRIVILEGES | PROPERTIES
    | RANGE | READ | RENAME | REPEATABLE | RETRIEVAL | REPLACE | RESET | RESTRICT | REVOKE | ROLE | ROLES | ROWFILTER
    | ROLLBACK | ROW | ROWS | REFRESH | POLICIES
    | SCHEMA | SCHEMAS | SECOND | SERIALIZABLE | SESSION | SET | SETS
    | SHOW | SOME | START | STATS | SUBSTRING | SYSTEM | SAML
    | TABLES | TABLESAMPLE | TEXT | TIME | TIMESTAMP | TO | TRANSACTION | TRY_CAST | TYPE
    | UNBOUNDED | UNCOMMITTED | USE | USER
    | VALIDATE | VERBOSE | VIEW
    | WORK | WRITE
    | YEAR
    | ZONE
    ;

ACCELERATOR: 'ACCELERATOR';
ACCELERATORS: 'ACCELERATORS';
ACCESS: 'ACCESS';
ADD: 'ADD';
ADMIN: 'ADMIN';
AGENCY_NAME: 'AGENCY_NAME';
ALL: 'ALL';
ALLOWED_LOCATIONS: 'ALLOWED_LOCATIONS';
ALTER: 'ALTER';
ANALYZE: 'ANALYZE';
AND: 'AND';
ANY: 'ANY';
ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
AT: 'AT';
ATTACH: 'ATTACH';
BERNOULLI: 'BERNOULLI';
BETWEEN: 'BETWEEN';
BLOCKED_LOCATIONS: 'BLOCKED_LOCATIONS';
BRANCH: 'BRANCH';
BRANCHES: 'BRANCHES';
BY: 'BY';
CALL: 'CALL';
CASCADE: 'CASCADE';
CASE: 'CASE';
CAST: 'CAST';
CATALOG: 'CATALOG';
CATALOGS: 'CATALOGS';
CHANGE: 'CHANGE';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
COMMENT: 'COMMENT';
COMMIT: 'COMMIT';
COMMITTED: 'COMMITTED';
COMPACT: 'COMPACT';
CONSTRAINT: 'CONSTRAINT';
COPY: 'COPY';
CREATE: 'CREATE';
CROSS: 'CROSS';
CUBE: 'CUBE';
CURRENT: 'CURRENT';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_PATH: 'CURRENT_PATH';
CURRENT_ROLE: 'CURRENT_ROLE';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
CURRENT_USER: 'CURRENT_USER';
DATA: 'DATA';
DATABASE: 'DATABASE';
DATABASES: 'DATABASES';
DATAGEN: 'DATAGEN';
DATE: 'DATE';
DAY: 'DAY';
DBPROPERTIES: 'DBPROPERTIES';
DDL: 'DDL';
DEALLOCATE: 'DEALLOCATE';
DELEGATE: 'DELEGATE';
DELEGATES: 'DELEGATES';
DELETE: 'DELETE';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DETACH: 'DETACH';
DISTINCT: 'DISTINCT';
DISTRIBUTED: 'DISTRIBUTED';
DROP: 'DROP';
DEFAULT: 'DEFAULT';
DML: 'DML';
ELSE: 'ELSE';
END: 'END';
ENGINE: 'ENGINE';
ENGINES: 'ENGINES';
ESCAPE: 'ESCAPE';
EXCEPT: 'EXCEPT';
EXCLUDING: 'EXCLUDING';
EXECUTE: 'EXECUTE';
EXISTS: 'EXISTS';
EXPLAIN: 'EXPLAIN';
EXTENDED : 'EXTENDED';
EXTERNAL: 'EXTERNAL';
EXTRACT: 'EXTRACT';
FALSE: 'FALSE';
FILE_FORMAT: 'FILE_FORMAT';
DELIMITER: 'DELIMITER';
FILTER: 'FILTER';
FIRST: 'FIRST';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FORMAT: 'FORMAT';
FORMATTED : 'FORMATTED';
FROM: 'FROM';
FULL: 'FULL';
FUNCTIONS: 'FUNCTIONS';
GRANT: 'GRANT';
GRANTED: 'GRANTED';
GRANTS: 'GRANTS';
GRAPHVIZ: 'GRAPHVIZ';
GROUP: 'GROUP';
GROUPING: 'GROUPING';
HAVING: 'HAVING';
HISTORY: 'HISTORY';
HOUR: 'HOUR';
ID: 'ID';
IF: 'IF';
IN: 'IN';
INCLUDING: 'INCLUDING';
INNER: 'INNER';
INPUT: 'INPUT';
INSERT: 'INSERT';
INTERSECT: 'INTERSECT';
INTERVAL: 'INTERVAL';
INTO: 'INTO';
IO: 'IO';
IS: 'IS';
ISOLATION: 'ISOLATION';
JOB: 'JOB';
JOIN: 'JOIN';
JSON: 'JSON';
KAFKA: 'KAFKA';
SOCKET: 'SOCKET';
LAST: 'LAST';
LATERAL: 'LATERAL';
LEFT: 'LEFT';
LEVEL: 'LEVEL';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LINEAGE: 'LINEAGE';
LOCALTIME: 'LOCALTIME';
LOCALTIMESTAMP: 'LOCALTIMESTAMP';
LOCATION: 'LOCATION';
LOGICAL: 'LOGICAL';
MAP: 'MAP';
MINUTE: 'MINUTE';
MONTH: 'MONTH';
MAINTAIN: 'MAINTAIN';
NATURAL: 'NATURAL';
NFC : 'NFC';
NFD : 'NFD';
NFKC : 'NFKC';
NFKD : 'NFKD';
NO: 'NO';
NONE: 'NONE';
NORMALIZE: 'NORMALIZE';
NOT: 'NOT';
NULL: 'NULL';
NULLIF: 'NULLIF';
NULLS: 'NULLS';
OF: 'OF';
ON: 'ON';
ONLY: 'ONLY';
OPTION: 'OPTION';
OR: 'OR';
ORDER: 'ORDER';
ORDINALITY: 'ORDINALITY';
OUTER: 'OUTER';
OUTPUT: 'OUTPUT';
OVER: 'OVER';
OVERWRITE: 'OVERWRITE';
PARTITION: 'PARTITION';
PARTITIONED: 'PARTITIONED';
PARTITIONS: 'PARTITIONS';
PATH: 'PATH';
POSITION: 'POSITION';
PRECEDING: 'PRECEDING';
PREPARE: 'PREPARE';
PRIVILEGES: 'PRIVILEGES';
PROPERTIES: 'PROPERTIES';
PROVIDER_DOMAIN_NAME: 'PROVIDER_DOMAIN_NAME';
PURGE : 'PURGE';
RANGE: 'RANGE';
READ: 'READ';
RECURSIVE: 'RECURSIVE';
REMOVE: 'REMOVE';
RENAME: 'RENAME';
REPEATABLE: 'REPEATABLE';
REPLACE: 'REPLACE';
RESET: 'RESET';
RESTORE: 'RESTORE';
RESTRICT: 'RESTRICT';
RETRIEVAL: 'RETRIEVAL';
REVOKE: 'REVOKE';
RIGHT: 'RIGHT';
ROLE: 'ROLE';
ROLES: 'ROLES';
ROLLBACK: 'ROLLBACK';
ROLLUP: 'ROLLUP';
ROW: 'ROW';
ROWS: 'ROWS';
SCHEMA: 'SCHEMA';
SCHEMAS: 'SCHEMAS';
SECOND: 'SECOND';
SELECT: 'SELECT';
SERIALIZABLE: 'SERIALIZABLE';
SESSION: 'SESSION';
SET: 'SET';
SETS: 'SETS';
SHOW: 'SHOW';
SOME: 'SOME';
START: 'START';
STATS: 'STATS';
STMPROPERTIES: 'STMPROPERTIES';
STORAGE_PROVIDER: 'STORAGE_PROVIDER';
OPTIONS: 'OPTIONS';
OTHERS: 'OTHERS';
STOP: 'STOP';
STORED: 'STORED';
STREAM: 'STREAM';
STREAMS: 'STREAMS';
STRUCT: 'STRUCT';
SUBSTRING: 'SUBSTRING';
SYSTEM: 'SYSTEM';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLESAMPLE: 'TABLESAMPLE';
TBLPROPERTIES: 'TBLPROPERTIES';
TEXT: 'TEXT';
THEN: 'THEN';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
TO: 'TO';
TRANSACTION: 'TRANSACTION';
TRUE: 'TRUE';
TRY_CAST: 'TRY_CAST';
TYPE: 'TYPE';
UESCAPE: 'UESCAPE';
UNBOUNDED: 'UNBOUNDED';
UNCOMMITTED: 'UNCOMMITTED';
UNDROP: 'UNDROP';
UNION: 'UNION';
UNNEST: 'UNNEST';
UNSET: 'UNSET';
USE: 'USE';
MERGE: 'MERGE';
USER: 'USER';
USING: 'USING';
VALIDATE: 'VALIDATE';
VALUES: 'VALUES';
VERBOSE: 'VERBOSE';
VERSION: 'VERSION';
VIEW: 'VIEW';
VIEWS: 'VIEWS';
MATERIALIZED: 'MATERIALIZED';
REFRESH: 'REFRESH';
WHEN: 'WHEN';
WHERE: 'WHERE';
WITH: 'WITH';
WITH_HEADER: 'WITH_HEADER';
WORK: 'WORK';
WRITE: 'WRITE';
YEAR: 'YEAR';
ZONE: 'ZONE';
UPSTREAM: 'UPSTREAM';
DOWNSTREAM: 'DOWNSTREAM';

POLICIES: 'POLICIES';
IAM: 'IAM';
SAML: 'SAML';
LDAP: 'LDAP';
EFFECT: 'EFFECT';
CONDITIONS: 'CONDITIONS';
ROWFILTER: 'ROWFILTER';
COLFILTER: 'COLFILTER';
COLMASK: 'COLMASK';
WITH_GRANT: 'WITH_GRANT';
ENCRYPT: 'ENCRYPT';
VOID: 'VOID';

EQ  : '=';
NEQ : '<>';
NEQJ: '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';
SHARE: 'SHARE';
SHARES: 'SHARES';
ACCOUNTS: 'ACCOUNTS';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

UNICODE_STRING
    : 'U&\'' ( ~'\'' | '\'\'' )* '\''
    ;

// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    :  'X\'' (~'\'')* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

//1.24E10
DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_' | '@' | ':')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

TIME_WITH_TIME_ZONE
    : 'TIME' WS 'WITH' WS 'TIME' WS 'ZONE'
    ;

TIMESTAMP_WITH_TIME_ZONE
    : 'TIMESTAMP' WS 'WITH' WS 'TIME' WS 'ZONE'
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

// comment to HIDDEN channel
SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

// comment to HIDDEN channel
BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

// whitespace to HIDDEN channel
WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

UNRECOGNIZED
    : .
    ;

