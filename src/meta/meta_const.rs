// catalog
pub const CATALOG_NAME: &str = "def";

// schema name of def
pub const SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA: &str = "information_schema";
pub const FULL_SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA: &str = "def.information_schema";
pub const SCHEMA_NAME_OF_DEF_MYSQL: &str = "mysql";
pub const FULL_SCHEMA_NAME_OF_DEF_MYSQL: &str = "def.mysql";
pub const SCHEMA_NAME_OF_DEF_PERFORMANCE_SCHEMA: &str = "performance_schema";
pub const FULL_SCHEMA_NAME_OF_DEF_PERFORMANCE_SCHEMA: &str = "def.performance_schema";

// table name of information_schema
pub const TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES: &str = "tables";
pub const FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES: &str = "def.information_schema.tables";
pub const TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS: &str = "columns";
pub const FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS: &str = "def.information_schema.columns";
pub const TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS: &str = "statistics";
pub const FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS: &str = "def.information_schema.statistics";
pub const TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA: &str = "schemata";
pub const FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA: &str = "def.information_schema.schemata";
pub const TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_DUAL: &str = "dual";
pub const FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_DUAL: &str = "def.information_schema.dual";
pub const TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_CHECK_CONSTRAINTS: &str = "check_constraints";
pub const FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_CHECK_CONSTRAINTS: &str = "def.information_schema.check_constraints";
pub const TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_KEY_COLUMN_USAGE: &str = "key_column_usage";
pub const FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_KEY_COLUMN_USAGE: &str = "def.information_schema.key_column_usage";
pub const TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLE_CONSTRAINTS: &str = "table_constraints";
pub const FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLE_CONSTRAINTS: &str = "def.information_schema.table_constraints";
pub const TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_REFERENTIAL_CONSTRAINTS: &str = "referential_constraints";
pub const FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_REFERENTIAL_CONSTRAINTS: &str = "def.information_schema.referential_constraints";

// table name of mysql
pub const TABLE_NAME_OF_DEF_MYSQL_USERS: &str = "user";
pub const FULL_TABLE_NAME_OF_DEF_MYSQL_USERS: &str = "def.mysql.user";
// table name of performance_schema
pub const TABLE_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES: &str = "global_variables";
pub const FULL_TABLE_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES: &str = "def.performance_schema.global_variables";

pub const PRIMARY_NAME: &str = "PRIMARY";
pub const CONSTRAINT_TYPE_PRIMARY: &str = "PRIMARY KEY";
pub const CONSTRAINT_TYPE_UNIQUE: &str = "UNIQUE";

pub const MYSQL_DATA_TYPE_CHAR: &str = "char";
pub const MYSQL_DATA_TYPE_VARCHAR: &str = "varchar";
pub const MYSQL_DATA_TYPE_TEXT: &str = "text";
pub const MYSQL_DATA_TYPE_MEDIUMINT: &str = "mediumint";
pub const MYSQL_DATA_TYPE_SMALLINT: &str = "smallint";
pub const MYSQL_DATA_TYPE_INT: &str = "int";
pub const MYSQL_DATA_TYPE_BIGINT: &str = "bigint";
pub const MYSQL_DATA_TYPE_DECIMAL: &str = "decimal";
pub const MYSQL_DATA_TYPE_ENUM: &str = "enum";

pub const MYSQL_ERROR_CODE_UNKNOWN_ERROR: u16 = 1105;

pub const COLUMN_ROWID: &str = "rowid";
pub const COLUMN_INFORMATION_SCHEMA_TABLE_CATALOG: &str = "table_catalog";
pub const COLUMN_INFORMATION_SCHEMA_TABLE_SCHEMA: &str = "table_schema";
pub const COLUMN_INFORMATION_SCHEMA_TABLE_NAME: &str = "table_name";
pub const COLUMN_INFORMATION_SCHEMA_COLUMN_NAME: &str = "column_name";
pub const COLUMN_INFORMATION_SCHEMA_ORDINAL_POSITION: &str = "ordinal_position";

// column of def.information_schema.tables
pub const COLUMN_DEF_INFORMATION_SCHEMA_TABLES_TABLE_CATALOG: &str = "table_catalog";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_SCHEMA: &str = "table_schema";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_SCHEMA: &str = "def.information_schema.tables.table_schema";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_NAME: &str = "table_name";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_NAME: &str = "def.information_schema.tables.table_name";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_TYPE: &str = "table_type";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_TYPE: &str = "def.information_schema.tables.table_type";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_ENGINE: &str = "engine";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_ENGINE: &str = "def.information_schema.tables.engine";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_VERSION: &str = "version";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_VERSION: &str = "def.information_schema.tables.version";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_DATA_LENGTH: &str = "data_length";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_DATA_LENGTH: &str = "def.information_schema.tables.data_length";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_INDEX_LENGTH: &str = "index_length";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_INDEX_LENGTH: &str = "def.information_schema.tables.index_length";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_AUTO_INCREMENT: &str = "auto_increment";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_AUTO_INCREMENT: &str = "def.information_schema.tables.auto_increment";
// column of def.information_schema.statistics
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_CATALOG: &str = "table_catalog";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_CATALOG: &str = "def.information_schema.statistics.table_catalog";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_SCHEMA: &str = "table_schema";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_SCHEMA: &str = "def.information_schema.statistics.table_schema";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_NAME: &str = "table_name";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_NAME: &str = "def.information_schema.statistics.table_name";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_COLUMN_NAME: &str = "column_name";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_COLUMN_NAME: &str = "def.information_schema.statistics.column_name";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_NON_UNIQUE: &str = "non_unique";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_NON_UNIQUE: &str = "def.information_schema.statistics.non_unique";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_INDEX_NAME: &str = "index_name";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_INDEX_NAME: &str = "def.information_schema.statistics.index_name";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_SEQ_IN_INDEX: &str = "seq_in_index";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_SEQ_IN_INDEX: &str = "def.information_schema.statistics.seq_in_index";
// column of def.information_schema.key_column_usage
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_KEY_COLUMN_USAGE_CONSTRAINT_CATALOG: &str = "constraint_catalog";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_KEY_COLUMN_USAGE_CONSTRAINT_SCHEMA: &str = "constraint_schema";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_KEY_COLUMN_USAGE_CONSTRAINT_NAME: &str = "constraint_name";
// column of def.information_schema.schemata
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_CATALOG_NAME: &str = "catalog_name";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_SCHEMA_NAME: &str = "schema_name";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_SCHEMA_NAME: &str = "def.information_schema.schemata.schema_name";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_DEFAULT_CHARACTER_SET_NAME: &str = "default_character_set_name";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_DEFAULT_CHARACTER_SET_NAME: &str = "def.information_schema.schemata.default_character_set_name";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_DEFAULT_COLLATION_NAME: &str = "default_collation_name";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_DEFAULT_COLLATION_NAME: &str = "def.information_schema.schemata.default_collation_name";
// column of def.information_schema.columns
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_CATALOG: &str = "table_catalog";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_SCHEMA: &str = "table_schema";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_SCHEMA: &str = "def.information_schema.columns.table_schema";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_NAME: &str = "table_name";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_NAME: &str = "def.information_schema.columns.table_name";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_COLUMN_NAME: &str = "column_name";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_COLUMN_NAME: &str = "def.information_schema.columns.column_name";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_ORDINAL_POSITION: &str = "ordinal_position";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_ORDINAL_POSITION: &str = "def.information_schema.columns.ordinal_position";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_IS_NULLABLE: &str = "is_nullable";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_IS_NULLABLE: &str = "def.information_schema.columns.is_nullable";
pub const COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_DATA_TYPE: &str = "data_type";
pub const FULL_COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_DATA_TYPE: &str = "def.information_schema.columns.data_type";
// column of def.performance_schema.global_variables
pub const COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_NAME: &str = "variable_name";
pub const FULL_COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_NAME: &str = "def.performance_schema.global_variables.variable_name";
pub const COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_VALUE: &str = "variable_value";
pub const FULL_COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_VALUE: &str = "def.performance_schema.global_variables.variable_value";

pub const OPTION_TABLE_TYPE: &str = "TABLE_TYPE";
pub const OPTION_TABLE_TYPE_BASE_TABLE: &str = "BASE TABLE";
pub const OPTION_TABLE_TYPE_SYSTEM_VIEW: &str = "SYSTEM VIEW";
pub const OPTION_TABLE_TYPE_VIEW: &str = "VIEW";
pub const OPTION_ENGINE: &str = "engine";
pub const OPTION_ENGINE_NAME_ROCKSDB: &str = "rocksdb";
pub const OPTION_ENGINE_NAME_SLED: &str = "sled";

// SHOW ......
pub const SHOW_VARIABLE_DATABASES: &str = "DATABASES";
pub const SHOW_VARIABLE_CREATE: &str = "CREATE";
pub const SHOW_VARIABLE_CREATE_TABLE: &str = "TABLE";
pub const SHOW_VARIABLE_VARIABLES: &str = "VARIABLES";
pub const SHOW_VARIABLE_GRANTS: &str = "GRANTS";
pub const SHOW_VARIABLE_PRIVILEGES: &str = "PRIVILEGES";
pub const SHOW_VARIABLE_ENGINES: &str = "ENGINES";
pub const SHOW_VARIABLE_CHARSET: &str = "CHARSET";
pub const SHOW_VARIABLE_COLLATION: &str = "COLLATION";

pub const SIGN_MASK: u64 = 0x8000000000000000;
