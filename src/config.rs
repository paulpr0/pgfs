//! Specify config as a toml file
//!
//!
//! Example:
//! ```
//! [database]
//!
//! database = "127.0.0.1/mydb"
//! user = "paul"
//! pass = "word"
//!
//!
//! [default]
//!
//! uid = 1001
//! gid = 1001
//! access = "readonly"
//! mountpoint = "/tmp/pgfs"
//! 
//! [pics]
//!
//! table_name = "pics"
//! data_type = "bytea"
//! id_field= "id"
//! length_field = "length"
//! data_field = "image"
//! name_field = "name"
//! data_query = "select id, 'image_'||id || regexp_replace(mime_type, '^.*/','.') as name, octet_length(image) as length from pics;"
//! readonly = true
//!
//! [filestest]
//!
//! table_name = "files_test"
//! data_type = "bytea"
//! id_field = "id"
//! length_field = "length"
//! data_field = "file"
//! created_date_field = "created"
//! modified_date_field = "modified"
//! data_query = "select id, name,  octet_length(file) as length, created, modified from files_test"
//! readonly = false
//!```
//!

use std::collections::HashMap;
use std::error::Error;
use toml::Value;

/// Main config object - contains config for connecting to a postgres database and a TableConfig
/// for each table.
///
/// Each table has a name which corresponds to the directory name under the root directory where
/// you will see the files.
#[derive(Debug)]
pub struct PgfsConfig {
    pub table_config: HashMap<String,TableConfig>,
    pub connection_string: Option<String>,
    pub mountpoint: String,
}

/// The config for an individual table/query. If mapping a table with a file as bytea, a name, an id
/// which is an normal postgres int4 then you can mostly use the defaults. For more complicated
/// table expressions, use the query configs to specify how to perform CRUD operations on your data.
#[derive(Clone, Debug)]
pub struct TableConfig {
    ///This will be the directory name under the root directory. It does not need to be the same as the
    /// actual table name in postgres if you specify the queries to retrieve data, but the default queries will
    /// use the table name
    pub table_name: String,
    ///Currently the only supported value is bytea. We should support text as well as a minimum.
    pub data_type: String,
    ///The id field. This should map 1:1 to a file
    pub id_field: String,
    ///This field should contain length of the file in bytes.
    pub length_field: String,
    ///The name of the field which contains the data (content of the file)
    pub data_field: String,
    ///This field should contain the name of the file. This should be unique per table as the contents
    /// of each table are in the same directory.
    pub name_field: String,
    ///Query (typically a select query) to fetch the id, name, length (size in bytes), and optionally the created and modified
    ///dates and times. The names of the columns must match those specified by the id_field, length_field, name_field.
    pub data_query: String,
    pub create_query: Option<String>,
    pub update_query: Option<String>,
    pub delete_query: Option<String>,
    pub read_only: bool,
    ///Optionally provide the uid for the owner of the files in this table. By default this is set to the user
    /// who mounts the directory. Maybe it should default to the owner of the mount (or that could be a config)
    pub uid: Option<u32>,
    ///Optionally provide the gid for the group owner of the files in this table. By default this is set to the group
    /// of the user who mounts the directory.
    pub gid: Option<u32>,

    pub created_date_field:Option<String>,
    pub modified_date_field: Option<String>,
    
}

impl PgfsConfig {
    pub fn new(data: &str) -> Result<PgfsConfig,Box<dyn Error>> {

        let mut result = PgfsConfig {
            table_config: HashMap::new(),
            connection_string: None,
            mountpoint: "/tmp/pgfs".to_string(),
        };

        let empty_string_value = Value::String("".to_string());

        let tml : toml::Value = toml::from_str(data)?;
        if let Some(database) = tml.get("database") {
            if let Some(table) = database.as_table() {
                let db = table.get("database").unwrap_or(&empty_string_value).as_str().unwrap_or("");
                let user = table.get("user").unwrap_or(&empty_string_value).as_str().unwrap_or("");
                let pass = table.get("pass").unwrap_or(&empty_string_value).as_str().unwrap_or("");
                result.connection_string = Some(format!("postgres://{}:{}@{}",user,pass,db))
            } else if let Some(db_string) = database.as_str() {
                result.connection_string = Some(db_string.to_string());
            }
        }

        //set mountpoint
        if let Some(mountpoint_value) = tml.get("mountpoint") {
            result.mountpoint = mountpoint_value.as_str().unwrap_or(result.mountpoint.as_str()).to_string();
        }

        //get defaults
        let mut defaults = TableConfig {
            table_name: "".to_string(),
            data_type: "bytea".to_string(),
            id_field: "id".to_string(),
            length_field: "length".to_string(),
            data_field: "data".to_string(),
            name_field: "name".to_string(),
            data_query: "".to_string(),
            create_query: None,
            update_query: None,
            delete_query: None,
            read_only:true,
            uid: None,
            gid: None,
            created_date_field:None,
            modified_date_field: None,

      //      database: None,
      //      user: None,
      //      pass: None
        };
        if let Some(default) = tml.get("default") {
            if let Some(table_name) = default.get("table_name") {
                defaults.table_name = table_name.as_str().unwrap().to_string();
            }
            if let Some(data_type) = default.get("data_type") {
                defaults.data_type = data_type.as_str().unwrap().to_string();
            }
            if let Some(id_field) = default.get("id_field") {
                defaults.id_field = id_field.as_str().unwrap().to_string();
            }
            if let Some(length_field) = default.get("length_field") {
                defaults.length_field = length_field.as_str().unwrap().to_string();
            }
            if let Some(data_field) = default.get("data_field") {
                defaults.data_field = data_field.as_str().unwrap().to_string();
            }
            if let Some(name_field) = default.get("name_field") {
                defaults.name_field = name_field.as_str().unwrap().to_string();
            }
            if let Some(data_query) = default.get("data_query") {
                defaults.data_query = data_query.as_str().unwrap().to_string();
            }
            if let Some(read_only) = default.get("read_only") {
                defaults.read_only = read_only.as_bool().unwrap_or(true);
            }
            if let Some(uid) = default.get("uid") {
                defaults.uid = uid.as_integer().map(|x|x as u32);
            }
            if let Some(gid) = default.get("gid") {
                defaults.gid = gid.as_integer().map(|x|x as u32);
            }
            if let Some(created_date_field) = default.get("created_date_field") {
                defaults.created_date_field = Some(created_date_field.as_str().unwrap().to_string());
            }
            if let Some(modified_date_field) = default.get("modified_date_field") {
                defaults.modified_date_field = Some(modified_date_field.as_str().unwrap().to_string());
            }
       }

        let tables = tml.as_table().unwrap();
        for (table_name, table) in tables.iter() {
            if table_name == "default" || table_name == "database" || table_name == "mountpoint" {
                continue
            }
            let mut t = defaults.clone();
            if let Some(table_name) = table.get("table_name") {
                t.table_name = table_name.as_str().unwrap().to_string();
            }
            if let Some(data_type) = table.get("data_type") {
                t.data_type = data_type.as_str().unwrap().to_string();
            }
            if let Some(id_field) = table.get("id_field") {
                t.id_field = id_field.as_str().unwrap().to_string();
            }
            if let Some(length_field) = table.get("length_field") {
                t.length_field = length_field.as_str().unwrap().to_string();
            }
            if let Some(data_field) = table.get("data_field") {
                t.data_field = data_field.as_str().unwrap().to_string();
            }
            if let Some(name_field) = table.get("name_field") {
                t.name_field = name_field.as_str().unwrap().to_string();
            }
            if let Some(data_query) = table.get("data_query") {
                t.data_query = data_query.as_str().unwrap().to_string();
            }
            if let Some(read_only) = table.get("read_only") {
                t.read_only = read_only.as_bool().unwrap_or(true);
            }
            if let Some(uid) = table.get("uid") {
                t.uid = uid.as_integer().map(|x|x as u32);
            }
            if let Some(gid) = table.get("gid") {
                t.gid = gid.as_integer().map(|x|x as u32);
            }
            if let Some(created_date_field) = table.get("created_date_field") {
                t.created_date_field = Some(created_date_field.as_str().unwrap().to_string());
            }
            if let Some(modified_date_field) = table.get("modified_date_field") {
                t.modified_date_field = Some(modified_date_field.as_str().unwrap().to_string());
            }

            result.table_config.insert(table_name.to_string(), t);
        }


        Ok(result)

    }
}
