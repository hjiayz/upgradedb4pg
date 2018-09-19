#[macro_use]
extern crate clap;
extern crate dotenv;
#[macro_use]
extern crate failure;
extern crate postgres;
extern crate rustyline;
use clap::{App, AppSettings, Arg, SubCommand};
use dotenv::dotenv;
use failure::Error;
use postgres::{Connection, TlsMode};
use std::env;
fn main() {
    dotenv().ok();
    let mut database_url = env::var("DATABASE_URL").unwrap_or_default();
    let mut db_init_file = env::var("DB_INIT_FILE").unwrap_or("init.sql".to_owned());
    let matches = App::new("database revision control tool")
        .version(crate_version!())
        .arg(
            Arg::with_name("database_url")
                .short("d")
                .long("database_url")
                .value_name("DATABASE URL"),
        ).arg(
            Arg::with_name("db_init_file")
                .short("f")
                .long("db_init_file")
                .value_name("DATABASE INITIALIZATION FILE"),
        ).arg(Arg::with_name("debug"))
        .subcommand(
            SubCommand::with_name("upgrade").about("Upgrade database schema to newest version"),
        ).subcommand(SubCommand::with_name("repl").about("Read eval print loop"))
        .subcommand(SubCommand::with_name("rebuild").about("Drop and rebuild database and upgrade"))
        .subcommand(
            SubCommand::with_name("load")
                .about("Load schema from database and create database initialization file"),
        ).setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();
    if let Some(url) = matches.value_of("database_url") {
        database_url = url.to_owned();
    };
    if let Some(path) = matches.value_of("db_init_file") {
        db_init_file = path.to_owned();
    };
    let mut debug_mode = false;
    if let Some(_) = matches.value_of("debug") {
        debug_mode = true;
    };
    let conn = || match Connection::connect(database_url.clone(), TlsMode::None) {
        Err(err) => {
            err_exit(err.into(), debug_mode);
            unimplemented!();
        }
        Ok(conn) => conn,
    };
    let result = match matches.subcommand() {
        ("upgrade", Some(_args)) => upgrade(&conn(), &database_url, &db_init_file).map(|_| ()),
        ("repl", Some(_args)) => repl(&conn(), &database_url, &db_init_file),
        ("load", Some(_args)) => load(&conn(), &db_init_file),
        ("rebuild", Some(_args)) => rebuild(&database_url, &db_init_file, debug_mode),
        _ => Ok(()),
    };
    if result.is_err() {
        err_exit(result.unwrap_err(), debug_mode);
    };
}

fn err_exit(e: Error, mode: bool) {
    use std::process::exit;
    if mode {
        eprintln!("{:?}", e);
    } else {
        eprintln!("{}", e);
    };
    exit(1);
}

fn upgrade(
    conn: &Connection,
    database_url: &str,
    db_init_file: &str,
) -> Result<Vec<String>, Error> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    println!("UPGRADE DATABASE");
    conn.execute("CREATE TABLE IF NOT EXISTS db_init_log ( line_number BIGINT PRIMARY KEY , statement TEXT )", &[])?;
    let rows = conn.query(
        "select line_number,statement from db_init_log order by line_number ASC",
        &[],
    )?;
    let mut prev = -1i64;
    if rows.iter().any(|row| {
        prev += 1;
        prev != row.get("line_number")
    }) {
        return Err(format_err!("Line number is discontinuous."));
    };
    let mut slines: Vec<String> = rows.iter().map(|row| row.get("statement")).collect();
    let file = File::open(db_init_file)?;
    let mut flines = BufReader::new(file).lines();
    let mut line_number = 0usize;
    while let Some(Ok(fline)) = flines.next() {
        if let Some(sline) = slines.get(line_number) {
            if sline != &fline {
                return Err(format_err!(
                    "Conflict --> \n{} : {}\n{}\n{}\n{}",
                    db_init_file,
                    line_number,
                    fline,
                    database_url,
                    sline
                ));
            }
            line_number += 1;
            continue;
        };
        let trans = conn.transaction()?;
        trans.execute(&fline, &[])?;
        trans.execute(
            "INSERT INTO db_init_log (line_number, statement) VALUES ($1, $2)",
            &[&(line_number as i64), &fline],
        )?;
        trans.commit()?;
        slines.push(fline);
        line_number += 1;
    }
    if line_number < slines.len() {
        return Err(format_err!(
            "The database schema in your database is newer than the database initialization file \"{}\""
        ,db_init_file));
    };
    println!("OK");
    Ok(slines)
}

fn repl(conn: &Connection, database_url: &str, db_init_file: &str) -> Result<(), Error> {
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};
    let init_file_path = Path::new(db_init_file);
    if !init_file_path.exists() {
        File::create(db_init_file)?;
        println!("A file {} is created.", db_init_file);
    };
    if !init_file_path.is_file() {
        return Err(format_err!("{} not a file", db_init_file));
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(db_init_file)?;
    let slines = upgrade(conn, database_url, db_init_file)?;
    let mut line_number = slines.len().checked_sub(20).unwrap_or(0);
    for line in slines.iter().skip(line_number) {
        println!("{}:{}", line_number, line);
        line_number += 1;
    }
    let mut rl = rustyline::Editor::<()>::new();
    while let Ok(line) = rl.readline(">> ") {
        rl.add_history_entry(line.as_ref());
        let now = format!(
            "/* {} */",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()
        );
        let trans = conn.transaction()?;
        match trans.execute(&line, &[]) {
            Ok(_) => {
                println!("OK");
                file.write_fmt(format_args!("{}\n{}\n", line, now))?;
                file.flush()?;
                trans.execute(
                    "INSERT INTO db_init_log (line_number, statement) VALUES ($1, $2),($3, $4)",
                    &[
                        &(line_number as i64),
                        &line,
                        &(line_number as i64 + 1),
                        &now,
                    ],
                )?;
                trans.commit()?;
                line_number += 2;
            }
            Err(e) => {
                eprintln!("{}", e);
            }
        }
    }
    Err(format_err!("bye bye"))
}

fn load(conn: &Connection, db_init_file: &str) -> Result<(), Error> {
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;
    let init_file_path = Path::new(db_init_file);
    if init_file_path.exists() {
        return Err(format_err!("Path {} already exists", db_init_file));
    };
    let mut file = File::create(db_init_file)?;
    println!("A file {} is created.", db_init_file);
    let rows = conn.query(
        "select line_number,statement from db_init_log order by line_number ASC",
        &[],
    )?;
    let mut prev = -1i64;
    if rows.iter().any(|row| {
        prev += 1;
        prev != row.get("line_number")
    }) {
        return Err(format_err!("Line number is discontinuous."));
    };
    for row in rows.iter() {
        file.write_fmt(format_args!("{}\n", row.get::<_, String>("statement")))?;
        file.flush()?;
    }
    println!("LOAD INITIALIZATION FILE FROM DATABASE SUCCEED");
    Ok(())
}

fn rebuild(database_url: &str, db_init_file: &str, debug_mode: bool) -> Result<(), Error> {
    use postgres::params::ConnectParams;
    use postgres::params::IntoConnectParams;
    if !debug_mode {
        return Err(format_err!(
            "The rebuild command will delete data and only be used in Debug mode."
        ));
    }
    let connect_params = match database_url.into_connect_params() {
        Ok(cp) => cp,
        Err(e) => return Err(format_err!("{}", e)),
    };
    let mut builder = ConnectParams::builder();
    builder
        .port(connect_params.port())
        .connect_timeout(connect_params.connect_timeout());
    let username = match connect_params.user() {
        Some(user) => {
            builder.user(user.name(), user.password());
            user.name()
        }
        None => {
            return Err(format_err!("no username"));
        }
    };
    for (name, value) in connect_params.options() {
        builder.option(&name, &value);
    }
    let without_database_connect_params = builder.build(connect_params.host().to_owned());
    let database_name: String = connect_params
        .database()
        .ok_or(format_err!("database name unset."))?
        .to_owned();
    if database_name.len() == 0 {
        return Err(format_err!("database name unset."));
    };
    let conn = Connection::connect(without_database_connect_params, TlsMode::None)?;
    if conn.execute(
        "select * from pg_user where usecreatedb = $1 AND usename = $2",
        &[&true, &username],
    )? == 1
    {
        println!(
            "terminate the connections to the {} database",
            &database_name
        );
        conn.execute("select pg_terminate_backend (pg_stat_activity.pid) from pg_stat_activity where pg_stat_activity.datname = $1",&[&database_name])?;
        println!("OK");
        println!("drop database {}", &database_name);
        conn.execute(&format!("DROP DATABASE IF EXISTS {}", &database_name), &[])?;
        println!("OK");
        println!("create database {}", &database_name);
        conn.execute(&format!("CREATE DATABASE {}", &database_name), &[])?;
        println!("OK");
    } else {
        return Err(format_err!("user {} no create database ability", &username));
    };
    upgrade(
        &Connection::connect(connect_params.clone(), TlsMode::None)?,
        database_url,
        db_init_file,
    )?;
    println!("REBUILD OK");
    Ok(())
}
