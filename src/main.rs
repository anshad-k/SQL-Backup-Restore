use dotenvy::dotenv;
use serde_json;
use std::error::Error;
use postgres::{Client, NoTls};
use std::fs::write;
use std::process::Command;


const BASE_URL: &str = "https://hackattic.com/challenges/backup_restore";
const OUTPUT_FILE: &str = "/tmp/dump.sql";

fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    let access_token = get_env("ACCESS_TOKEN");
    let postgres_host = get_env("DB_HOST");
    let postgres_db = get_env("DB_NAME");
    let postgres_user = get_env("DB_USER");
    let postgres_password = get_env("DB_PASSWORD");

    let problem_url = format!("{}/problem?access_token={}", BASE_URL, access_token);
    let solution_url = format!("{}/solve?access_token={}&playground=1", BASE_URL, access_token);
    let postgres_url = format!(
        "host={} dbname={} user={} password={}",
        postgres_host, postgres_db, postgres_user, postgres_password
    );
  
    let response = json_get(&problem_url)?;
    
    let encoded_bytes = response["dump"].as_str().ok_or("Missing 'dump' field")?;
    let decoded_bytes = base64_decode(encoded_bytes)?;

    let sql_sump = gzip_decompress(decoded_bytes)?;
    let sql = String::from_utf8(sql_sump)?;

    write(OUTPUT_FILE, sql)?;

    let output = Command::new("psql")
        .arg("-U")
        .arg(postgres_user)
        .arg("-d")
        .arg(postgres_db)
        .arg("-f")
        .arg(OUTPUT_FILE)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)?;
        eprintln!("Failed to execute psql: {}", stderr);
        return Ok(());
    }

    let mut postgres_client = Client::connect(postgres_url.as_str(), NoTls)?;

    const PROBLEM_QUERY: &str = "SELECT ssn FROM public.criminal_records WHERE status=\'alive\';";
    const RESET: &str = "DROP TABLE public.criminal_records;";

    let rows = postgres_client.query(PROBLEM_QUERY, &[])?;
    let _ = postgres_client.execute(RESET, &[])?;

    let solution = serde_json::json!({
        "alive_ssns": rows.iter().map(|row| row.get::<usize, &str>(0)).collect::<Vec<&str>>()
    }); 

    println!("Solution: {:?}", solution);

    let response = json_post(&solution_url, &solution)?;

    println!("Response: {:?}", response);   
    
    Ok(())
}

fn get_env(name: &str) -> String {
    use std::env;
    env::var(name).expect(&format!("{} is not set", name))
}

fn json_get(url: &str) -> Result<serde_json::Value, Box<dyn Error>> {
    use reqwest::blocking::Client;

    let http_client = Client::new();
    let response_text = http_client.get(url).send()?.text()?;
    let response = serde_json::from_str(&response_text)?;
    Ok(response)
}

fn json_post(url: &str, body: &serde_json::Value) -> Result<serde_json::Value, Box<dyn Error>> {
    use reqwest::blocking::Client;
    use reqwest::header::CONTENT_TYPE;

    let http_client = Client::new();
    let response = http_client.post(url)
        .header(CONTENT_TYPE, "application/json")
        .json(body)
        .send()?;
    let response_text = response.text()?;
    let response = serde_json::from_str(&response_text)?;
    Ok(response)
}   

fn base64_decode(encoded: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    use base64::engine::general_purpose;
    use base64::Engine; 

    let decoded = general_purpose::STANDARD.decode(encoded)?;
    Ok(decoded)
}

fn gzip_decompress(compressed: Vec<u8>) -> Result<Vec<u8>, Box<dyn Error>> {
    use flate2::read::GzDecoder;
    use std::io::Cursor;
    use std::io::Read;

    let mut decoder = GzDecoder::new(Cursor::new(compressed));
    let mut decompressed: Vec<u8> = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}
