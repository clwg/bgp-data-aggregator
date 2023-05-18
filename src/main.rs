// Import necessary libraries and modules
use bgpkit_parser::BgpkitParser;
use dotenv::dotenv;
use postgres::{Client, NoTls};
use std::collections::HashMap;
use std::env;
use uuid::Uuid;

fn main() {
    dotenv().ok();

    // Collect command line arguments
    let args: Vec<String> = env::args().collect();
    // Get the URL from command line arguments
    let url = args.get(1).expect("Missing URL argument");

    // Create a HashMap to store records and their associated data
    let mut record_map: HashMap<String, (f64, f64, u32, Uuid)> = HashMap::new();

    let namespace_uuid = Uuid::parse_str("40689d13-36ac-4216-9c41-f02b007d46c2").unwrap();

    // Connect to PostgreSQL database
    let mut client = Client::connect(
        &format!(
            "host={} user={} password={} dbname={}",
            env::var("PGHOST").unwrap(),
            env::var("PGUSER").unwrap(),
            env::var("PGPASSWORD").unwrap(),
            env::var("PGDATABASE").unwrap(),
        ),
        NoTls,
    )
    .unwrap();

    client
    .execute(
        &format!("SET search_path TO {}", env::var("PGSCHEMA").unwrap()),
        &[],
    )
    .unwrap();

    // Create table if not exists
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS log_table (
                elem_type VARCHAR(255),
                prefix VARCHAR(255),
                as_path VARCHAR(255),
                next_hop VARCHAR(255),
                peer_ip VARCHAR(255),
                min_timestamp FLOAT,
                max_timestamp FLOAT,
                count INTEGER,
                uuid UUID
            );",
            &[],
        )
        .unwrap();

    // Iterate through elements returned by BgpkitParser
    for elem in BgpkitParser::new(url).unwrap() {
        // Convert elem_type, as_path, next_hop, and peer_ip to strings
        let elem_type = format!("{:?}", elem.elem_type);
        let as_path = elem.as_path.as_ref().map(|p| p.to_string()).unwrap_or_default();
        let next_hop = elem.next_hop.map_or_else(|| String::new(), |ip| ip.to_string());
        let peer_ip = elem.peer_ip.to_string();

        // Construct a key from the string representations
        let key = format!(
            "{}|{}|{}|{}|{}",
            elem_type, elem.prefix, as_path, next_hop, peer_ip
        );

        // Generate the UUID v5 for the key
        let uuid5 = Uuid::new_v5(&namespace_uuid, key.as_bytes());

        // Update or insert the min_timestamp, max_timestamp, count, and UUID v5 for the key in the record_map
        let (min_timestamp, max_timestamp, count, _uuid) = record_map.entry(key.clone()).or_insert((
            elem.timestamp,
            elem.timestamp,
            0,
            uuid5,
        ));

        // Update min_timestamp and max_timestamp
        *min_timestamp = f64::min(*min_timestamp, elem.timestamp);
        *max_timestamp = f64::max(*max_timestamp, elem.timestamp);
        // Increment count
        *count += 1;
    }

    // Iterate through the record_map and output the final JSON objects
    for (key, (min_timestamp, max_timestamp, count, uuid)) in record_map {
        // Split the key into tokens for constructing the JSON object
        let tokens: Vec<&str> = key.split('|').collect();
    
        // Insert into Postgres
        match client.execute(
            "INSERT INTO log_table (
                elem_type, prefix, as_path, next_hop, peer_ip,
                min_timestamp, max_timestamp, count, uuid
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            &[
                &tokens[0],
                &tokens[1],
                &tokens[2],
                &tokens[3],
                &tokens[4],
                &(min_timestamp as f64),
                &(max_timestamp as f64),
                &(count as i32),
                &uuid,
            ],
        ) {
            Ok(_) => {
                // Insertion successful
                // Handle success if needed
            }
            Err(err) => {
                // Error occurred during insertion
                // Handle the error in an appropriate way
                eprintln!("Error inserting record: {}", err);
                // You can choose to log the error, retry the operation, or take other actions based on your requirements.
            }
        }
    }
}
