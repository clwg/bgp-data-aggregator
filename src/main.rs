use bgpkit_parser::BgpkitParser;
use dotenv::dotenv;
use postgres::{Client, NoTls};
use std::collections::HashMap;
use std::env;
use uuid::Uuid;

fn main() {
    dotenv().ok();

    let args: Vec<String> = env::args().collect();
    let url = args.get(1).expect("Missing URL argument");

    let mut record_map: HashMap<Uuid, (String, String, String, String, String, f64, f64, u32)> = HashMap::new();
    let namespace_uuid = Uuid::parse_str("40689d13-36ac-4216-9c41-f02b007d46c2").unwrap();

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
    .expect("Failed to connect to the database, ensure your .env file is correct");

    client
        .execute(
            &format!("SET search_path TO {}", env::var("PGSCHEMA").unwrap()),
            &[],
        )
        .unwrap();

    match client.execute(
        "CREATE TABLE IF NOT EXISTS bgp_data (
            uuid UUID PRIMARY KEY,
            elem_type VARCHAR(255),
            prefix INET,
            as_path VARCHAR(255),
            next_hop VARCHAR(255),
            peer_ip VARCHAR(255),
            min_timestamp FLOAT,
            max_timestamp FLOAT,
            count INTEGER DEFAULT 0
        );",
        &[],
    ) {
        Ok(_) => {}
        Err(err) => {
            eprintln!("Error creating table: {}", err);
        }
    }

    for elem in BgpkitParser::new(url).unwrap() {
        let elem_type = format!("{:?}", elem.elem_type);
        let as_path = elem.as_path.as_ref().map(|p| p.to_string()).unwrap_or_default();
        let next_hop = elem.next_hop.map_or_else(|| String::new(), |ip| ip.to_string());
        let peer_ip = elem.peer_ip.to_string();
        let key = format!(
            "{}|{}|{}|{}|{}",
            elem_type, elem.prefix, as_path, next_hop, peer_ip
        );
        let uuid5 = Uuid::new_v5(&namespace_uuid, key.as_bytes());

        let (_elem_type, _prefix, _as_path, _next_hop, _peer_ip, min_timestamp, max_timestamp, count) = 
            record_map.entry(uuid5).or_insert((
                elem_type.clone(), 
                elem.prefix.to_string(), 
                as_path.clone(), 
                next_hop.clone(), 
                peer_ip.clone(), 
                elem.timestamp, 
                elem.timestamp, 
                0,
            ));

        *min_timestamp = f64::min(*min_timestamp, elem.timestamp);
        *max_timestamp = f64::max(*max_timestamp, elem.timestamp);
        *count += 1;
    }

    for (uuid, (elem_type, prefix, as_path, next_hop, peer_ip, min_timestamp, max_timestamp, count)) in record_map {
        match client.execute(
            "INSERT INTO bgp_data (
                uuid, elem_type, prefix, as_path, next_hop, peer_ip,
                min_timestamp, max_timestamp, count
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (uuid) DO UPDATE 
            SET count = bgp_data.count + 1,
                min_timestamp = LEAST(bgp_data.min_timestamp, EXCLUDED.min_timestamp),
                max_timestamp = GREATEST(bgp_data.max_timestamp, EXCLUDED.max_timestamp)",
            &[
                &uuid,
                &elem_type,
                &prefix,
                &as_path,
                &next_hop,
                &peer_ip,
                &(min_timestamp as f64),
                &(max_timestamp as f64),
                &(count as i32),
            ],
        ) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("Error inserting record: {}", err);
            }
        }
    }
}
