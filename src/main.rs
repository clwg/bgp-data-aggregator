use bgpkit_parser::BgpkitParser;
use dotenv::dotenv;
use ipnetwork::IpNetwork;
use rusqlite::{params, Connection};
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{Write, BufWriter};
use uuid::Uuid;
use std::net::IpAddr;
use num_bigint::BigUint;
use csv::Writer;

fn ip_to_int(ip: IpAddr) -> BigUint {
    match ip {
        IpAddr::V4(addr) => BigUint::from(u32::from(addr)),
        IpAddr::V6(addr) => BigUint::from_bytes_be(&addr.octets()),
    }
}

fn main() {
    dotenv().ok();

    let args: Vec<String> = env::args().collect();
    let url = args.get(1).expect("Missing URL argument");

    // Command-line switches
    let use_jsonl = args.contains(&"--jsonl".to_string());
    let use_csv = args.contains(&"--csv".to_string());
    let use_sqlite = args.contains(&"--sqlite".to_string());

    // File output setup
    let extension = if use_jsonl {
        "jsonl"
    } else if use_csv {
        "csv"
    } else {
        "txt"
    };
    let filename = format!("{}_output.{}", url.split('/').last().unwrap_or("output"), extension);
    
    // Initialize output file or CSV writer
    let mut file = if !use_sqlite && !use_csv {
        Some(BufWriter::new(File::create(&filename).expect("Unable to create file")))
    } else {
        None
    };

    let mut csv_writer = if use_csv {
        let wtr = Writer::from_path(&filename).expect("Unable to create CSV file");
        Some(wtr)
    } else {
        None
    };

    // SQLite setup
    let conn = if use_sqlite {
        let db_filename = String::from("bgp_db.sqlite");
        let conn = Connection::open(db_filename).expect("Unable to open SQLite database");
        conn.execute("PRAGMA synchronous = OFF", []).unwrap();

        // Create table with asn column
        conn.execute(
            "CREATE TABLE IF NOT EXISTS bgp_data (
                uuid TEXT PRIMARY KEY,
                elem_type TEXT,
                prefix TEXT,
                start_ip BIGINT,
                end_ip BIGINT,
                as_path TEXT,
                asn TEXT,
                next_hop TEXT,
                peer_ip TEXT,
                min_timestamp INTEGER,
                max_timestamp INTEGER,
                count INTEGER DEFAULT 0
            );",
            [],
        )
        .expect("Failed to create table");

        Some(conn)
    } else {
        None
    };

    // Processing data
    let mut record_map: HashMap<Uuid, (String, String, BigUint, BigUint, String, String, String, String, f64, f64, u32)> = HashMap::new();
    let namespace_uuid = Uuid::parse_str("40689d13-36ac-4216-9c41-f02b007d46c2").unwrap();

    for elem in BgpkitParser::new(url).unwrap() {
        let elem_type = format!("{:?}", elem.elem_type);
        let as_path = elem.as_path.as_ref().map(|p| p.to_string()).unwrap_or_default();
        let asn = as_path.split_whitespace().last().unwrap_or("").to_string();
        let next_hop = elem.next_hop.map_or_else(|| String::new(), |ip| ip.to_string());
        let peer_ip = elem.peer_ip.to_string();

        // Parse the prefix to get the start and end IPs
        let prefix = elem.prefix.to_string();
        let ip_network: IpNetwork = prefix.parse().expect("Invalid IP network format");
        let start_ip = ip_network.network();
        let end_ip = ip_network.broadcast();

        // Convert start_ip and end_ip to their integer representations
        let start_ip_int = ip_to_int(start_ip);
        let end_ip_int = ip_to_int(end_ip);

        let key = format!(
            "{}|{}|{}|{}|{}",
            elem_type, prefix, as_path, next_hop, peer_ip
        );
        let uuid5 = Uuid::new_v5(&namespace_uuid, key.as_bytes());

        let (_elem_type, _prefix, _start_ip, _end_ip, _as_path, _asn, _next_hop, _peer_ip, min_timestamp, max_timestamp, count) = 
            record_map.entry(uuid5).or_insert((
                elem_type.clone(), 
                prefix.clone(), 
                start_ip_int.clone(), 
                end_ip_int.clone(), 
                as_path.clone(), 
                asn.clone(),
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

    // CSV headers
    if let Some(ref mut writer) = csv_writer {
        writer.write_record(&["uuid", "element_type", "prefix", "start_ip", "end_ip", "as_path", "asn", "next_hop", "peer_ip", "min_timestamp", "max_timestamp", "count"])
            .expect("Failed to write CSV headers");
    }

    // Output results
    for (uuid, (elem_type, prefix, start_ip_int, end_ip_int, as_path, asn, next_hop, peer_ip, min_timestamp, max_timestamp, count)) in record_map {
        if let Some(ref conn) = conn {
            conn.execute(
                "INSERT INTO bgp_data (uuid, elem_type, prefix, start_ip, end_ip, as_path, asn, next_hop, peer_ip, min_timestamp, max_timestamp, count)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                ON CONFLICT(uuid) DO UPDATE SET
                count = count + 1,
                min_timestamp = MIN(min_timestamp, excluded.min_timestamp),
                max_timestamp = MAX(max_timestamp, excluded.max_timestamp)",
                params![
                    uuid.to_string(),
                    elem_type,
                    prefix,
                    start_ip_int.to_string(),
                    end_ip_int.to_string(),
                    as_path,
                    asn,
                    next_hop,
                    peer_ip,
                    min_timestamp,
                    max_timestamp,
                    count,
                ],
            )
            .expect("Error inserting record into SQLite");
        } else if use_jsonl {
            let record = json!({
                "uuid": uuid.to_string(),
                "elem_type": elem_type,
                "prefix": prefix,
                "start_ip": start_ip_int.to_string(),
                "end_ip": end_ip_int.to_string(),
                "as_path": as_path,
                "asn": asn,
                "next_hop": next_hop,
                "peer_ip": peer_ip,
                "min_timestamp": min_timestamp,
                "max_timestamp": max_timestamp,
                "count": count
            });
            writeln!(file.as_mut().unwrap(), "{}", record.to_string()).expect("Unable to write to file");
        } else if use_csv {
            csv_writer.as_mut().unwrap().write_record(&[
                uuid.to_string(),
                elem_type,
                prefix,
                start_ip_int.to_string(),
                end_ip_int.to_string(),
                as_path,
                asn,
                next_hop,
                peer_ip,
                min_timestamp.to_string(),
                max_timestamp.to_string(),
                count.to_string(),
            ]).expect("Unable to write to CSV");
        } else {
            // Write plain text format with asn
            writeln!(
                file.as_mut().unwrap(),
                "{}\t|\t{}\t|\t{}\t|\t{}\t|\t{}\t|\t{}\t|\t{}\t|\t{}\t|\t{}\t|\t{}\t|\t{}\t|\t{}",
                uuid, elem_type, prefix, start_ip_int.to_string(), end_ip_int.to_string(), as_path, asn, next_hop, peer_ip, min_timestamp, max_timestamp, count
            ).expect("Unable to write to file");
        }
    }

    // Finalize CSV
    if let Some(ref mut writer) = csv_writer {
        writer.flush().expect("Failed to finalize CSV file");
    }
}