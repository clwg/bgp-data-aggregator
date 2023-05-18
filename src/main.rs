// Import necessary libraries and modules
use bgpkit_parser::BgpkitParser;
use serde_json::json;
use std::collections::HashMap;
use std::env;
use uuid::Uuid;

fn main() {
    // Collect command line arguments
    let args: Vec<String> = env::args().collect();
    // Get the URL from command line arguments
    let url = args.get(1).expect("Missing URL argument");

    // Create a HashMap to store records and their associated data
    let mut record_map: HashMap<String, (f64, f64, u32, Uuid)> = HashMap::new();

    let namespace_uuid = Uuid::parse_str("40689d13-36ac-4216-9c41-f02b007d46c2").unwrap(); 
    
    // Namespace UUID for the UUID v5 generation
    //let namespace_uuid = Uuid::NAMESPACE_DNS;

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

        // Print the JSON object
        println!(
            "{}",
            json!({
                "elem_type": tokens[0],
                "prefix": tokens[1],
                "as_path": tokens[2],
                "next_hop": tokens[3],
                "peer_ip": tokens[4],
                "min_timestamp": min_timestamp,
                "max_timestamp": max_timestamp,
                "count": count,
                "uuid": uuid.to_string()
            })
            .to_string()
        );
    }
}
