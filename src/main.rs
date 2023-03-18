// Import necessary libraries and modules
use bgpkit_parser::BgpkitParser;
use serde_json::json;
use std::collections::HashMap;
use std::env;

fn main() {
    // Collect command line arguments
    let args: Vec<String> = env::args().collect();
    // Get the URL from command line arguments
    let url = args.get(1).expect("Missing URL argument");

    // Create a HashMap to store records and their associated data
    let mut record_map: HashMap<String, (f64, f64, u32)> = HashMap::new();

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

        // Update or insert the min_timestamp, max_timestamp, and count for the key in the record_map
        let (min_timestamp, max_timestamp, count) = record_map.entry(key.clone()).or_insert((
            elem.timestamp,
            elem.timestamp,
            0,
        ));

        // Update min_timestamp and max_timestamp
        *min_timestamp = f64::min(*min_timestamp, elem.timestamp);
        *max_timestamp = f64::max(*max_timestamp, elem.timestamp);
        // Increment count
        *count += 1;
    }

    // Iterate through the record_map and output the final JSON objects
    for (key, (min_timestamp, max_timestamp, count)) in record_map {
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
                "count": count
            })
            .to_string()
        );
    }
}

