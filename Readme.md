# BGP Data Aggregator

This Rust application processes BGP data files from a given URL, aggregates the records, and outputs the processed data in JSON format. It groups records by their attributes, calculates the minimum and maximum timestamps, and counts the occurrences of each unique record.

## Requirements
- Rust programming language (https://www.rust-lang.org/tools/install)

## Dependencies
- bgpkit-parser: Parses BGP data from various sources
- serde_json: Used for JSON manipulation and output

## Usage
- Clone the repository or copy the source code into a new Rust project.
- Build the project using ```cargo build --release```.
- Run the application with the URL of the BGP data file as an argument:

```
./target/release/bgp_data_aggregator "http://archive.routeviews.org/bgpdata/2023.01/UPDATES/updates.20230101.1430.bz2"
```

## Output
The application outputs the aggregated BGP data in JSON format with the following fields:

elem_type: The type of the BGP record (e.g., "announce" or "withdraw")
prefix: The IP prefix associated with the record
as_path: The Autonomous System (AS) path of the record
next_hop: The next hop IP address
peer_ip: The peer IP address
min_timestamp: The minimum timestamp of the aggregated records
max_timestamp: The maximum timestamp of the aggregated records
count: The number of occurrences of the unique record

```
{
  "elem_type": "announce",
  "prefix": "197.186.0.0/15",
  "as_path": "7018 6939 37662 327708 37133",
  "next_hop": "12.0.1.63",
  "peer_ip": "12.0.1.63",
  "min_timestamp": 1633047113.088827,
  "max_timestamp": 1633047113.088827,
  "count": 2
}
```

