# BGP Data Aggregator

Analyzes BGP rib files and aggregates the data into a sqlite database, jsonl or csv files.

## Building

`cargo build --release`

## Usage

First find the BGP data you want to analyze here http://archive.routeviews.org/bgpdata/

Full RIB processing can be intensive, it is recommended that you have atleast 16GB of available RAM if you want to process the full RIB.

Example usage:

`./target/release/bgp_data_aggregator http://archive.routeviews.org/bgpdata/2024.08/UPDATES/updates.20240801.0030.bz2`

The output format can be specified with a flag, the default is to a raw text file, but --json, --csv and --sqlite are also available.

`./target/release/bgp_data_aggregator http://archive.routeviews.org/bgpdata/2024.08/UPDATES/updates.20240801.0030.bz2 --json`

`./target/release/bgp_data_aggregator http://archive.routeviews.org/bgpdata/2024.08/UPDATES/updates.20240801.0030.bz2 --csv`

`./target/release/bgp_data_aggregator http://archive.routeviews.org/bgpdata/2024.08/UPDATES/updates.20240801.0030.bz2 --sqlite`
