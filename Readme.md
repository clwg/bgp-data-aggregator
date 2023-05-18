# BGP Data Aggregator

This Rust application takes a URL as an argument and processes Border Gateway Protocol (BGP) rib file (Routing Information Base), storing the result in a PostgreSQL database. The application utilizes bgpkit_parser to parse BGP data and postgres for database interaction. It uniquely identifies BGP data records based on a combination of data attributes and updates the records based on minimum and maximum timestamps.

## Requirements
To build and run this project, you will need:

- Rust
- PostgreSQL
- An Internet connection (for fetching BGP data)

## Setup
1. Clone the repository - Clone this repository to your local machine.
2. Environment Variables - Create a .env file in the root of the project, and provide the following environment variables, required for connecting to your PostgreSQL database:
```
PGHOST=<your-host>
PGUSER=<your-username>
PGPASSWORD=<your-password>
PGDATABASE=<your-database>
PGSCHEMA=<your-schema>
```
3. Build the project
```
cargo build --release     
```

## Running the Application
To run the application, provide the URL of the BGP data as a command line argument. Here's an example:
```
./target/release/bgp_data_aggregator "http://archive.routeviews.org/bgpdata/2023.01/UPDATES/updates.20230101.1430.bz2"
```
