# sap-etl
A structured Rust ETL pipeline that replays Solana data with plugin data sources, maps the observed ledger stream into a Solana ontology, and loads the result either as sharded Turtle (`.ttl`) files or into QLever through SPARQL Update.
