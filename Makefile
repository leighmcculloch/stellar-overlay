.PHONY: build test test-integration install doc

build:
	cargo build

test:
	cargo test

test-integration:
	cargo test -p stellar-txsub-cli --test integration -- --ignored

install:
	cargo install --locked --force --path stellar-txsub-cli

doc:
	cargo doc --open
