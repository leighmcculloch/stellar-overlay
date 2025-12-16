.PHONY: build test test-integration install doc readme

build:
	cargo build

test:
	cargo test

test-integration:
	cargo test -p stellar-txsub-cli --test integration -- --ignored

install:
	cargo install --locked --force --path stellar-txsub-cli

doc:
	cargo doc --no-deps --open

readme:
	cd stellar-overlay \
		&& cargo +nightly rustdoc -- -Zunstable-options -wjson \
		&& cat ../target/doc/stellar_overlay.json \
		| jq -r '.index[.root|tostring].docs' \
		> README.md
