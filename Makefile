venv:
		. .venv/bin/activate

lint:
		ruff check . 

fmt:
		ruff format .

test:
		pytest

cargo-test:
	cd src/pgn-parser && cargo test

cargo-lint:
	cd src/pgn-parser && cargo clippy

cargo-fmt:
	cd src/pgn-parser && cargo fmt

cargo-build:
	cd src/pgn-parser && maturin develop --release 

cargo:
	cd src/pgn-parser && cargo fmt
	cd src/pgn-parser && cargo clippy
	cd src/pgn-parser && cargo test --release
	cd src/pgn-parser && maturin develop --release

all: lint fmt 
