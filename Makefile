venv:
		. .venv/bin/activate

lint:
		ruff check . 

fmt:
		ruff format .

test:
		pytest

all: lint fmt 
