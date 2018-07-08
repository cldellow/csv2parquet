setup:
	pip install pipenv
	pipenv install --dev --three

test:
	pipenv run -- pylint csv2parquet
	pipenv run -- pytest
