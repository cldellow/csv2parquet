setup:
	pip install pipenv
	pipenv install --dev --three

test:
	pipenv run -- pylint
	pipenv run -- pytest
