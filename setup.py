import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="csv2parquet",
    version="0.0.4post4",
    author="Colin Dellow",
    author_email="cldellow@cldellow.com",
    description="A tool to convert CSVs to Parquet files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cldellow/csv2parquet",
    packages=setuptools.find_packages(),
    entry_points = {
        "console_scripts": ['csv2parquet = csv2parquet.csv2parquet:main']
    },
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ),
)

