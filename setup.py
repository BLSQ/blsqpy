import setuptools

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='blsqpy',
    version='0.1',
    scripts=[],
    author="BLSQ",
    author_email="tech@bluesquarehub.com",
    description="A pandas dhis2 helper script",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/BLSQ/blsq-py",
    packages=find_packages(exclude=('tests', 'docs')),
    package_data={
      'queries': ['blsqpy/queries/*.sql'],
   },
    include_package_data=True,
    install_requires=[
       "scipy", "pandas", "boto3", "python-dotenv", "psycopg2-binary", "sqlalchemy", "jinja2", "requests","geopandas","shapely== 1.6.4.post2", "matplotlib==2.2.4","descartes"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)