from setuptools import setup

from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='pgnotifier',
    version='0.0.10',
    install_requires=[
	'pyrsistent>=0.20.0',
	'psycopg>=3.2.1',
    ],
	include_package_data=True,
	long_description=long_description,
    long_description_content_type='text/markdown',
)


