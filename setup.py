from setuptools import setup

setup(
    name='pgnotifier',
    version='0.0.6',
    install_requires=[
	'pyrsistent>=0.20.0',
	'psycopg>=3.2.1',
    ],
)
