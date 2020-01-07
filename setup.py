from setuptools import setup, find_packages

with open("VERSION")as f:
    version = f.read()

setup(
    name="filebeat-oracle",
    version=version,
    packages=find_packages(exclude=["test"]),
    author='shinhwagk',
    author_email='shinhwagk@outlook.com',
    license="MIT"
)
