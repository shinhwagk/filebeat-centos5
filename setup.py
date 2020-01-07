from setuptools import setup

with open("VERSION")as f:
    version = f.read()

setup(
    name="filebeat_oracle",
    version=version,
    author='shinhwagk',
    author_email='shinhwagk@outlook.com',
)
