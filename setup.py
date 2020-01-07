from setuptools import setup, find_packages

__version__ = None
exec(open("version.py").read())

setup(
    name="filebeat-oracle",
    version=__version__,
    py_modules=['filebeat_oracle', 'version'],
    author='shinhwagk',
    author_email='shinhwagk@outlook.com',
    license="MIT"
)
