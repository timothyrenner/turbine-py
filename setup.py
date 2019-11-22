from setuptools import setup, find_packages

setup(
    name="turbine",
    version="0.1.alpha1",
    packages=find_packages(exclude=["tests", "scripts"]),
    install_requires=[],
    author="Tim Renner",
)
