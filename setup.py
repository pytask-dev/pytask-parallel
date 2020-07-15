from setuptools import find_packages
from setuptools import setup

setup(
    name="pytask-parallel",
    version="0.0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    entry_points={"pytask": ["pytask_parallel = pytask_parallel.plugin"]},
)
