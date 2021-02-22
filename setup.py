from setuptools import find_packages
from setuptools import setup

import versioneer

setup(
    name="pytask-parallel",
    version=versioneer.get_version(),
    cmd_class=versioneer.get_cmdclass(),
    description="Parallelize the execution of pytask.",
    author="Tobias Raabe",
    author_email="raabe@posteo.de",
    python_requires=">=3.6",
    license="MIT",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    platforms="any",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    entry_points={"pytask": ["pytask_parallel = pytask_parallel.plugin"]},
    zip_safe=False,
)
