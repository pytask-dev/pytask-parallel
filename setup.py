from pathlib import Path

from setuptools import find_packages
from setuptools import setup

import versioneer


README = Path("README.rst").read_text()

PROJECT_URLS = {
    "Documentation": "https://github.com/pytask-dev/pytask-parallel",
    "Github": "https://github.com/pytask-dev/pytask-parallel",
    "Tracker": "https://github.com/pytask-dev/pytask-parallel/issues",
    "Changelog": "https://github.com/pytask-dev/pytask-parallel/blob/main/CHANGES.rst",
}


setup(
    name="pytask-parallel",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Parallelize the execution of pytask.",
    long_description=README,
    long_description_content_type="text/x-rst",
    author="Tobias Raabe",
    author_email="raabe@posteo.de",
    python_requires=">=3.6",
    url=PROJECT_URLS["Github"],
    project_urls=PROJECT_URLS,
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    install_requires=[
        "click",
        "cloudpickle",
        "loky",
        "pytask >= 0.0.11",
    ],
    platforms="any",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    entry_points={"pytask": ["pytask_parallel = pytask_parallel.plugin"]},
    include_package_data=True,
    zip_safe=False,
)
