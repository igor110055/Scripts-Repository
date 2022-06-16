from setuptools import find_namespace_packages, setup

packages = [package for package in find_namespace_packages(include=['emr_ingestion', 'emr_ingestion.*'])]

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='emr_pipe_ingestion',
    version='1.1.4',
    author='Ernani Britto',
    author_email='ernani.murtinho@zooxsmart.com',
    description='PIPE-EMR-INGESTION: Pipe de ingestao dos arquivos raw para o datalake',
    long_description=long_description,
    python_requires='>=3.7',
    platforms='MacOS X; Linux',
    packages=packages,
    install_requires=(
        'pex',
    ),
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.7',
    ],
)

