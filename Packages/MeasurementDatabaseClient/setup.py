import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="MeasurementDatabaseClient",
    version="1.1.4",
    author="Dan Narsavage",
    author_email="Dan.Narsavage@idwr.idaho.gov",
    description="Python API for interacting with the MeasurementDatabase database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    install_requires=[
        'pyodbc>=4.0.27',
    ],
    python_requires='>=3.6',
)
