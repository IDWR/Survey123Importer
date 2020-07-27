import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="Survey123Client",
    version="1.1.0",
    author="Dan Narsavage",
    author_email="Dan.Narsavage@idwr.idaho.gov",
    description="Python API for interacting with Esri Survey123",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
	install_requires=[
          'arcgis>=1.6.1',
	],
	python_requires='>=3.6',
)
