import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="chainspotter",
    version="0.0.2",
    author="Alexander Dmitriev",
    author_email="sasha.engineer@gmail.com",
    description="Redis Streams wrapper for user-item interaction tracking and fetching",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/uSasha/chainspotter",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
)
