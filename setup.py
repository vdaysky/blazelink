import setuptools

setuptools.setup(
    name="blazelink",
    packages=setuptools.find_packages(include=["blazelink"]),
    version="0.1.0",
    author="Volodymyr Odaysky",
    description="",
    author_email="volodymyr@odays.ky",
    # dependencies
    install_requires=[
        "ariadne"
    ]
)
