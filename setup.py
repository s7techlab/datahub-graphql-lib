from setuptools import find_packages, setup


def readme():
    with open("README.md", "r", encoding="utf-8") as show_the_readme:
        return show_the_readme.read()


setup(
    name="datahub_edp_lib",
    version="0.0.1",
    author="s.bubnov",
    author_email="s.bubnov@s7.ru",
    description="datahub_edp_lib",
    long_description=readme(),
    long_description_content_type="text/markdown",
    # url='home_link',
    # packages=["datahub_edp_lib"],
    packages=find_packages(),
    install_requires=[
        "aiohttp==3.8.3",
        "aiosignal==1.2.0",
        "async-timeout==4.0.2",
        "attrs==22.1.0",
        "backoff==2.1.2",
        "botocore==1.27.84",
        "certifi==2022.9.24",
        "charset-normalizer==2.1.1",
        "frozenlist==1.3.1",
        "gql==3.4.0",
        "graphql-core==3.2.3",
        "idna==3.4",
        "jmespath==1.0.1",
        "multidict==6.0.2",
        "python-dateutil==2.8.2",
        "requests==2.28.1",
        "requests-toolbelt==0.9.1",
        "six==1.16.0",
        "urllib3==1.26.12",
        "websockets==10.3",
        "yarl==1.8.1",
    ],
    classifiers=[
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    keywords="datahub_edp_lib",
    # project_urls={
    #   'Documentation': 'link'
    # },
    python_requires=">=3.7",
)
