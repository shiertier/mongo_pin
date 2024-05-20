from setuptools import setup, find_packages

setup(
    name='pinmongo',
    version='0.1.0',
    description='A Python library for interacting with MongoDB',
    author='shiertier',
    url='https://github.com/shiertier/pinmongo',
    packages=find_packages(),
    package_data={
        'pinmongo': ['mongo_dovaban497_2'],
    },
    install_requires=[
        'pymongo',
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
)