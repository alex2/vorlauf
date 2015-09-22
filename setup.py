import os
from setuptools import setup, find_packages

version = '0.0.0'


def fread(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='vorlauf',
    version=version,
    description='Lightweight tool for piping subprocess processes to each other',
    long_description=fread('README.rst') + '\n\n' + fread('CHANGES.rst'),
    packages=find_packages(),
    keywords='vorlauf subprocess pipe',
    author='Alex Holmes',
    author_email='alex@alex-holmes.com',
    license='Apache Software License',
    install_requires=[
        'unittest2',
        'mock',
    ],
    zip_safe=False,
    include_package_data=True,
)
