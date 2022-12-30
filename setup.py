import unittest

from setuptools import setup, find_packages


def testsuite():
    return unittest.TestLoader().discover('tests', pattern='test_*.py')


setup(
    name='adbc',
    version='0.0.1',
    packages=[_ for _ in find_packages() if _ != "tests"],
    url='https://github.com/Platob/pyADBC.git',
    license='Apache',
    author='Platob',
    python_requires=">= 3.9",
    author_email='nfillot.pro@gmail.com',
    install_requires=open('requirements.txt').read().splitlines(),
    description='Python based Arrow Data Base Connector',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache 2.0 License',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: Text Processing :: Linguistic',
    ],
    test_suite='setup.testsuite'
)
