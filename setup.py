# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name='qianka-cache',
    version='1.0.0',

    packages=find_packages(),

    install_requires=[
        'msgpack-python',
        'redis',
        'Werkzeug'
    ],
    setup_requires=[],
    tests_require=[],

    author="Qianka Inc.",
    description="",
    url='http://github.com/qianka/qianka-cache'
)
