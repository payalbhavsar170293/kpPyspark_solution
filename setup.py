from setuptools import setup, find_packages

setup(
    name='kpPysparkSolution_app',
    version='1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'pyspark',
        'pytest',
        'chispa',
    ],
    entry_points={
        'console_scripts': [
            'kpPysparkSolution_app=kpPysparkSolution_app.main:main'
        ]
    }
)
