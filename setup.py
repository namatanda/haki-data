from setuptools import setup, find_packages

setup(
    name='haki-data',
    version='0.1.0',  
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'haki_data=haki_data.main:main'  
        ]
    }
)