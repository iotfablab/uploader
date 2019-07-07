from setuptools import setup

setup(name='uploader',
    version=1.0,
    description='Migrate values from local InfluxDB instance to InfluxDB Server/Cloud',
    url='https://gitlab.ips.biba.uni-bremen.de/usg/umg-uploader',
    author='Shantanoo Desai',
    author_email='des@biba.uni-bremen.de',
    license='MIT',
    packages=['uploader'],
    scripts=['bin/uploader'],
    install_requires=[
        'influxdb'
    ],
    include_data_package=True,
    zip_safe=False
)