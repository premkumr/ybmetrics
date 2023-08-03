from setuptools import setup 
setup( 
    name='ybmetrics', 
    version='0.5.5',
    author='Prem',
    author_email='contactprem@gmail.com',
    license='MIT',
    packages=['ybmetrics'],
    python_requires=">=3.0",
    install_requires = [
        'requests >=2.0',
        'tabulate >=0.8',
    ],
    entry_points={
        "console_scripts": ["ybmetrics=ybmetrics.metrics:cli"]
    },
)
