from setuptools import setup

setup(
    name='sqllogictest',
    version='0.1',
    python_requires='>=3.6',
    package_dir={'': 'src'},
    entry_points={
        'console_scripts': [
            'sqllogictetst = sqllogictest:main',
            'multi_runner = multi_runner:main',
            'inserter = inserter:main',
        ]
    },
    install_requires=[
        'psycopg2==2.7.1',
        'cr8==0.10.0'
    ],
)
