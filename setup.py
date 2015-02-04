from setuptools import setup

setup(name='divideAndSlurm',
	version='0.1',
	description='Map-reduce style submission of jobs to a Slurm cluster.',
	url='http://github.com/afrendeiro/divideAndSlurm',
	author='Andre Rendeiro',
	author_email='arendeiro@cemm.oeaw.ac.at',
	long_description=open('README.md').read(),
	license='GPL3',
	packages=['divideAndSlurm'],
	install_requires=[],
	classifiers=['Development Status :: 1 - Alpha',
                 'Intended Audience :: Developers',
                 'License :: OSI Approved :: GPL3',
                 'Operating System :: POSIX :: Linux',
                 'Programming Language :: Python :: 2.7'],
	zip_safe=False
)
