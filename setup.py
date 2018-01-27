from setuptools import setup

def readme():
    with open('README.rst') as f:
        return f.read()
        
setup(name='workflow',
      version='0.1',
      description='tools for using queue system for parallel workflow',
      url='https://github.com/NCAR/workflow',
      author='Matthew Long',
      author_email='mclong@ucar.edu',
      license='MIT',
      packages=['workflow'],
      install_requires=['xarray','numpy'],
      zip_safe=False)
