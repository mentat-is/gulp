from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext
import subprocess
import os

def get_version():
    """
    get the version from environment variable.
    
    Returns:
        str: the version string from environment
        
    Raises:
        ValueError: if VERSION environment variable is not set
    """
    version = os.getenv("VERSION")
    if not version:
        raise ValueError("Environment variable VERSION is not set.")
    return version


def get_requirements():
    """
    parse requirements from requirements.txt file.
    
    Returns:
        list[str]: list of required packages
    """
    with open("requirements.txt") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]

class MakefileExtension(Extension):
    """
    custom extension class that uses makefile for building.
    
    Args:
        name (str): name of the extension
        makefile_dir (str): directory containing the makefile
    """
    def __init__(self, name: str, makefile_dir: str = None):
        super().__init__(name, sources=[])
        self.makefile_dir = makefile_dir or '.'


class MakefileBuild(build_ext):
    """
    custom build_ext command that uses make instead of distutils.
    
    this class replaces the default build_ext command to use our makefile
    for building the c extension.
    """
    
    def build_extension(self, ext: MakefileExtension) -> None:
        """
        build the given extension using make.
        
        Args:
            ext: extension to build
            
        Raises:
            RuntimeError: if make build fails
        """
        if not isinstance(ext, MakefileExtension):
            # use the default implementation for other extensions
            super().build_extension(ext)
            return
            
        # get the build directory
        build_dir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))
        
        # get the makefile directory
        makefile_dir = ext.makefile_dir
        
        # change to the makefile directory
        cwd = os.getcwd()
        os.chdir(makefile_dir)
        
        try:
            # run make to build the extension
            subprocess.check_call(['make', f'BUILD_DIR={build_dir}'])
            
            # ensure the destination directory exists
            os.makedirs(os.path.dirname(self.get_ext_fullpath(ext.name)), exist_ok=True)
            
            # copy the built extension to the right location
            dist_dir = os.path.dirname(self.get_ext_fullpath(ext.name))
            subprocess.check_call(['make', 'install', f'DIST_DIR={dist_dir}'])
        finally:
            # restore original working directory
            os.chdir(cwd)


setup(
    name="gulp",
    install_requires=get_requirements(),
    ext_modules=[
        MakefileExtension(
            'gulp.libgulp', 
            makefile_dir='src/gulp/libgulp'
        )
    ],
    cmdclass={'build_ext': MakefileBuild},
)