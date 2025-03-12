from typing import List

from setuptools import Extension, setup


def get_requirements() -> List[str]:
    """
    parse requirements from requirements.txt file.

    Args:
        None

    Returns:
        List[str]: list of required packages
    """
    with open("requirements.txt") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]


# define the extension module using standard setuptools approach
gulp_extension = Extension(
    'gulp.libgulp',
    # list all your source files here
    sources=['src/gulp/libgulp/libgulp.c'],
    # include directories
    include_dirs=['src/gulp/libgulp/include'],
    # libraries to link against
    libraries=[],
    # additional compiler flags if needed
    extra_compile_args=['-O3'],
    # additional linker flags if needed
    extra_link_args=[],
)

setup(
    name="gulp",
    install_requires=get_requirements(),
    ext_modules=[gulp_extension],
)

