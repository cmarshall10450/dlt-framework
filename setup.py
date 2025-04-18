from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="dlt-framework",
    version="0.1.0",
    author="Your Organization",
    author_email="your.email@organization.com",
    description="A decorator-based framework for Delta Live Tables implementing the Medallion architecture",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/dlt-framework",
    packages=find_packages(include=["dlt_framework", "dlt_framework.*"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "black>=23.3.0",
            "isort>=5.12.0",
            "flake8>=6.0.0",
            "mypy>=1.3.0",
            "pytest>=7.3.1",
            "pytest-cov>=4.1.0",
        ],
    },
) 