# Contributing to DLT Medallion Framework

First off, thank you for considering contributing to the DLT Medallion Framework! It's people like you that make this framework better for everyone.

## Code of Conduct

This project and everyone participating in it is governed by our Code of Conduct. By participating, you are expected to uphold this code.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the issue list as you might find out that you don't need to create one. When you are creating a bug report, please include as many details as possible:

* Use a clear and descriptive title
* Describe the exact steps which reproduce the problem
* Provide specific examples to demonstrate the steps
* Describe the behavior you observed after following the steps
* Explain which behavior you expected to see instead and why
* Include any error messages or stack traces

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, please include:

* Use a clear and descriptive title
* Provide a step-by-step description of the suggested enhancement
* Provide specific examples to demonstrate the steps
* Describe the current behavior and explain which behavior you expected to see instead
* Explain why this enhancement would be useful

### Pull Requests

* Fill in the required template
* Do not include issue numbers in the PR title
* Include screenshots and animated GIFs in your pull request whenever possible
* Follow the Python style guide
* Include tests for new features
* Document new code based on the Documentation Styleguide

## Development Process

1. Fork the repository
2. Create a new branch for your feature
3. Make your changes
4. Run the test suite
5. Submit a Pull Request

### Setting Up Development Environment

```bash
# Clone your fork
git clone https://github.com/your-username/dlt-framework.git
cd dlt-framework

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -e ".[dev]"
```

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_specific.py

# Run with coverage
pytest --cov=dlt_framework tests/
```

## Style Guide

### Python Style Guide

* Follow PEP 8
* Use type hints for function arguments and return values
* Use docstrings for all public modules, functions, classes, and methods
* Keep lines under 100 characters
* Use meaningful variable names

### Documentation Style Guide

* Use Markdown for documentation
* Include code examples when relevant
* Keep documentation up to date with code changes
* Document both the what and the why

### Commit Messages

* Use the present tense ("Add feature" not "Added feature")
* Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
* Limit the first line to 72 characters or less
* Reference issues and pull requests liberally after the first line

## Project Structure

```
dlt-framework/
├── dlt_framework/          # Main package directory
│   ├── core/              # Core functionality
│   ├── decorators/        # Layer decorators
│   └── utils/             # Utility functions
├── tests/                 # Test files
├── docs/                  # Documentation
├── examples/              # Example code
└── setup.py              # Package setup file
```

## Testing Guidelines

* Write tests for all new features
* Maintain test coverage above 80%
* Use meaningful test names that describe the behavior being tested
* Use fixtures and parametrize when appropriate
* Mock external dependencies

## Documentation Guidelines

* Keep README.md up to date
* Document all public APIs
* Include examples in docstrings
* Update user guide for significant changes
* Add doctest examples where appropriate

## Release Process

1. Update version number in setup.py
2. Update CHANGELOG.md
3. Create a new release on GitHub
4. Build and upload to PyPI

## Questions?

Feel free to open an issue with the "question" label if you have any questions about contributing.

## License

By contributing, you agree that your contributions will be licensed under the MIT License. 