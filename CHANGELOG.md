# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial implementation of the DLT Medallion Framework
- Base medallion decorator with DLT integration
- Layer-specific decorators (bronze, silver, gold)
- Decorator stacking support with metadata merging
- Comprehensive test suite for all components
- Documentation including README and contributing guidelines

### Changed
- Enhanced decorator registry to support proper ordering and validation
- Improved metadata handling for stacked decorators

### Fixed
- Circular dependency detection in decorator stacking
- Type hints and docstrings consistency

## [0.1.0] - YYYY-MM-DD

### Added
- Base project structure
- Core functionality for decorator registry
- Initial implementation of medallion pattern
- Basic test framework
- Documentation structure

[Unreleased]: https://github.com/your-username/dlt-framework/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/your-username/dlt-framework/releases/tag/v0.1.0 