#!/bin/bash
# Script for building and publishing the package to PyPI

set -e  # Exit on error

# Create directory if it doesn't exist
mkdir -p scripts

# Clean up any existing build artifacts
echo "Cleaning up previous builds..."
rm -rf build/ dist/ *.egg-info/

# Create and activate virtual environment
echo "Setting up virtual environment..."
python3 -m venv .venv
source .venv/bin/activate

# Install build dependencies
echo "Installing build dependencies..."
pip install --upgrade build twine

# Build the package
echo "Building package..."
python -m build

# Run twine check to verify the package
echo "Checking package..."
twine check dist/*

# Ask for confirmation before publishing
read -p "Do you want to publish to PyPI? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Publishing to PyPI..."
    twine upload dist/*
    echo "Package published successfully!"
else
    echo "Publication aborted. Package files are available in the 'dist/' directory."
fi 