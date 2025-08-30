#!/usr/bin/env python3
"""
Test runner script for the stock pipeline application.

This script runs all unit tests with proper configuration and provides
a summary of test results.
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path

def main():
    """Run the test suite with proper configuration."""
    parser = argparse.ArgumentParser(description='Run stock pipeline tests')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Verbose output')
    parser.add_argument('--coverage', '-c', action='store_true',
                       help='Run with coverage report')
    parser.add_argument('--test-file', '-t', type=str,
                       help='Run specific test file')
    parser.add_argument('--test-function', '-f', type=str,
                       help='Run specific test function')
    
    args = parser.parse_args()
    
    # Get the tests directory
    tests_dir = Path(__file__).parent
    app_dir = tests_dir.parent
    
    # Add app directory to Python path
    sys.path.insert(0, str(app_dir))
    
    # Build pytest command
    cmd = ['python', '-m', 'pytest']
    
    if args.verbose:
        cmd.append('-v')
    
    if args.coverage:
        cmd.extend(['--cov=src', '--cov-report=html', '--cov-report=term'])
    
    if args.test_file:
        cmd.append(args.test_file)
    else:
        cmd.append(str(tests_dir))
    
    if args.test_function:
        cmd.append(f'-k={args.test_function}')
    
    # Add pytest options for better output
    cmd.extend([
        '--tb=short',
        '--strict-markers',
        '--disable-warnings'
    ])
    
    print(f"Running tests with command: {' '.join(cmd)}")
    print(f"Tests directory: {tests_dir}")
    print(f"App directory: {app_dir}")
    print("-" * 50)
    
    try:
        # Run the tests
        result = subprocess.run(cmd, cwd=app_dir, check=False)
        
        print("-" * 50)
        if result.returncode == 0:
            print("✅ All tests passed!")
        else:
            print("❌ Some tests failed!")
            sys.exit(result.returncode)
            
    except KeyboardInterrupt:
        print("\n⚠️  Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
