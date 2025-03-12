"""Root level conftest to adjust Python path."""
import sys
import os

# Add the src directory to the path so imports work correctly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__)))) 