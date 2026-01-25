"""
Kimball Framework Processing Module.

Contains core data processing components for ETL operations.
"""

from kimball.processing.late_arriving_dimension import LateArrivingDimensionProcessor
from kimball.processing.skeleton_generator import SkeletonGenerator

__all__ = [
    "LateArrivingDimensionProcessor",
    "SkeletonGenerator",
]
