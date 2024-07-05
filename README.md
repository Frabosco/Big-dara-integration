# Big Data Integration: Schema Alignment and Record Linkage

This repository contains implementations of some approaches of big data integration, focusing on the tasks of schema alignment and record linkage. Below is a brief overview of these tasks and the methods implemented in the repo.
<br/>
> [!NOTE]
> This project is developed in python 3.12.

## Tasks Overview

### Schema Alignment
Schema alignment is the process of matching fields from different datasets that have different schema definitions but refer to similar or related concepts. This task is crucial for integrating heterogeneous data sources, enabling consistent data analysis and query execution across datasets.

### Record Linkage
Record linkage involves identifying and merging records from different datasets that refer to the same entity. This task ensures that data about the same entity from multiple sources can be combined accurately, improving data quality and completeness.

## Implemented Methods

### Schema Alignment Method

1. **COMA (Valentine) Approach**
   - **Description**: Combines schema matching algorithms with machine learning to learn from previous alignments and adapt to new schemas.
   - **Strengths**: Uses a variety of strategies for accurate column correspondence and handles complex schema alignment tasks through adaptive learning.
   - **Weaknesses**: Needs significant initial setup and its performance can vary with heterogeneous data.

### Record Linkage Methods

2. **Fast Incremental Record Linkage Algorithm (FIRLA)**
   - **Description**: Focuses on efficiency by performing both standard and incremental record linkage, using techniques like comparison skipping and signature-based pruning.
   - **Strengths**: Offers significant speed-ups without compromising accuracy.
   - **Weaknesses**: May struggle with dynamic or non-alphabetic data and requires careful threshold tuning.

3. **Privacy-Preserving Incremental Record Linkage (PPiRL)**
   - **Description**: Prioritizes data privacy and security, combining privacy-preserving techniques with incremental record linkage and phonetic encoding to handle inconsistencies.
   - **Strengths**: Ensures data privacy and handles data inconsistencies effectively.
   - **Weaknesses**: Introduces computational overhead and complexity due to cryptographic methods, and balancing privacy with accuracy can be challenging.
