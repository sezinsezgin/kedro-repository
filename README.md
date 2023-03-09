# kedro-repository

This repository involves the kedro project for the crm pipeline created.
- The input data can be found under 01_raw 
- intermediate data can be found under 02_intermediate 
- the output data is in 03_primary in multiple formats saved from the pipeline.

#### Nodes
- Nodes that are used in the pipeline are under the path : crm-pipeline/src/crm_pipeline/pipelines/data_processing/nodes.py 

- Unit Tests on Nodes are under : crm-pipeline/src/tests/pipelines/data_processing/test_base_nodes.py and under crm-pipeline/src/tests/pipelines/data_processing/test_preprocessing_nodes.py

#### Pipeline
- Pipeline code is under crm-pipeline/src/crm_pipeline/pipelines/data_processing/pipeline.py
