## udacity-airflow-project
This is Airflow Data Pipelines project assignment in Udacity Data Engineering Nanodegree program.

The project uses Airflow to define and schedule interdependent set of Python processing modules which take set of json files from public Udacity AWS S3 buckets, load them into AWS Redshift staging tables, extract and populate dimension tables in a star schema and then run verification data quality checks.

The Airflow role in this project is 3-fold:
- As a scheduling and orchestration tool
- As a programming framework in a sense that Python code that you write does need to confirm to Airflow conventions and rules
- As an automatic workflow documentation and diagramming tool. It builds workflow diagrams showing dependencies between processing steps and it also shows execution history in a web interface.

The Airflow runs outside of AWS. 
It starts Python execution steps which use AWS credentials to remotely execute Redshift copy and various sql commands.
The actual data never leaves AWS. It moves from S3 to Redshift and then gets massaged within Redshift.

The main concept in Airflow is that a workflow needs to be expressed as a so called "Directed Acyclic Graph", or DAG.
All operations are related to a DAG.
Individual processing steps are then make up the DAG, with the DAG controlling which step follows which.
Here is how this project DAG looks in Airflow web page :

![alt text](https://github.com/abalbekov/udacity-airflow-project/blob/master/dags.PNG "a DAG in Airflow DAGs page")

Airflow can show this DAG in either "Graph" view:
![alt text](https://github.com/abalbekov/udacity-airflow-project/blob/master/dag-graph.PNG "a DAG Graph view in Airflow")

Or in "Tree" view:
![alt text](https://github.com/abalbekov/udacity-airflow-project/blob/master/dag-tree.PNG "a DAG Tree view in Airflow")




 

