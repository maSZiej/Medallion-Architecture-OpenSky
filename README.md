# open sky pipeline
Project have on purpose to show aircraft traffic on Polish sky
Project uses medallion architecture with 3 layers bronze, silver, gold

### Bronze layer:

Raw data from open sky api
Stored in s3 bucket 

### Silver layer:

Provides sufficient data types and enrichment columns
newest timestamp stored in s3 bucket
previus timestamps stored in history version of table

### Gold layer:

Agreggate Data to bussines goals

### Data ingestion type

Batching

### Data Flow
Here is visualalization of data flow:

<p align="center">
<img src="Images/Data_Flow.jng" width="400" alt="Centered Screenshot">
</p>

### Technologies
| Technology | used For|
|----------|----------|
| UV | libraries depedency| 
| Pyspark | disitribiuted processing|
| Minio | data lake - S3 bucket on prem | 
| Delta-spark | transaction and data time travel| 
| Kedro | bulding effective pipeline| 
| Docker| contenerization services | 
| Github | version control| 
<!-- | Row 2 C1 | Row 2 C2 | Row 2 C3 |
| Row 2 C1 | Row 2 C2 | Row 2 C3 |
| Row 2 C1 | Row 2 C2 | Row 2 C3 | -->
<!-- Technologies used: -->

<!-- Airflow for orchestration and automatization

Github-action for CI/CD
Kubernetes for deploy
Terraform for manage kubernetes -->


# New Kedro Project

[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro)](https://kedro.org)

## Overview

This is your new Kedro project with PySpark setup, which was generated using `kedro 1.5.0`.

Take a look at the [Kedro documentation](https://docs.kedro.org) to get started.

## Rules and guidelines

In order to get the best out of the template:

* Don't remove any lines from the `.gitignore` file we provide
* Make sure your results can be reproduced by following a [data engineering convention](https://docs.kedro.org/en/stable/faq/faq.html#what-is-data-engineering-convention)
* Don't commit data to your repository
* Don't commit any credentials or your local configuration to your repository. Keep all your credentials and local configuration in `conf/local/`

## How to install dependencies

Declare any dependencies in `requirements.txt` for `pip` installation.

To install them, run:

```
pip install -r requirements.txt
```

## How to run your Kedro pipeline

You can run your Kedro project with:

```
kedro run
```

## How to test your Kedro project

Have a look at the files `tests/test_run.py` and `tests/pipelines/data_science/test_pipeline.py` for instructions on how to write your tests. Run the tests as follows:

```
pytest
```

You can configure the coverage threshold in your project's `pyproject.toml` file under the `[tool.coverage.report]` section.

## Project dependencies

To see and update the dependency requirements for your project use `requirements.txt`. Install the project requirements with `pip install -r requirements.txt`.

[Further information about project dependencies](https://docs.kedro.org/en/stable/kedro_project_setup/dependencies.html#project-specific-dependencies)

## How to work with Kedro and notebooks

> Note: Using `kedro jupyter` or `kedro ipython` to run your notebook provides these variables in scope: `catalog`, `context`, `pipelines` and `session`.
>
> Jupyter, JupyterLab, and IPython are already included in the project requirements by default, so once you have run `pip install -r requirements.txt` you will not need to take any extra steps before you use them.

### Jupyter
To use Jupyter notebooks in your Kedro project, you need to install Jupyter:

```
pip install jupyter
```

After installing Jupyter, you can start a local notebook server:

```
kedro jupyter notebook
```

### JupyterLab
To use JupyterLab, you need to install it:

```
pip install jupyterlab
```

You can also start JupyterLab:

```
kedro jupyter lab
```

### IPython
And if you want to run an IPython session:

```
kedro ipython
```

### How to ignore notebook output cells in `git`
To automatically strip out all output cell contents before committing to `git`, you can use tools like [`nbstripout`](https://github.com/kynan/nbstripout). For example, you can add a hook in `.git/config` with `nbstripout --install`. This will run `nbstripout` before anything is committed to `git`.

> *Note:* Your output cells will be retained locally.

## Package your Kedro project

[Further information about building project documentation and packaging your project](https://docs.kedro.org/en/stable/tutorial/package_a_project.html)
