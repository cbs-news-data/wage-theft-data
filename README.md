# Wage Theft Complaints

A collection of state wage theft complaint data gathered by CBS News. 

## Introduction

Wage theft is a common problem in the United States. It can take many forms, including: 
- Not paying workers the federal, state, or local minimum wage
- Not paying workers overtime
- Failing to pay tipped workers the difference between their tips and the minimum wage
- Denying workers meal breaks
- Denying workers sick time
- Misclassifying workers as independent contractors

When someone is a victim of wage theft, they have several options. They can ask file complaints with the U.S. Department of Labor, though the department has limited resources to investigate these complaints and typically only pursues those with large dollar amounts or lots of victims. They can also file a lawsuit, though this can be expensive and time-consuming. Most victims of wage theft do not pursue either of these options. Instead, they file complaints with their state's department of labor. 

In nearly every state in the country, state labor departments are supposed to investigate wage theft complaints, issue findings, and help recover funds on behalf of victims and levy fines to deter future violations. 

Beginning in July 2022, I began requesting wage theft complaint data from nearly every state in the country. I asked for more than a decade of case-level data from each department, including the name of the employer, the amount of money the worker claimed they were owed, the outcome of the case, the amount of money the state determined was owed, and the amount of money the state recovered on behalf of the worker.

This repository contains all the data I received from each state, as well as all the code used to clean, merge, and analyze the data. 

## Methodology overview

The layout, time period and quality of the data I received from each state varied wildly between jurisdictions. The greatest challenge was getting it to a point where it could be analyzed nationally. 

### State-specific data cleaning

The data from each state was cleaned and standardized individually. Data was extracted from the source files provided in response to our public records requests, and cleaned using Python. Values for things like disposition codes and case types were standardized, and rows with missing or apparently invalid data were dropped wherever possible. 

### Merging data from multiple states

Once the data was extracted, cleaned, and passed validation, it was compiled into a single csv file. Because the data from each state was so different, I had to make some decisions about how to standardize it. Those decisions centered around three main questions for the analysis: 

#### **1. How much money were people owed?**

This may seem like a simple question, but in practice it was quite challenging to determine. Some states separately tracked the amount the person claimed they were owed, the amount they assessed was owed after the investigation, and the amount collected on behalf of the claimant. Others did not. Instead, some only collected the assessed amount, while others only collected the amount claimed. Some provided case statuses that clearly denoted a case as having been decided in favor of the claimant, while others did not. 

To address this inconsistency, I assigned an "overall case amount" field to each case in the cleaned data. That field prioritized the amount the agency *assessed* was owed to the claimant, because that accounted for cases where the claimant claimed they were owed more than was determined (or the opposite - claimed they were owed less than was assessed, which did occur). If the assessed amount was not available for a given case but the amount paid was available, I used the amount paid. If neither were available, I used the amount claimed. This gave me the most reliable estimate of the value of each case possible given the quality of the data. 

#### **2. What was the outcome of each case?**

Because the mere existence of a wage theft complaint does not necessarily mean that wage theft occurred, I needed to be able to distinguish between cases where the agency determined that wage theft occurred and cases where it did not. Like the case amounts, this was not a simple task. To address this, I assigned a single Boolean field to each case which denoted whether the case was decided by the agency in favor of the claimant. 

Some states provided a case status or disposition that indicated the outcome of the case. In many cases, the meanings of those status codes were not immediately obvious to me and required some follow-up with each agency, which are documented in the issues for this repo. 

Other states appeared not to track the dispositions at all. For those cases, I used the amounts described above to infer the outcome of the case. Here is an outline of the decisions that were made for each case that did not have a clear outcome based on the status: 
1. If the case had no amounts at all, it was determined to have not been decided in favor of the claimant
2. If the case had an amount assessed greater than zero, it was determined to have been decided in favor of the claimant
3. If the case had an amount assessed of zero, it was determined to have not been decided in favor of the claimant
4. If the case had an amount paid greater than zero, it was determined to have been decided in favor of the claimant
5. If the case had an amount paid of zero, it was determined to have not been decided in favor of the claimant
6. All other cases were determined to have not been decided in favor of the claimant

Some case statuses were indeterminate - i.e., they indicated that a case was incomplete or its outcome was ambiguous. In those cases, I used the amount as described above. 

Finally, to prevent confusion and make this process more auditable, I assigned a "case decided in favor of claimant reason" column to each case that denoted which of the above steps was used to make this determination. 

#### **3. How long did each case take to resolve?**

The case duration was the least widely available of these three data points. Few states actually provided enough data to do this analysis. However, for those that did, I calculated the duration of each case by subtracting the date the case was opened from the date it was closed.

For the start date, I used the date the case was opened as reported by the labor department. If the case had the date money was paid to the claimant, I used that date for the end date. If the case did not have a date money was paid to the claimant, I used the date the case was closed, if one was provided. 

### Analysis

Once the data was cleaned and merged, I used Python to analyze it. The analysis is contained in the [`national_analysis.ipynb`](notebooks/national_analysis/national_analysis.ipynb) notebook in this repo.

First, I assessed the completeness of the data from each state in order to determine which states' data can be used for each data point in the analysis. 

Then, I calculated the total number of cases, total amount claimed, total amount assessed, and total amount paid for the entire dataset, and for each state. I also analyzed the case duration for states that provided it.

I did not analyze trends over time because the time periods covered by each state's data varied widely. 

Finally, after doing the analysis, I added an additional data point: the relationship between the overall case amounts and each state's felony theft threshold. This was done to determine whether a case would have been considered a felony theft in each state if it had been an incident of property theft rather than wage theft. 

## Project structure

### Raw data

All data files obtained from state labor departments are stored in the [`raw/`](raw/) directory. Each file or files are stored in a subdirectory named after the state from which they were obtained, and are in the original format with their original file names. 

### Hand-generated files

The [`hand/`](hand/) directory contains files that I created by hand that are used in various points in this pipeline. 

### Code

All code used by two or more tasks or subtasks in this project are stored in [`shared/src/`](shared/src/). They include: 
1. [`normalize_data.py`](shared/src/normalize_data.py) - does cleaning tasks common to multiple datasets and outputs a standardized csv file
2. [`schema.py`](shared/src/schema.py) - contains a pandera schema that validates the output of normalize_data.py
3. [`shared_functions.py`](shared/src/shared_functions.py) - contains functions used by multiple tasks
4. [`fix_case_status.py`](shared/src/fix_case_status.py) - contains functions used to fix some edge cases that were explicitly labeled as open but had paid amounts
5. [`constants.py`](shared/src/constants.py) - contains constants used by multiple tasks

There is also a [`shared/bin/`](shared/bin/) directory that contains the binary file for tabula, which is used to extract text from some PDF documents that are not machine-readable.

There is also a [`scripts/`](scripts/) folder that contains scripts used in the development of this project. They are not needed to run the pipeline. 

### Tasks

The data cleaning and analysis tasks are stored in the [`tasks/`](tasks/) directory. There are 5 tasks: 
1. [transform source data](tasks/1-transform-source-data/) - transforms the raw data into a standardized format
2. [merge transformed files](tasks/2-merge-transformed-files/) - merges the transformed files into a single file
3. [assign new fields](tasks/3-assign-new-fields/) - assigns fields for overall case amount, case outcome, and case duration to each case
4. [get report data](tasks/4-get-report-data/) - calculates totals and does aggregation used in state-level reports
5. [generate state reports](tasks/5-generate-state-reports/) - uses a jinja template to generate a report for each state we got data for

Each task contains an `input` folder containing symlinks to all data files needed for that task, an `output` folder where output files of that task are written, and an optional `hand` and `src` directory that contain task-specific files. 

Each task also contains a `Makefile` that runs the task. 

### Notebooks

The [`notebooks/`](notebooks/) directory contains notebooks used for analysis. All analysis is done in the [`national_analysis.ipynb`](notebooks/national_analysis/national_analysis.ipynb) notebook.

There is also [`notebooks/texas`](notebooks/texas/) directory that contains notebooks used to analyze the Texas data. Documentation of the particular quirks of the Texas data are in that notebook. 

## Reproducing this analysis

This repo is designed to be reproducible with a few simple steps. To do so, you will need to have a linux machine running Ubuntu 20.04 and a minimum python version of 3.9.

### 1. Clone this repo

```bash
git clone https://github.com/CBS-Innovation-Lab/wage-theft-data.git
```

### 2. Install the dependencies

This project uses a recursive makefile system to run the entire analysis with a single command. Once you've cloned the repo, you can install the dependencies by running the following command from the root directory of the repo: 

```bash
make init
```

This will create a python virtual environment, install any python dependencies, and also install any system dependencies needed. 

### 3. Run the analysis

Once the dependencies are installed, you can run the analysis by running the following command from the root directory of the repo: 

```bash
make
```

This will run the entire analysis, including all tasks and subtasks, in the correct order. 
