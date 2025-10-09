<div align="center">
    <h1>Code of HyQBench</h1>
</div>

## 📑 Contents

- [Overview](#📝-overview)
- [Environment Setup](#⚙️-environment-setup)
- [Download Data](#📥-download-data)
- [Generate Data (Optional)](#🛠️-generate-data-optional)
- [Evaluate](#📊-evaluate)
- [Relevant Projects](#🔗-relevant-projects)


## 📝 Overview

This directory contains all the code and usage instructions for HyQBench, supporting automated data generation and evaluation.

## ⚙️ Environment Setup

First, ensure that your machine has Python 3.10 installed.

```shell
$ python --version
Python 3.10.16
```

Next, clone this repository and install the dependencies for the project.

```shell
# Clone the repository
$ git clone https://github.com/XMUDM/HyQBench.git

# Enter the directory
$ cd HyQBench/2_Code

# Install dependencies
$ pip install -r requirements.txt 
```



## 📥 Download Data

### Tables
It should be noted that both data generation and evaluation are based on sampled tables, which is done to ensure efficiency. Therefore, the table data should first be downloaded from:

- [HyQBench/data.tar.gz](https://drive.google.com/drive/folders/1VpyF05rvHxEAIU3bwERNnWosrpqK6rri?usp=sharing)

After downloading, place the `data` folder into the `2_Code/dataGeneration` directory.

### Test Data
All test data required for evaluation is needed to be downloaded from:
- [HyQBench/test.tar.gz](https://drive.google.com/drive/folders/1VpyF05rvHxEAIU3bwERNnWosrpqK6rri?usp=sharing)

After downloading, place the `test` folder into the `2_Code/evaluate` directory.


## 🛠️ Generate Data (Optional)

Data generation is optional. If you want to use our method to generate your own data, you can achieve it through the code in the `dataGeneration` directory.

```shell
$ cd dataGeneration
```

### Reasoning Process Generation

First, execute the following command to get the reasoning process (operator sequence) for a flat query.
```shell
$ python flat_generate.py
```

Then execute the following command to expand the flat queries into nested queries.
```shell
$ python nested_generate.py
```

### Reasoning Process Generation

The following command is for query optimization, to get efficient operator order.
```shell
$ python optimize.py
```

Next, execute the operator sequence obtained from the topological sorting to get the answer.
```shell
$ python answer_generate.py
```

### Reasoning Process Generation

Finally, add the corresponding natural language queries and some thoughts from the LLM to the entire data.
```shell
$ python query_generate.py
```



## 📊 Evaluate

First, generate results from different models and methods through automated processes, then evaluate the results.

### Configuration Files

#### evaluate/test.sh 

This is the main script for generating results. You can set the GPU, target, and model to be tested in this script.

```shell
GPU_DEVICES="0,1"           # GPU
TARGET="lotus"              # lotus and blendsql are used to test the end-to-end performance of two approaches, cost is used to verify the effectiveness of query optimization strategies and test the planning capability of the model
MODEL="DeepSeek-V3"         # LLMs such as DeepSeek-V3, GPT-4.1, and Qwen-Max, etc.
```

#### utils/llm.py

This file is a model configuration file that can specify the model and path to be used.

```shell  
# Online model configuration
Add your own api-key and fill in the model type.

# Local model configuration
Modify the parameters of the from_pretrained function to specify the local model storage location. Two methods are supported for setting:
1、Change the corresponding value to the absolute path of the model.
2 If the above local path does not exist, the huggingface model is used.
```

Each LLM is defined as a class, and is instantiated directly when used. You can add your own defined LLM.

### Generate Results

Execute test.sh directly.

```shell
$ cd evaluate
$ ./test.sh
```

### Evaluate Results

Three scripts correspond to the evaluation of three types of results. Change the model name to be evaluated in the main function.

```shell
$ python evaluate_lotus.py   # Evaluate the end-to-end performance of Lotus with different LLMs

$ python evaluate_blendsql.py   # Evaluate the end-to-end performance of BlendSQL with different LLMs

$ python evaluate_cost.py   # Provide the ratio of the cost of the solution provided by LLM to the cost of the solution provided by HyQBench.
```



## 🔗 Relevant Projects

- [lotus-data/lotus](https://github.com/lotus-data/lotus)

