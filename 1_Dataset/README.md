<div align='center'>
    <h1>Dataset</h1>
</div>

## Contents

* [Overview](#1-overview)
* [Download Link](#2-download-link)
* [Directory Structure](#3-directory-structure)
* [Data Example](#4-data-example)

## 1. Overview

This section shows the HyQBench dataset, which is constructed by an innovative method based on large language model generation. The dataset contains 7,463 queries, covering six query types, with each query consisting of three parts: natural language query, reasoning process, and answer. This directory provides data examples and download links for the complete dataset.

## 2. Download Link

This repository only displays part of the data. For the complete dataset, please go to [HyQBench](https://drive.google.com/drive/folders/1VpyF05rvHxEAIU3bwERNnWosrpqK6rri?usp=sharing) to download.

## 3. Directory Structure
```shell
Dataset
├── aggregate
│   ├── query
│   └── table
├── filter
├── group
├── join
├── sort
└── transform

```

A total of six types, each type has two folders, `query` and `table`, which store the json files for queries and the corresponding intermediate result tables, respectively.

## 4. Data Example

