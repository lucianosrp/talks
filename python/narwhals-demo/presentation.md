---
title: "Narwhals"
sub_title: Discover DataFrame Interoperability
author: Luciano Scarpulla
theme:
  name: catppuccin-latte
options:
  end_slide_shorthand: true
  incremental_lists: true
---


What is Narwhals?
==

Narwhals is a python library that provides a unified interface for working with dataframes in various libraries such as Pandas, Pyspark, DuckDB, etc.

<!-- pause -->
```sh
uv add narwhals
```

---

Who is Narwhals for?
==

Narwhals is ideal for:
- Developers building libraries that work with dataframes
- Teams creating applications that process tabular data
- Anyone wanting to write dataframe-agnostic code

---
Why using Narwhals?
==

# Lightweight
- It has zero dependencies
- Fast
- Little overhead

# Easy to use
- Flexible Polars API syntax
- Compatible with lazy and eager frames


---

Use case example
==

Let's imagine we have two major projects:

- **Project A**: A data processing pipeline that uses Pandas
- **Project B**: Another project that uses Polars

<!-- pause -->
What if we are tasked to create a new tool that needs to be integrated in both projects?

<!-- pause -->
```python

def get_max_datetime_pd(frame: pd.DataFrame, category: str) -> datetime:
    ...

def get_max_datetime_pl(frame: pl.DataFrame, category: str) -> datetime:
    ...

```
<!-- pause -->
```python
def get_max_datetime(frame: pd.DataFrame | pl.DataFrame, category: str) -> datetime:
    if isinstance(frame, pd.DataFrame):
        ...
    elif isinstance(frame, pl.DataFrame):
        ...
    else:
        raise TypeError("Unsupported dataframe type")

```
---

Use case example (introducing Narwhals)
==

```python
import narwhals as nw

def get_max_datetime(frame: nw.IntoFrame, category: str) -> datetime:
    ...

```
