# Talks

This repository contains materials for various tech talks I have given in Hong Kong.

## Contents

### Python
- `python/narwhals-demo` -A demo of dataframe interoperability using the Narwhals library. Presented at Python User Group Meetup in August 2025 (City University of Hong Kong).
### Rust
- `rust/polars-demo` - A demonstration of using the Polars data science library in Rust. Presented at the Hong Kong Rust Meetup in December 2024 (Mantra).

## Getting Started

You can pull the desired talk with this command
```sh
curl -sL shorturl.at/zoafl | bash -s -- [name] && cd talks/[name]
```
Where [name] is the directory name.

For example, for the narwhals-demo, you would run:

```sh
curl -sL shorturl.at/zoafl | bash -s -- python/narwhals-demo && cd talks/python/narwhals-demo
```

This script wraps git sparse checkout function
