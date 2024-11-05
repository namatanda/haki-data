# Court Caseload Analytics

Haki Data is a Python tool for processing and analyzing court case data. It provides functionality to clean, transform, and analyze court case information, generating insights on case outcomes, timelines, and more.

## Features
- Raw data processing  
- Data cleaning and preprocessing
- Case categorization
- Timeline analysis
- Outcome analysis
- Productivity metrics
- Judgment scheduling analysis

## Installation

1. Ensure you have Python 3.8 or later installed.
2. Install Poetry if you haven't already:
   ```
   curl -sSL https://install.python-poetry.org | python3 -
   ```
3. Clone the repository:
   ```
   git clone https://github.com/yourusername/haki-data.git
   cd haki-data
   ```
4. Install the dependencies using Poetry:
   ```
   poetry install
   ```

## Usage

1. Activate the Poetry virtual environment:
   ```
   poetry shell
   ```

2. Run the main script:
   ```
   haki-data
   ```

   This will process the default input file and generate analysis results.

3. To use custom input files or specify output locations, use command-line arguments:
   ```
   haki-data --input /path/to/input.csv --output /path/to/output/
   ```
   ```
   haki-data '/home/stanoo/dcrt/data/INPUT' dcrt_2020-2021.csv --start_year 2020 --end_year 2021
   ```
## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.


## Usage

Here's a basic example of how to use the package:

```python
from haki_data import process_case_status, analyze_court_outcomes
import pandas as pd

# Load your data
df = pd.read_csv('your_data.csv')

# Process case status
df = process_case_status(df, RESOLVED_OUTCOMES)

# Analyze court outcomes
outcomes = analyze_court_outcomes(df, '2023-07-01', '2024-06-30', 'concluded')

print(outcomes)
```

For more examples, see the `examples/` directory.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.