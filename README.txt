Download another user's IMDb ratings

usage: main.py [-h] [--start START] [--cookies COOKIES] [--threads THREADS]
               ratings_url outfile

positional arguments:
  ratings_url        URL to IMDb user ratings page
  outfile            Path to output CSV file

optional arguments:
  -h, --help         show this help message and exit
  --start START      Specify page number to start from
  --cookies COOKIES  Load cookies from file
  --threads THREADS  Number of simultaneous downloads