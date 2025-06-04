# polygon


## Requirements

This python app will take a list of nasdaq stocks and periodically add market data using
polygon.io to update a csv on AWS s3. At a certain time of day, it will run analysis on 
the qualifiers below to screen the stocks and alert the user using SNS on the filtered data.

## Qualifying data

- volume > 300K
- >= 2.5% from previous close
- previous close >= $0.01
- open > previous close

As far as data I keep an eye on:
- market cap
- avg volume 
- Latest quote info at given time intervals in cdt
  - previous day close
  - open
  - 8:32
  - 8:32
  - 8:40
  - 8:50
  - 8:55
  - 9:00
  - 9:15
  - 9:30
  - 2:30
- high of the day, but after the 8:50 am cdt timestamp
- low of the day, but after the 8:50 am cdt timestamp