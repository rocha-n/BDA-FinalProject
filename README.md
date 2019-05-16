# BDA-FinalProject
HES-SO Big Data Analytics project

This is a project of Big Data Analytics, teached in the context of Master HES-SO.

## Main idea and goals
We are going to use data from Kaggle, that is thausends of whines described and rated by eonologs.
We will match this data with surface radiation data, obtained from another source.

The first dataset contains region information, and our first big task will be to match this information with the surface ratiation, in order to retrieve a medium value.
The difficulty is that the first data source gives us a string with a region name, optionally a second string with a more precise geographic information, and the second data source only works by gps coordinates.

Once we will have passed this data consiliation phase, we will be able to find if there is a correlation between solar radiance and the whine appreciation.
We will also be able to potentially map a whine to a region, based on its ranking (and potentially other parameters), using machine learning.

We will see alos in witch country the pinot noir is the best for exemple

## Data description
We used two datasets for this project. 

The first one was retrieved from kaggle, at [this address](https://www.kaggle.com/zynicide/wine-reviews). It contains two csv files, that have the following common fields:

Field | Description
------------ | -------------
country | The country that the wine is from
description | A few sentences from a sommelier describing the wine's taste, smell, look, feel, etc.
designation | The vineyard within the winery where the grapes that made the wine are from
points | The number of points WineEnthusiast rated the wine on a scale of 1-100 (though they say they only post reviews for wines that score >=80)
price | The cost for a bottle of the wine
province | The province or state that the wine is from
region_1 | The wine growing area in a province or state (ie Napa)
region_2 | Sometimes there are more specific regions specified within a wine growing area (ie Rutherford inside the Napa Valley), but this value can sometimes be blank
variety | The type of grapes used to make the wine (ie Pinot Noir)
winery | The winery that made the wine

One of the files has this extra informations:

Field | Description
------------ | -------------
taster_name | Name of the taster that reviewed this wine
taster_twitter_handle | Taster's twitter name
title | The title of the wine review, which often contains the vintage if you're interested in extracting that feature


The Kaggle page states that there are duplicates in the files, and that the extended one has around 25K more records to play with.

The second dataset if from the [OpenSolarDB project](http:\\www.opensolardb.org).  
They provide data comming from measures of solar radiation on specific points on the planet.  
Their data has the following structure:

Field | Description
------------ | -------------
country (ISO 3166-1) | Country identification, for instance US, UK, ES, etc.
Place | The name of the place where data has been measured
Lattitude | Latitude where the measures have been taken
Longitude | Longitude where the measures have been taken 
Radiation January | Radiation for January in MJ/(day*m2)
 ... | ...
Radiation December | Radiation for December in MJ/(day*m2)


So the first step is to perform some data cleaning on the kaggle files, in order to obtains a single table without duplicates, nor records where country or region information is null.  

After that, we will have to match each record of the database with climate records, in order to associate with each wine a medium solar irradiation value. Obviously, we will have to match the region_1 and region_2 fields from the Kaggle database with the Region field of the OpenSolar one. 

//TODO : If we don't find a match, medium value or closest point?

### Dataset cleaning and data quality observations

<!-- 
select * into finalWine from (
select [country]
      ,[description]
      ,[designation]
      ,[points]
      ,[price]
      ,[province]
      ,[region_1]
      ,[region_2]
      ,[variety]
      ,[winery] from winemag
union
select [country]
      ,[description]
      ,[designation]
      ,[points]
      ,[price]
      ,[province]
      ,[region_1]
      ,[region_2]
      ,[variety]
      ,[winery] from winemag2
	  ) as vFinalWine;
-- (170522 rows affected)

delete from finalWine where country is null;
-- (60 rows affected)

select count(1) from finalWine;
170462
-->

First of all, we had to merge the two Kaggle files. We cleaned the duplicates, so now we only have unique values in the database.
The total number wines we have is now 170522. Among those, there are 60 that have a null value in the country field. We won't obviously be able to calculate any solar radiation for a unknown place. Therefore, we deleted them from our dataset, bringing our number of records to 170462. This is now our wine DB.

Then, we had to retrieve the data from OpenSolarDB. As it is available only in a one file per country format, we listed all the countries present in our wine DB. For each one of the 50 countries available on our database, we downloaded manually a csv file. We haven't found any way to automate that phase.  
A that point, another problem surge: not all of our 50 countries were available on OpenSolarDB. 
The missing ones are at the count of five: Brazil, Montenegro, Peru, Uruguay, and US-France (no, this isn't a typo. There was a single wine that was made with grapes from this two countries, so it has been labeled like that in the database).

Here is a chart of the number of wines impacted by this issue:
<!-- select country, count(1) as cnt from finalWine where country in ('Brazil','Montenegro','Peru','Uruguay','US-France') group by country order by cnt desc -->

country	| nb of wines
---|---
Uruguay|	142
Brazil	|57
Peru	|16
Montenegro|	1
US-France|	1

So, between the null values removed at the first step, and the ones mentionned in the above chart, we have now a total of 0.16% of data loss to lack of consistency and/or availability of data.  

We spent a bit of time searching for a replacement for OpenSolarDB, in order to have a full coverage of data. Finally, due to the nearly-insignificant proportion of data loss, we agreed on going forward with our current data. It seems absolutely acceptable from our point of view.

//TODO: A chart of nb of wines / country, and maybe the ratings distributsion

### Matching regions with solar radiation

<!-- ESSAIS INFRUCTUEUX
select * from [xxxxx].[dbo].[wineWIP] where region_1 is not null and region_2 is not null

select * from solarAverages where country = 'US'

-- 1 -> create a mean of solar radiation
	select * into solarAverages from (select country, place, ([January]
      +[February]
      +[March]
      +[April]
      +[May]
      +[June]
      +[July]
      +[August]
      +[September]
      +[October]
      +[November]
      +[December])/12 as average from [xxxxx].[dbo].[radiation]) as vSolarAverages
	  
-- 2 -> delete those who have a country that is not in the OpenSolarDB database
	delete from WineWIP where country in ('Brazil','Montenegro','Peru','Uruguay','US-France');

-- 3 -> alter the table WineWIP to add a column for solar radiation
	ALTER TABLE [xxxxx].[dbo].[wineWIP] ADD solarRadiation float;

-- 4 -> check data coherence
	select count(1) from [xxxxx].[dbo].[wineWIP] where region_1 is null and region_2 is null;		  -- no region_1 and no region_2: 27686
	select count(1) from [xxxxx].[dbo].[wineWIP] where region_1 is not null and region_2 is null;	  -- only region_1				: 74909
	select count(1) from [xxxxx].[dbo].[wineWIP] where region_1 is null and region_2 is not null;	  -- only region_2				: 0
	select count(1) from [xxxxx].[dbo].[wineWIP] where region_1 is not null and region_2 is not null; -- region_1 and region_2		: 67650
																																------------
																									  -- TOTAL						 170245
-- 5 -> complete the records having no region_1 or region_2 values
UPDATE [xxxxx].[dbo].[wineWIP]
SET [xxxxx].[dbo].[wineWIP].solarRadiation = (select AVG(solarAverages.average) from solarAverages where solarAverages.country = [xxxxx].[dbo].[wineWIP].country) 
where [xxxxx].[dbo].[wineWIP].region_1 is null and [xxxxx].[dbo].[wineWIP].region_2 is null

-- 6 -> case only region_1
UPDATE [xxxxx].[dbo].[wineWIP]
SET [xxxxx].[dbo].[wineWIP].solarRadiation = (select AVG(solarAverages.average) from solarAverages where solarAverages.country = [xxxxx].[dbo].[wineWIP].country and [xxxxx].[dbo].[wineWIP].region_2 = solarAverages.Place) 
where [xxxxx].[dbo].[wineWIP].region_1 is not null and [xxxxx].[dbo].[wineWIP].region_2 is not null; -- 67650 matches
-->
