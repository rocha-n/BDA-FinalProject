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

//TODO: The second dataset description

So the first step is to perform some data cleaning on the kaggle files, in order to obtains a single table without duplicates, nor records where country or region information is null.  

After that, we will have to match each record of the database with climate records, in order to associate with each wine a medium solar irradiation value.

### Cleaning the wine dataset


