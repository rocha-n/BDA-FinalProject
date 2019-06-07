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

We will see alos in witch country the pinot noir is the best for exemple.

Also, this data will allow us to answer to the following questions:  
1 - What is the solar irradiation corellation with the appreciation score?  
2 - Is there a correlation between the price of the wine and the appreciation score?  
3 - What are the most appreciated grapes types?  


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

First of all, we had to merge the two Kaggle files (280910 ligne). We cleaned the duplicates, so now we only have unique values in the database.
The total number wines we have is now 170426. Among those, there have a null value in the country field and the points. We won't obviously be able to calculate any solar radiation for a unknown place. Therefore, we deleted them from our dataset, bringing our number of records to 170426. This is now our wine DB.

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

##Démarche
Pour la création des données, nous avons passé par la création de plusieurs fichiers.
La création de fichiers n’a pas été facile réaliser nous somme aller voir sur stack overflow pour trouver la manière de pouvoir créer des fichiers csv. 
Comme nous travaillons avec peu de données, il est possible de créer un seul fichier csv. 
Si nous avions beaucoup de données, il aurait fallu premièrement travailler avec un autre type de fichier pour que cela soit plus rapide avec Spark et cela aurait aussi généré plusieurs fichiers.
L’avantage d’un fichier CSV c’est qu’il peut être lu facilement par les humais.
C’est pour cette raison que nous avons utilisé ce type de fichiers.
Nous pouvons nous permettre de faire cela car la quantité de donnée n’est pas grande. 
##Amélioration
Nous n’avons pas l’année du vin, nous faisons donc une moyenne pour les notes et aussi pour le rayonnement de soleil. 
Nous pensons qu’il aurait été bien d’avoir ces informations pour être plus précis. 
Il serait donc intéressant de trouver d’autres donnée pour avoir l’année du vin et son évaluation.

## Conclusion des résulats
Il est difficile de dire que le soleil à une grande importance, sauf s’il y a trop de soleil.

### Matching regions with solar radiation

# Divers information on wine

## Data structur

root  
 |-- country: string (nullable = true)  
 |-- description: string (nullable = true)  
 |-- designation: string (nullable = true)  
 |-- points: integer (nullable = true)  
 |-- price: double (nullable = true)  
 |-- province: string (nullable = true)  
 |-- region_1: string (nullable = true)  
 |-- region_2: string (nullable = true)  
 |-- variety: string (nullable = true)  
 |-- winery: string (nullable = true)  

## With the max point
|country  |points|province  |price|radiation|
|---------|------|----------|-----|---------|
|US       |100   |Oregon    |65.0 |12.66    |
|US       |100   |Washington|80.0 |16.56    |
|US       |100   |Washington|80.0 |16.56    |
|US       |100   |California|100.0|18.57    |
|France   |100   |Bordeaux  |150.0|15.81    |
|Italy    |100   |Tuscany   |195.0|19.14    |
|US       |100   |California|200.0|18.57    |
|Italy    |100   |Tuscany   |210.0|19.14    |
|US       |100   |California|215.0|18.57    |
|US       |100   |California|245.0|18.57    |
|France   |100   |Champagne |250.0|12.03    |
|France   |100   |Champagne |259.0|12.03    |
|Italy    |100   |Tuscany   |270.0|15.23    |
|Australia|100   |Victoria  |300.0|18.35    |
|Australia|100   |Victoria  |350.0|18.35    |
|France   |100   |Bordeaux  |359.0|15.81    |
|Portugal |100   |Douro     |450.0|14.81    |
|US       |100   |California|450.0|18.57    |
|Italy    |100   |Tuscany   |460.0|14.46    |
|Italy    |100   |Tuscany   |550.0|15.23    |

## With the min point
|country  |province        |radiation|variety           |nbTest|
|---------|----------------|---------|------------------|------|
|Argentina|Mendoza Province|21.75    |Malbec            |23    |
|Argentina|Mendoza Province|21.75    |Cabernet Sauvignon|12    |
|Argentina|Mendoza Province|21.75    |Chardonnay        |10    |
|US       |California      |18.57    |Zinfandel         |10    |
|US       |California      |18.57    |Zinfandel         |10    |
|US       |California      |18.57    |Cabernet Sauvignon|9     |
|US       |California      |18.57    |Red Blend         |8     |
|Spain    |Northern Spain  |14.49    |Tempranillo       |8     |
|US       |California      |18.57    |Merlot            |7     |
|US       |California      |18.57    |Zinfandel         |7     |
|US       |California      |18.35    |Zinfandel         |7     |
|Spain    |Catalonia       |13.87    |Sparkling Blend   |7     |
|Spain    |Central Spain   |19.14    |Tempranillo       |6     |
|US       |California      |18.57    |Chardonnay        |6     |
|Argentina|Mendoza Province|21.75    |Merlot            |6     |
|Spain    |Northern Spain  |14.49    |Viura             |6     |
|US       |California      |18.35    |Cabernet Sauvignon|5     |
|Chile    |Central Valley  |21.75    |Sauvignon Blanc   |5     |
|US       |California      |18.35    |Petite Sirah      |5     |
|US       |California      |18.57    |Cabernet Sauvignon|5     |

## Points vs radiation
|points|nbTested|max_radiation|min_radiation|stddev_radiation|avg_radiation|
|------|--------|-------------|-------------|----------------|-------------|
|80    |766     |21.75        |10.65        |2.84            |18.00        |
|81    |1269    |21.75        |9.54         |3.00            |17.62        |
|82    |3316    |22.63        |9.54         |2.70            |17.56        |
|83    |5180    |22.63        |9.54         |2.94            |17.25        |
|84    |9903    |22.63        |9.54         |2.90            |17.04        |
|85    |13027   |22.63        |9.54         |2.95            |16.64        |
|86    |16653   |22.63        |9.54         |2.90            |16.61        |
|87    |22138   |22.63        |9.54         |2.84            |16.52        |
|88    |21566   |22.63        |9.54         |2.86            |16.26        |
|89    |15517   |22.63        |9.54         |2.96            |15.96        |
|90    |19405   |22.63        |9.54         |2.89            |16.15        |
|91    |13879   |22.63        |9.54         |2.86            |16.08        |
|92    |11798   |21.75        |9.54         |2.87            |16.12        |
|93    |8046    |21.75        |9.54         |2.89            |15.93        |
|94    |4613    |21.75        |9.54         |2.91            |15.81        |
|95    |2068    |21.75        |9.54         |2.92            |15.62        |
|96    |754     |21.75        |10.26        |2.90            |15.71        |
|97    |339     |21.75        |9.54         |2.83            |15.78        |
|98    |121     |19.14        |10.65        |2.61            |16.00        |
|99    |42      |19.14        |12.03        |2.69            |15.67        |
|100   |26      |19.14        |12.03        |2.69            |16.05        |


## Country with the most radiation
|country               |radiation|nbTest|
|----------------------|---------|------|
|Egypt                 |21.88    |3     |
|Peru                  |21.75    |16    |
|Argentina             |19.96    |5491  |
|India                 |19.42    |9     |
|US-France             |19.14    |1     |
|South Korea           |19.14    |2     |
|Cyprus                |18.89    |19    |
|Israel                |18.89    |596   |
|Morocco               |18.78    |26    |
|South Africa          |18.75    |2192  |
|Mexico                |18.67    |89    |
|Australia             |18.21    |4407  |
|Chile                 |17.62    |6134  |
|US                    |17.50    |71732 |
|Lebanon               |17.38    |42    |
|Brazil                |17.35    |57    |
|Tunisia               |17.26    |2     |
|Portugal              |16.69    |6913  |
|Uruguay               |16.47    |142   |
|Montenegro            |16.16    |1     |
|Greece                |16.00    |750   |
|Spain                 |15.98    |8832  |
|Italy                 |15.93    |25012 |
|Turkey                |15.77    |88    |
|China                 |15.13    |3     |
|Japan                 |15.13    |1     |
|Romania               |15.02    |156   |
|Bulgaria              |14.82    |151   |
|Switzerland           |14.76    |7     |
|Albania               |14.41    |1     |
|New Zealand           |13.92    |2589  |
|France                |13.79    |27223 |
|Serbia                |13.72    |11    |
|Macedonia             |13.28    |18    |
|Moldova               |13.19    |77    |
|Armenia               |13.10    |2     |
|Slovenia              |12.76    |95    |
|Croatia               |12.55    |94    |
|Canada                |12.54    |274   |
|Georgia               |12.42    |86    |
|Bosnia and Herzegovina|12.25    |3     |
|Slovakia              |12.18    |2     |
|Austria               |12.07    |3966  |
|Hungary               |11.81    |210   |
|England               |11.74    |64    |
|Germany               |11.54    |2794  |
|Czech Republic        |11.51    |14    |
|Ukraine               |11.01    |17    |
|Luxembourg            |10.68    |8     |
|Lithuania             |10.13    |4     |

## Variety th most tested

|variety                      |radiation|nbTested|avg_points|max_radiation|min_radiation|stddev_radiation|max_price|min_price|max_points|min_points|stddev_points|stddev_price|
|-----------------------------|---------|--------|----------|-------------|-------------|----------------|---------|---------|----------|----------|-------------|------------|
|Pinot Noir                   |16.08    |16887   |89.18     |21.75        |9.54         |3.00            |2500.0   |5.0      |100       |80        |3.23         |45.24       |
|Chardonnay                   |16.64    |15706   |88.12     |22.63        |9.54         |3.04            |2013.0   |4.0      |100       |80        |3.31         |42.68       |
|Cabernet Sauvignon           |18.06    |13349   |88.39     |22.63        |9.54         |2.12            |625.0    |4.0      |100       |80        |3.44         |39.35       |
|Red Blend                    |16.71    |11272   |88.25     |22.63        |9.54         |2.42            |500.0    |5.0      |100       |80        |2.91         |30.57       |
|Bordeaux-style Red Blend     |15.12    |9034    |89.23     |21.75        |9.94         |2.91            |3300.0   |7.0      |100       |80        |3.20         |90.97       |
|Sauvignon Blanc              |16.82    |6879    |87.14     |22.63        |10.65        |3.15            |140.0    |5.0      |96        |80        |2.73         |9.57        |
|Riesling                     |13.33    |6630    |89.10     |21.75        |9.54         |3.04            |775.0    |5.0      |98        |80        |2.97         |41.07       |
|Syrah                        |17.46    |5889    |88.90     |21.75        |11.15        |2.14            |750.0    |4.0      |100       |80        |3.27         |29.24       |
|Merlot                       |17.38    |4781    |86.80     |22.63        |11.01        |2.31            |625.0    |4.0      |100       |80        |3.09         |29.28       |
|Rosé                         |16.06    |3926    |86.70     |22.63        |9.54         |2.45            |800.0    |5.0      |96        |80        |2.57         |15.78       |
|Zinfandel                    |18.64    |3869    |87.32     |21.75        |12.66        |1.06            |100.0    |4.0      |97        |80        |3.27         |12.04       |
|Sangiovese                   |15.91    |3650    |88.34     |21.75        |12.64        |1.95            |800.0    |6.0      |100       |80        |2.83         |39.08       |
|Malbec                       |18.35    |3459    |87.78     |22.63        |11.15        |3.64            |400.0    |4.0      |97        |80        |3.14         |26.28       |
|Nebbiolo                     |14.41    |3127    |90.32     |21.75        |12.52        |0.87            |595.0    |12.0     |99        |80        |2.72         |45.96       |
|White Blend                  |16.24    |3111    |87.26     |22.63        |9.54         |2.65            |375.0    |4.0      |97        |80        |2.81         |16.85       |
|Portuguese Red               |16.27    |2902    |88.81     |19.14        |14.34        |1.32            |450.0    |4.0      |100       |80        |2.96         |24.62       |
|Sparkling Blend              |16.08    |2637    |87.89     |22.63        |9.54         |2.76            |250.0    |5.0      |98        |80        |2.97         |20.90       |
|Tempranillo                  |16.09    |2505    |87.13     |21.75        |11.16        |1.76            |600.0    |4.0      |97        |80        |3.20         |35.40       |
|Rhône-style Red Blend        |16.53    |1872    |88.88     |20.64        |11.15        |2.08            |500.0    |7.0      |98        |80        |2.87         |23.56       |
|Champagne Blend              |12.75    |1815    |89.65     |22.63        |11.15        |2.02            |600.0    |7.0      |100       |80        |3.07         |62.58       |
|Pinot Gris                   |13.79    |1753    |88.21     |22.63        |9.54         |2.63            |269.0    |7.0      |97        |80        |2.70         |16.03       |
|Shiraz                       |18.20    |1681    |88.51     |22.63        |11.16        |1.48            |850.0    |5.0      |99        |80        |3.22         |57.16       |
|Cabernet Franc               |16.31    |1673    |87.93     |21.75        |11.15        |2.58            |180.0    |9.0      |98        |80        |2.85         |20.81       |
|Grüner Veltliner             |12.10    |1515    |89.83     |19.14        |10.73        |1.91            |1100.0   |9.0      |96        |82        |2.38         |34.44       |
|Bordeaux-style White Blend   |14.49    |1446    |89.07     |21.75        |12.33        |2.55            |1000.0   |6.0      |100       |80        |3.41         |83.48       |
|Pinot Grigio                 |15.59    |1433    |86.03     |22.63        |9.54         |2.95            |70.0     |4.0      |92        |80        |2.11         |6.63        |
|Viognier                     |17.31    |1424    |87.51     |21.88        |11.15        |2.09            |140.0    |5.0      |96        |80        |2.94         |14.62       |
|Portuguese White             |16.09    |1313    |87.04     |19.14        |14.34        |1.33            |95.0     |5.0      |95        |80        |2.44         |9.45        |
|Gewürztraminer               |13.67    |1281    |88.31     |21.75        |9.54         |2.89            |210.0    |6.0      |96        |80        |2.86         |17.99       |
|Corvina, Rondinella, Molinara|18.88    |1263    |88.60     |19.14        |12.89        |1.24            |535.0    |8.0      |96        |80        |2.62         |44.36       |


## Witch is best variety

Variety                             | Points mean | Points stddev| Price mean|Price min|Price max|Price stddev|Number tested|Row number
----------------------------------- | ----------- | ------------- | ---------- | --------- | --------- | ------------ | -------------|---------- 
|Picolit                            |90.94      |2.41         |72.00     |19.0     |230.0    |41.94       |33           |1         |
|Tokaji                             |90.71      |2.15         |74.04     |13.0     |544.0    |78.08       |51           |2         |
|Sangiovese Grosso                  |90.45      |2.58         |65.13     |12.0     |900.0    |44.19       |1088         |3         |
|Scheurebe                          |90.39      |3.66         |43.19     |12.0     |125.0    |33.98       |33           |4         |
|Nebbiolo                           |90.32      |2.72         |66.97     |12.0     |595.0    |45.96       |3127         |5         |
|Tannat-Cabernet                    |90.27      |2.54         |20.15     |11.0     |65.0     |11.71       |33           |6         |
|Zibibbo                            |90.18      |3.05         |37.34     |19.0     |60.0     |11.89       |38           |7         |
|Austrian white blend               |90.18      |2.50         |31.98     |11.0     |120.0    |20.92       |79           |8         |
|Provence red blend                 |90.13      |2.69         |30.61     |13.0     |66.0     |12.68       |55           |9         |
|Baga                               |90.12      |1.98         |37.73     |9.0      |80.0     |21.07       |33           |10        |
|Blaufränkisch                      |89.98      |2.63         |32.84     |9.0      |129.0    |21.98       |281          |11        |
|Petit Manseng                      |89.98      |3.11         |30.56     |11.0     |128.0    |16.65       |63           |12        |
|Alsace white blend                 |89.86      |3.39         |33.00     |10.0     |98.0     |23.05       |70           |13        |
|Tinto Fino                         |89.86      |3.55         |65.48     |10.0     |450.0    |73.89       |110          |14        |
|Grüner Veltliner                   |89.83      |2.38         |28.30     |9.0      |1100.0   |34.44       |1515         |15        |
|Gros and Petit Manseng             |89.81      |1.79         |20.95     |11.0     |39.0     |5.67        |53           |16        |
|Carricante                         |89.73      |2.00         |39.55     |19.0     |195.0    |28.58       |41           |17        |
|Encruzado                          |89.72      |1.73         |22.91     |12.0     |60.0     |10.99       |39           |18        |
|Nerello Mascalese                  |89.67      |2.67         |40.04     |10.0     |225.0    |28.36       |135          |19        |
|Sherry                             |89.65      |2.46         |31.97     |7.0      |170.0    |31.37       |123          |20        |
|Champagne Blend                    |89.65      |3.07         |71.54     |7.0      |600.0    |62.58       |1815         |21        |
|Pedro Ximénez                      |89.63      |3.13         |31.24     |9.0      |258.0    |39.56       |57           |22        |
|Sagrantino                         |89.62      |2.52         |53.01     |21.0     |135.0    |21.72       |128          |23        |
|Rotgipfler                         |89.59      |1.82         |26.69     |19.0     |42.0     |8.69        |22           |24        |
|Albana                             |89.57      |2.51         |25.65     |8.0      |66.0     |14.36       |30           |25        |
|Syrah-Cabernet Sauvignon           |89.51      |2.71         |44.89     |12.0     |85.0     |16.51       |37           |26        |
|Austrian Red Blend                 |89.46      |2.68         |35.82     |14.0     |115.0    |20.12       |79           |27        |
|St. Laurent                        |89.46      |2.29         |32.83     |10.0     |90.0     |18.26       |110          |28        |
|Savagnin                           |89.43      |2.19         |42.12     |15.0     |90.0     |21.03       |23           |29        |
|Shiraz-Grenache                    |89.41      |2.04         |24.70     |11.0     |65.0     |15.37       |27           |30        |
|Furmint                            |89.40      |2.59         |44.76     |14.0     |764.0    |null        |55           |31        |
|Carignano                          |89.36      |2.11         |36.78     |11.0     |91.0     |21.87       |44           |32        |
|Cabernet Franc-Merlot              |89.33      |2.82         |45.55     |22.0     |125.0    |23.34       |21           |33        |
|Tinta de Toro                      |89.31      |3.57         |46.84     |7.0      |250.0    |51.16       |223          |34        |
|Welschriesling                     |89.30      |4.13         |32.55     |9.0      |105.0    |22.94       |37           |35        |
|Syrah-Petite Sirah                 |89.24      |4.00         |39.52     |24.0     |85.0     |13.36       |21           |36        |
|Bordeaux-style Red Blend           |89.23      |3.20         |47.86     |7.0      |3300.0   |90.97       |9034         |37        |
|Shiraz-Viognier                    |89.19      |3.20         |44.24     |10.0     |225.0    |51.17       |74           |38        |
|Pinot Noir                         |89.18      |3.23         |46.53     |5.0      |2500.0   |45.24       |16887        |39        |
|Spätburgunder                      |89.17      |3.30         |58.35     |14.0     |181.0    |35.16       |84           |40        |
|Touriga Nacional Blend             |89.14      |2.29         |28.44     |11.0     |144.0    |32.13       |22           |41        |
|Riesling                           |89.10      |2.97         |31.02     |5.0      |775.0    |41.07       |6630         |42        |
|Cabernet Blend                     |89.09      |3.86         |59.47     |8.0      |500.0    |55.09       |226          |43        |
|Aglianico                          |89.09      |2.49         |37.13     |6.0      |180.0    |22.60       |461          |44        |
|Weissburgunder                     |89.08      |2.16         |25.33     |11.0     |50.0     |8.83        |61           |45        |
|Bordeaux-style White Blend         |89.07      |3.41         |34.14     |6.0      |1000.0   |83.48       |1446         |46        |
|Port                               |89.01      |3.32         |57.22     |11.0     |1000.0   |83.23       |1086         |47        |
|Pinot Meunier                      |88.98      |2.49         |44.13     |9.0      |145.0    |27.15       |49           |48        |
|Alicante Bouschet                  |88.97      |2.66         |30.20     |7.0      |150.0    |27.09       |63           |49        |
|Kerner                             |88.92      |2.02         |23.20     |18.0     |44.0     |5.64        |26           |50        |

## Witch country as most variety 

|Country               |Number tested|Count variety|Points mean|Points stddev|Price mean|Price min|Price max|Price stddev|Row number|
|----------------------|-------------|-------------|-----------|-------------|----------|---------|---------|------------|----------|
|US                    |71732        |274          |88.30      |3.26         |35.56     |4.0      |2013.0   |26.40       |1         |
|Italy                 |25012        |201          |88.50      |2.70         |38.95     |5.0      |900.0    |37.43       |2         |
|France                |27223        |163          |88.87      |3.13         |42.92     |5.0      |3300.0   |74.65       |3         |
|Spain                 |8832         |135          |87.07      |3.14         |28.03     |4.0      |770.0    |34.50       |4         |
|Portugal              |6913         |96           |88.24      |2.99         |26.84     |4.0      |1000.0   |39.44       |5         |
|Australia             |4407         |87           |88.15      |3.03         |32.73     |5.0      |850.0    |43.81       |6         |
|Argentina             |5491         |76           |86.44      |3.19         |23.09     |4.0      |250.0    |22.47       |7         |
|Chile                 |6134         |70           |86.35      |2.73         |20.12     |5.0      |400.0    |20.73       |8         |
|South Africa          |2192         |69           |87.63      |2.52         |22.78     |5.0      |330.0    |18.10       |9         |
|Austria               |3966         |62           |89.93      |2.60         |31.62     |7.0      |1100.0   |26.63       |10        |
|Israel                |596          |53           |88.22      |2.46         |31.62     |8.0      |150.0    |18.85       |11        |
|Greece                |750          |52           |86.80      |2.26         |22.20     |7.0      |120.0    |12.18       |12        |
|New Zealand           |2589         |40           |87.85      |2.50         |25.60     |7.0      |130.0    |15.39       |13        |
|Germany               |2794         |38           |89.33      |2.82         |40.38     |5.0      |775.0    |59.38       |14        |
|Canada                |274          |34           |89.13      |2.50         |35.47     |12.0     |145.0    |20.97       |15        |
|Hungary               |210          |32           |88.46      |3.19         |43.27     |7.0      |764.0    |70.35       |16        |
|Slovenia              |95           |30           |88.20      |1.82         |26.52     |7.0      |90.0     |14.70       |17        |
|Turkey                |88           |25           |88.15      |1.96         |25.86     |14.0     |120.0    |15.47       |18        |
|Bulgaria              |151          |25           |87.50      |2.36         |14.44     |7.0      |100.0    |9.38        |19        |
|Croatia               |94           |24           |87.04      |2.42         |24.49     |12.0     |65.0     |12.58       |20        |
|Mexico                |89           |24           |85.22      |2.75         |27.26     |8.0      |108.0    |16.30       |21        |
|Moldova               |77           |24           |86.04      |2.92         |16.04     |8.0      |42.0     |8.72        |22        |
|Romania               |156          |22           |85.60      |2.13         |14.79     |4.0      |320.0    |26.77       |23        |
|Uruguay               |142          |17           |85.89      |3.02         |25.49     |7.0      |130.0    |17.80       |24        |
|Brazil                |57           |14           |84.42      |2.34         |23.73     |10.0     |60.0     |10.86       |25        |
|Czech Republic        |14           |12           |86.86      |1.56         |21.14     |15.0     |45.0     |9.69        |26        |
|Peru                  |16           |11           |83.56      |1.86         |18.06     |10.0     |68.0     |13.66       |27        |
|Georgia               |86           |11           |87.09      |2.58         |18.83     |9.0      |40.0     |7.65        |28        |
|Morocco               |26           |9            |88.62      |2.30         |19.08     |6.0      |40.0     |6.97        |29        |
|Ukraine               |17           |9            |84.29      |1.53         |9.88      |6.0      |13.0     |2.47        |30        |
|Macedonia             |18           |9            |85.83      |2.48         |15.28     |12.0     |25.0     |3.08        |31        |
|Lebanon               |42           |7            |86.79      |3.06         |30.10     |12.0     |75.0     |16.01       |32        |
|Serbia                |11           |7            |87.64      |1.12         |23.91     |15.0     |42.0     |8.99        |33        |
|Cyprus                |19           |5            |86.32      |2.58         |15.79     |10.0     |22.0     |3.54        |34        |
|Switzerland           |7            |5            |88.14      |2.67         |65.14     |19.0     |160.0    |65.10       |35        |
|Luxembourg            |8            |5            |88.13      |1.25         |29.88     |16.0     |50.0     |10.67       |36        |
|India                 |9            |4            |89.33      |3.24         |14.44     |10.0     |20.0     |4.00        |37        |
|England               |64           |4            |91.75      |1.83         |52.63     |25.0     |95.0     |15.28       |38        |
|China                 |3            |3            |84.33      |4.04         |17.33     |7.0      |27.0     |10.02       |39        |
|Egypt                 |3            |3            |83.67      |0.58         |null      |null     |null     |null        |40        |
|Armenia               |2            |2            |87.50      |0.71         |14.50     |14.0     |15.0     |0.71        |41        |
|Tunisia               |2            |2            |86.00      |1.41         |null      |null     |null     |null        |42        |
|Bosnia and Herzegovina|3            |2            |85.33      |2.52         |12.67     |12.0     |13.0     |0.58        |43        |
|Slovakia              |2            |1            |84.50      |3.54         |15.50     |15.0     |16.0     |0.71        |44        |
|Albania               |1            |1            |88.00      |null         |20.00     |20.0     |20.0     |null        |45        |
|Lithuania             |4            |1            |84.25      |0.50         |10.00     |10.0     |10.0     |0.00        |46        |
|US-France             |1            |1            |88.00      |null         |50.00     |50.0     |50.0     |null        |47        |
|South Korea           |2            |1            |81.50      |0.71         |13.50     |11.0     |16.0     |3.54        |48        |
|Montenegro            |1            |1            |82.00      |null         |10.00     |10.0     |10.0     |null        |49        |
|Japan                 |1            |1            |85.00      |null         |24.00     |24.0     |24.0     |null        |50        |


## In witch country is there the best wine

|Country     |Number tested|Points mean|Points stddev|Points mean|Price min|Price max|Price stddev|Row number|
|------------|-------------|-----------|-------------|-----------|---------|---------|------------|----------|
|England     |64           |91.75      |1.83         |91.75      |25.0     |95.0     |15.28       |1         |
|Austria     |3966         |89.93      |2.60         |89.93      |7.0      |1100.0   |26.63       |2         |
|Germany     |2794         |89.33      |2.82         |89.33      |5.0      |775.0    |59.38       |3         |
|Canada      |274          |89.13      |2.50         |89.13      |12.0     |145.0    |20.97       |5         |
|France      |27223        |88.87      |3.13         |88.87      |5.0      |3300.0   |74.65       |6         |
|Morocco     |26           |88.62      |2.30         |88.62      |6.0      |40.0     |6.97        |7         |
|Italy       |25012        |88.50      |2.70         |88.50      |5.0      |900.0    |37.43       |8         |
|Hungary     |210          |88.46      |3.19         |88.46      |7.0      |764.0    |70.35       |9         |
|US          |71732        |88.30      |3.26         |88.30      |4.0      |2013.0   |26.40       |10        |
|Portugal    |6913         |88.24      |2.99         |88.24      |4.0      |1000.0   |39.44       |11        |
|Israel      |596          |88.22      |2.46         |88.22      |8.0      |150.0    |18.85       |12        |
|Slovenia    |95           |88.20      |1.82         |88.20      |7.0      |90.0     |14.70       |13        |
|Australia   |4407         |88.15      |3.03         |88.15      |5.0      |850.0    |43.81       |14        |
|Turkey      |88           |88.15      |1.96         |88.15      |14.0     |120.0    |15.47       |15        |
|New Zealand |2589         |87.85      |2.50         |87.85      |7.0      |130.0    |15.39       |20        |
|South Africa|2192         |87.63      |2.52         |87.63      |5.0      |330.0    |18.10       |22        |
|Bulgaria    |151          |87.50      |2.36         |87.50      |7.0      |100.0    |9.38        |23        |
|Georgia     |86           |87.09      |2.58         |87.09      |9.0      |40.0     |7.65        |25        |
|Spain       |8832         |87.07      |3.14         |87.07      |4.0      |770.0    |34.50       |26        |
|Croatia     |94           |87.04      |2.42         |87.04      |12.0     |65.0     |12.58       |27        |
|Greece      |750          |86.80      |2.26         |86.80      |7.0      |120.0    |12.18       |29        |
|Lebanon     |42           |86.79      |3.06         |86.79      |12.0     |75.0     |16.01       |30        |
|Argentina   |5491         |86.44      |3.19         |86.44      |4.0      |250.0    |22.47       |31        |
|Chile       |6134         |86.35      |2.73         |86.35      |5.0      |400.0    |20.73       |32        |
|Moldova     |77           |86.04      |2.92         |86.04      |8.0      |42.0     |8.72        |34        |
|Uruguay     |142          |85.89      |3.02         |85.89      |7.0      |130.0    |17.80       |36        |
|Romania     |156          |85.60      |2.13         |85.60      |4.0      |320.0    |26.77       |38        |
|Mexico      |89           |85.22      |2.75         |85.22      |8.0      |108.0    |16.30       |40        |
|Brazil      |57           |84.42      |2.34         |84.42      |10.0     |60.0     |10.86       |43        |

## Witch is the best variety in the top 10 countries

|Country |Variety                      |Points mean|Points stddev|Price mean|Price min|Price max|Number tested|Price stddev|Row number|
|--------|-----------------------------|-----------|-------------|----------|---------|---------|-------------|------------|----------|
|France  |Petit Manseng                |91.70      |1.78         |31.94     |11.0     |128.0    |43           |20.04       |1         |
|England |Sparkling Blend              |91.53      |1.79         |52.38     |25.0     |95.0     |43           |16.06       |2         |
|Italy   |Cabernet Franc               |91.50      |3.82         |85.15     |12.0     |180.0    |40           |39.78       |3         |
|France  |Tannat                       |91.39      |2.25         |35.72     |13.0     |160.0    |61           |28.74       |4         |
|Austria |Riesling                     |91.19      |2.51         |38.31     |10.0     |145.0    |731          |19.64       |5         |
|Italy   |Picolit                      |90.94      |2.41         |72.00     |19.0     |230.0    |33           |41.94       |6         |
|Hungary |Tokaji                       |90.71      |2.15         |74.04     |13.0     |544.0    |51           |78.08       |7         |
|Italy   |Sangiovese Grosso            |90.46      |2.58         |65.16     |12.0     |900.0    |1087         |44.21       |8         |
|Italy   |Nebbiolo                     |90.38      |2.68         |67.83     |12.0     |595.0    |3055         |46.24       |9         |
|France  |Tannat-Cabernet              |90.27      |2.54         |20.15     |11.0     |65.0     |33           |11.71       |10        |
|Austria |White Blend                  |90.19      |2.95         |36.54     |10.0     |160.0    |106          |28.56       |11        |
|Austria |Austrian white blend         |90.18      |2.50         |31.98     |11.0     |120.0    |79           |20.92       |12        |
|Italy   |Zibibbo                      |90.18      |3.05         |37.34     |19.0     |60.0     |38           |11.89       |13        |
|Austria |Chardonnay                   |90.15      |2.77         |35.55     |12.0     |111.0    |67           |23.76       |14        |
|France  |Provence red blend           |90.13      |2.69         |30.61     |13.0     |66.0     |55           |12.68       |15        |
|Portugal|Baga                         |90.12      |1.98         |37.73     |9.0      |80.0     |33           |21.07       |16        |
|Austria |Blaufränkisch                |90.12      |2.60         |33.32     |9.0      |129.0    |264          |22.52       |17        |
|Austria |Welschriesling               |90.00      |3.99         |35.39     |9.0      |105.0    |32           |23.77       |18        |
|France  |Champagne Blend              |89.98      |2.87         |78.16     |9.0      |600.0    |1608         |63.91       |19        |
|France  |Riesling                     |89.97      |3.16         |32.19     |9.0      |120.0    |847          |17.50       |20        |
|Austria |Grüner Veltliner             |89.95      |2.38         |28.98     |9.0      |1100.0   |1399         |35.96       |21        |
|France  |Alsace white blend           |89.86      |3.39         |33.00     |10.0     |98.0     |70           |23.05       |22        |
|France  |Gros and Petit Manseng       |89.81      |1.79         |20.95     |11.0     |39.0     |53           |5.67        |23        |
|US      |Syrah-Cabernet Sauvignon     |89.78      |2.87         |48.67     |32.0     |85.0     |27           |14.29       |24        |
|Austria |Pinot Noir                   |89.76      |2.64         |37.66     |14.0     |89.0     |130          |16.34       |25        |
|Italy   |Carricante                   |89.73      |2.00         |39.55     |19.0     |195.0    |41           |28.58       |26        |
|Portugal|Encruzado                    |89.72      |1.73         |22.91     |12.0     |60.0     |39           |10.99       |27        |
|Italy   |Sagrantino                   |89.70      |2.51         |53.41     |21.0     |135.0    |124          |21.97       |28        |
|France  |Pinot Noir                   |89.67      |3.17         |82.08     |5.0      |2500.0   |2425         |null        |29        |
|Italy   |Nerello Mascalese            |89.67      |2.67         |40.04     |10.0     |225.0    |135          |28.36       |30        |
|Italy   |Albana                       |89.66      |2.51         |26.24     |8.0      |66.0     |29           |14.33       |31        |
|US      |Cabernet Blend               |89.64      |4.51         |71.67     |8.0      |500.0    |118          |65.63       |32        |
|Austria |Red Blend                    |89.62      |2.12         |35.42     |11.0     |106.0    |126          |18.39       |33        |
|Austria |Rotgipfler                   |89.59      |1.82         |26.69     |19.0     |42.0     |22           |8.69        |34        |
|Germany |Riesling                     |89.58      |2.76         |42.09     |5.0      |775.0    |2344         |63.37       |35        |
|US      |Bordeaux-style Red Blend     |89.57      |3.14         |58.19     |7.0      |500.0    |2305         |46.24       |36        |
|France  |Marsanne                     |89.52      |3.18         |59.21     |10.0     |260.0    |56           |49.77       |37        |
|Austria |St. Laurent                  |89.51      |2.30         |32.98     |10.0     |90.0     |107          |18.43       |38        |
|France  |Malbec                       |89.49      |2.96         |34.49     |7.0      |400.0    |505          |38.50       |39        |
|Hungary |Furmint                      |89.47      |2.47         |46.98     |14.0     |764.0    |51           |null        |40        |
|US      |Cabernet Sauvignon-Syrah     |89.46      |3.46         |42.64     |10.0     |100.0    |95           |19.79       |41        |
|Austria |Austrian Red Blend           |89.46      |2.68         |35.82     |14.0     |115.0    |79           |20.12       |42        |
|France  |Pinot Gris                   |89.45      |2.95         |35.33     |11.0     |269.0    |509          |25.81       |43        |
|US      |Semillon-Sauvignon Blanc     |89.43      |3.52         |32.94     |16.0     |85.0     |51           |20.17       |44        |
|France  |Syrah                        |89.40      |3.05         |62.96     |7.0      |450.0    |369          |66.93       |45        |
|Italy   |Merlot                       |89.40      |3.61         |80.36     |8.0      |625.0    |224          |null        |46        |
|US      |Pinot Noir                   |89.38      |3.21         |43.66     |6.0      |275.0    |12538        |18.53       |47        |
|France  |Chardonnay                   |89.37      |3.17         |61.26     |6.0      |1400.0   |3485         |79.02       |48        |
|Italy   |Carignano                    |89.36      |2.11         |36.78     |11.0     |91.0     |44           |21.87       |49        |
|Portugal|Alicante Bouschet            |89.34      |2.55         |31.09     |7.0      |150.0    |50           |29.90       |50        |



## Witch is variety the most tested in country

|Country     |Variety                      |Number tested|Price mean|Price min|Price max|Price stddev|Row number|
|------------|-----------------------------|-------------|----------|---------|---------|------------|----------|
|US          |Pinot Noir                   |12538        |43.66     |6.0      |275.0    |18.53       |1         |
|US          |Cabernet Sauvignon           |10057        |52.75     |4.0      |625.0    |40.44       |2         |
|US          |Chardonnay                   |8935         |29.42     |4.0      |2013.0   |25.99       |3         |
|France      |Bordeaux-style Red Blend     |6175         |41.58     |7.0      |3300.0   |null        |4         |
|US          |Syrah                        |4506         |36.70     |5.0      |750.0    |20.64       |5         |
|Italy       |Red Blend                    |4280         |40.02     |6.0      |500.0    |37.17       |6         |
|US          |Zinfandel                    |3861         |28.21     |4.0      |100.0    |12.01       |7         |
|US          |Red Blend                    |3852         |33.27     |5.0      |290.0    |21.78       |8         |
|US          |Merlot                       |3498         |27.74     |4.0      |200.0    |15.79       |9         |
|France      |Chardonnay                   |3485         |61.26     |6.0      |1400.0   |79.02       |10        |
|Italy       |Nebbiolo                     |3055         |67.83     |12.0     |595.0    |46.24       |11        |
|Italy       |Sangiovese                   |3028         |45.71     |6.0      |800.0    |42.26       |12        |
|US          |Sauvignon Blanc              |3009         |19.81     |5.0      |90.0     |7.70        |13        |
|Portugal    |Portuguese Red               |2902         |24.78     |4.0      |450.0    |24.62       |14        |
|France      |Pinot Noir                   |2425         |82.08     |5.0      |2500.0   |null        |15        |
|Germany     |Riesling                     |2344         |42.09     |5.0      |775.0    |63.37       |16        |
|US          |Bordeaux-style Red Blend     |2305         |58.19     |7.0      |500.0    |46.24       |17        |
|Argentina   |Malbec                       |2137         |26.55     |4.0      |225.0    |25.67       |18        |
|US          |Riesling                     |2084         |18.99     |5.0      |200.0    |9.50        |19        |
|Spain       |Tempranillo                  |2036         |30.79     |4.0      |600.0    |38.72       |20        |
|France      |Rosé                         |2015         |19.39     |7.0      |800.0    |21.22       |21        |
|France      |Champagne Blend              |1608         |78.16     |9.0      |600.0    |63.91       |22        |
|Austria     |Grüner Veltliner             |1399         |28.98     |9.0      |1100.0   |35.96       |23        |
|France      |Bordeaux-style White Blend   |1387         |34.43     |6.0      |1000.0   |86.61       |24        |
|Portugal    |Portuguese White             |1313         |15.45     |5.0      |95.0     |9.45        |25        |
|Italy       |Corvina, Rondinella, Molinara|1263         |47.25     |8.0      |535.0    |44.36       |26        |
|Australia   |Shiraz                       |1247         |43.41     |5.0      |850.0    |64.36       |27        |
|US          |Cabernet Franc               |1243         |33.92     |10.0     |150.0    |17.86       |28        |
|France      |Sauvignon Blanc              |1163         |25.97     |6.0      |140.0    |14.67       |29        |
|Chile       |Cabernet Sauvignon           |1123         |20.28     |5.0      |400.0    |25.32       |30        |
|US          |Viognier                     |1103         |23.78     |5.0      |100.0    |8.66        |31        |
|Italy       |Sangiovese Grosso            |1087         |65.16     |12.0     |900.0    |44.21       |32        |
|Spain       |Red Blend                    |1081         |36.19     |6.0      |450.0    |34.28       |33        |
|France      |Gamay                        |1079         |20.42     |8.0      |224.0    |10.87       |34        |
|Italy       |White Blend                  |1050         |27.69     |5.0      |375.0    |22.21       |35        |
|US          |Pinot Gris                   |1041         |18.24     |9.0      |65.0     |5.70        |36        |
|US          |Rosé                         |1025         |19.49     |6.0      |150.0    |7.53        |37        |
|US          |Petite Sirah                 |997          |30.46     |8.0      |115.0    |12.63       |38        |
|Portugal    |Port                         |969          |60.86     |11.0     |1000.0   |89.07       |39        |
|France      |Rhône-style Red Blend        |962          |31.53     |7.0      |500.0    |29.36       |40        |
|New Zealand |Sauvignon Blanc              |888          |18.16     |7.0      |75.0     |6.32        |41        |
|Chile       |Sauvignon Blanc              |884          |13.70     |5.0      |40.0     |4.83        |42        |
|France      |Riesling                     |847          |32.19     |9.0      |120.0    |17.50       |43        |
|US          |Rhône-style Red Blend        |838          |36.05     |9.0      |125.0    |15.23       |44        |
|US          |Sparkling Blend              |837          |35.73     |5.0      |250.0    |24.86       |45        |
|Italy       |Glera                        |830          |20.01     |5.0      |75.0     |7.38        |46        |
|Italy       |Pinot Grigio                 |813          |17.36     |5.0      |70.0     |7.04        |47        |
|Spain       |Tempranillo Blend            |810          |35.45     |7.0      |450.0    |38.24       |48        |
|US          |White Blend                  |804          |21.41     |4.0      |85.0     |11.20       |49        |
|Argentina   |Cabernet Sauvignon           |794          |18.27     |5.0      |230.0    |16.01       |50        |


## Witch country has the best variety ( top 20 )

|Country |Variety               |Points mean|Points stddev|Price mean|Price min|Price max|Price stddev|Number tested|Row number|
|--------|----------------------|-----------|-------------|----------|---------|---------|------------|-------------|----------|
|France  |Petit Manseng         |91.70      |1.78         |31.94     |11.0     |128.0    |20.04       |43           |1         |
|Italy   |Picolit               |90.94      |2.41         |72.00     |19.0     |230.0    |41.94       |33           |2         |
|Hungary |Tokaji                |90.71      |2.15         |74.04     |13.0     |544.0    |78.08       |51           |3         |
|Italy   |Sangiovese Grosso     |90.46      |2.58         |65.16     |12.0     |900.0    |44.21       |1087         |4         |
|Italy   |Nebbiolo              |90.38      |2.68         |67.83     |12.0     |595.0    |46.24       |3055         |5         |
|France  |Tannat-Cabernet       |90.27      |2.54         |20.15     |11.0     |65.0     |11.71       |33           |6         |
|Austria |Austrian white blend  |90.18      |2.50         |31.98     |11.0     |120.0    |20.92       |79           |7         |
|Italy   |Zibibbo               |90.18      |3.05         |37.34     |19.0     |60.0     |11.89       |38           |8         |
|France  |Provence red blend    |90.13      |2.69         |30.61     |13.0     |66.0     |12.68       |55           |9         |
|Portugal|Baga                  |90.12      |1.98         |37.73     |9.0      |80.0     |21.07       |33           |10        |
|Austria |Blaufränkisch         |90.12      |2.60         |33.32     |9.0      |129.0    |22.52       |264          |11        |
|Austria |Grüner Veltliner      |89.95      |2.38         |28.98     |9.0      |1100.0   |35.96       |1399         |12        |
|France  |Alsace white blend    |89.86      |3.39         |33.00     |10.0     |98.0     |23.05       |70           |13        |
|Spain   |Tinto Fino            |89.86      |3.55         |65.48     |10.0     |450.0    |73.89       |110          |14        |
|France  |Gros and Petit Manseng|89.81      |1.79         |20.95     |11.0     |39.0     |5.67        |53           |15        |
|Italy   |Carricante            |89.73      |2.00         |39.55     |19.0     |195.0    |28.58       |41           |16        |
|Portugal|Encruzado             |89.72      |1.73         |22.91     |12.0     |60.0     |10.99       |39           |17        |
|Spain   |Sherry                |89.69      |2.42         |32.41     |7.0      |170.0    |31.79       |119          |18        |
|Italy   |Nerello Mascalese     |89.67      |2.67         |40.04     |10.0     |225.0    |28.36       |135          |19        |
|US      |Grüner Veltliner      |88.43      |1.89         |21.72     |12.0     |40.0     |6.12        |101          |20        |
|US      |Nebbiolo              |88.13      |3.66         |36.95     |15.0     |90.0     |16.78       |61           |21        |




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
