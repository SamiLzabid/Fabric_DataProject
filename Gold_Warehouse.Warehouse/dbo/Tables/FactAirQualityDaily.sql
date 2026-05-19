CREATE TABLE [dbo].[FactAirQualityDaily] (

	[DateKey] date NULL, 
	[timezone] varchar(8000) NULL, 
	[locality] varchar(8000) NULL, 
	[location_name] varchar(8000) NULL, 
	[Pollutant] varchar(8000) NULL, 
	[AvgValue] decimal(10,2) NULL, 
	[ReadingCount] int NULL
);