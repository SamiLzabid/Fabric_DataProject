CREATE TABLE [dbo].[FactTaxiDaily] (

	[DateKey] date NULL, 
	[HourOfDay] int NULL, 
	[pickup_zone] varchar(8000) NULL, 
	[TotalTrips] int NULL, 
	[TotalRevenue] decimal(18,2) NULL, 
	[AvgFare] decimal(10,2) NULL, 
	[AvgTripDistance] decimal(10,2) NULL, 
	[AvgTripDuration] decimal(10,2) NULL
);