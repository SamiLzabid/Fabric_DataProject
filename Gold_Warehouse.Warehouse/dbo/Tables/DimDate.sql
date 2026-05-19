CREATE TABLE [dbo].[DimDate] (

	[DateKey] date NULL, 
	[YearKey] int NULL, 
	[MonthNumber] int NULL, 
	[MonthName] varchar(20) NULL, 
	[Quarter] int NULL, 
	[DayName] varchar(20) NULL, 
	[IsWeekend] int NOT NULL
);