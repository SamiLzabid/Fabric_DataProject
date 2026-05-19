[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Query = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json")),
  #"Drill down" = Source{1},
  #"Converted to table" = Table.FromList(#"Drill down", Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table", "Column1", {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}, {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded Column1", {{"countryiso3code", type text}, {"date", Int64.Type}, {"value", Int64.Type}, {"decimal", type number}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"obs_status", "unit"}),
  #"Expanded country" = Table.ExpandRecordColumn(#"Removed columns", "country", {"id", "value"}, {"id", "value.1"}),
  #"Expanded indicator" = Table.ExpandRecordColumn(#"Expanded country", "indicator", {"id", "value"}, {"id.1", "value.2"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded indicator", {{"id.1", "indicator_id"}, {"value.2", "indicator_value"}, {"id", "country_id"}, {"value.1", "country_value"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"indicator_id", type text}, {"indicator_value", type text}, {"country_id", type text}, {"country_value", type text}})
in
  #"Changed column type 1";
shared Query_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "fe4aac4e-7569-4ec8-b8f2-0200e2c8ff25"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "5415b106-597c-4a98-906c-b8b15e7ef384"]}[Data],
  TableNavigation = Navigation_2{[Id = "WB_USA_GDP", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "D USD EUR SP00_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"D USD EUR SP00" = let
  Source = Csv.Document(Web.Contents("https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata"), [Delimiter = ",", Columns = 32, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"KEY", type text}, {"FREQ", type text}, {"CURRENCY", type text}, {"CURRENCY_DENOM", type text}, {"EXR_TYPE", type text}, {"EXR_SUFFIX", type text}, {"TIME_PERIOD", type date}, {"OBS_VALUE", type number}, {"OBS_STATUS", type text}, {"OBS_CONF", type text}, {"OBS_PRE_BREAK", type text}, {"OBS_COM", type text}, {"TIME_FORMAT", type duration}, {"BREAKS", type text}, {"COLLECTION", type text}, {"COMPILING_ORG", type text}, {"DISS_ORG", type text}, {"DOM_SER_IDS", type text}, {"PUBL_ECB", type text}, {"PUBL_MU", type text}, {"PUBL_PUBLIC", type text}, {"UNIT_INDEX_BASE", type text}, {"COMPILATION", type text}, {"COVERAGE", type text}, {"DECIMALS", Int64.Type}, {"NAT_TITLE", type text}, {"SOURCE_AGENCY", type text}, {"SOURCE_PUB", type text}, {"TITLE", type text}, {"TITLE_COMPL", type text}, {"UNIT", type text}, {"UNIT_MULT", Int64.Type}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"OBS_PRE_BREAK", "OBS_COM", "OBS_CONF", "TIME_FORMAT", "COMPILING_ORG", "DISS_ORG", "DOM_SER_IDS", "PUBL_ECB", "PUBL_MU", "PUBL_PUBLIC", "COMPILATION", "COVERAGE", "SOURCE_PUB", "KEY", "BREAKS", "NAT_TITLE"})
in
  #"Removed columns";
shared #"D USD EUR SP00_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "fe4aac4e-7569-4ec8-b8f2-0200e2c8ff25"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "5415b106-597c-4a98-906c-b8b15e7ef384"]}[Data],
  TableNavigation = Navigation_2{[Id = "ECB_dailyFX", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
