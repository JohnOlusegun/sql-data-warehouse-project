
-- All numeric format specifiers can be used in FORMAT SQL Function	
SELECT 
    'N' AS FormatType, 
    FORMAT(1234.56, 'N') AS FormattedValue, 
    'Numeric_Default' AS Description
UNION ALL
SELECT 
    'P' AS FormatType, 
    FORMAT(1234.56, 'P') AS FormattedValue, 
    'Percentage'
UNION ALL
SELECT 
    'C' AS FormatType, 
    FORMAT(1234.56, 'C') AS FormattedValue, 
    'Currency'
UNION ALL
SELECT 
    'E' AS FormatType, 
    FORMAT(1234.56, 'E') AS FormattedValue, 
    'Scientific_Notation'
UNION ALL
SELECT 
    'F' AS FormatType, 
    FORMAT(1234.56, 'F') AS FormattedValue, 
    'Fixed_point'
UNION ALL
SELECT 
    'N0' AS FormatType, 
    FORMAT(1234.56, 'N0') AS FormattedValue, 
    'Numeric no decimals'
UNION ALL
SELECT 
    'N1' AS FormatType, 
    FORMAT(1234.56, 'N1') AS FormattedValue, 
    'Numeric one Decimal'
UNION ALL
SELECT 
    'N2' AS FormatType, 
    FORMAT(1234.56, 'N2') AS FormattedValue, 
    'Numeric two Decimals'
UNION ALL
SELECT 
    'N_de-DE' AS FormatType, 
    FORMAT(1234.56, 'N', 'de-DE') AS FormattedValue, 
    'Numeric German format'
UNION ALL
SELECT 
    'N_en-US' AS FormatType, 
    FORMAT(1234.56, 'N', 'en-US') AS FormattedValue, 
    'Numeric US format';
