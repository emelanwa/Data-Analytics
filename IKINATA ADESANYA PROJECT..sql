
-- IKINATA ADESANYA REQUIREMENT 1

CREATE TABLE DimCities(
	CityKey INT NOT NULL,
	CityName NVARCHAR(50) NULL,
	StateProvinceCode NVARCHAR(5) NULL,
	StateProvinceName NVARCHAR(50) NULL,
	CountryName NVARCHAR(60) NULL,
	CountryFormalName NVARCHAR(60) NULL
);

CREATE TABLE DimCustomers(
	CustomerKey INT NOT NULL,
	CustomerName NVARCHAR(100) NULL,
	CustomerCategoryName NVARCHAR(50) NULL,
	DeliveryCityName NVARCHAR(50) NULL,
	DeliveryStateProvinceCode NVARCHAR(5) NULL,
	DeliveryCountryName NVARCHAR(50) NULL,
	PostalCityName NVARCHAR(50) NULL,
	PostalStateProvinceCode NVARCHAR(5) NULL,
	PostalCountryName NVARCHAR(50) NULL,
	StartDate DATE NOT NULL,
	EndDate DATE NULL
);

CREATE TABLE FactOrders(
    CustomerKey    INT NOT NULL,
    CityKey        INT NOT NULL,
    ProductKey     INT NOT NULL,
    SalesPersonKey INT NOT NULL,
	DateKey        INT NOT NULL,
	SupplierKey	   INT NOT NULL,
    Quantity       INT NOT NULL,
    UnitPrice      DECIMAL(18,2) NOT NULL,
    TaxRate        DECIMAL(18,3) NOT NULL,
    TotalBeforeTax DECIMAL(18,2) NOT NULL,
    TotalAfterTax  DECIMAL(18,2) NOT NULL
);

CREATE TABLE DimDate(
    DateKey       INT NOT NULL,
    DateValue     DATE NOT NULL,
    Year          SMALLINT NOT NULL,
    Month         TINYINT NOT NULL,
    Day           TINYINT NOT NULL,
    Quarter       TINYINT NOT NULL,
    StartOfMonth  DATE NOT NULL,
    EndOfMonth    DATE NOT NULL,
    MonthName     VARCHAR(10) NOT NULL,
    DayOfWeekName VARCHAR(10) NOT NULL  
);

CREATE TABLE DimProducts(
	ProductKey INT NOT NULL,
	ProductName NVARCHAR(100) NULL,
	ProductColour NVARCHAR(20) NULL,
	ProductBrand NVARCHAR(50) NULL,
	ProductSize NVARCHAR(20) NULL,
	StartDate DATE NOT NULL,
	EndDate DATE NULL
);

CREATE TABLE DimSuppliers(
	SupplierKey INT NOT NULL,
	FullName NVARCHAR(100) NOT NULL,
	PhoneNumber NVARCHAR(20) NOT NULL,
	FaxNumber NVARCHAR(20) NOT NULL,
	WebsiteURL NVARCHAR(256) NOT NULL,
	SupplierReference NVARCHAR(20) NULL,
	BankAccountName NVARCHAR(50) NULL
)

CREATE TABLE DimSalesPeople(
	SalesPersonKey INT NOT NULL,
	FullName NVARCHAR(50) NOT NULL,
	PreferredName NVARCHAR(50) NOT NULL,
	LogonName NVARCHAR(50) NULL,
	PhoneNumber NVARCHAR(20) NULL,
	FaxNumber NVARCHAR(20) NULL,
	EmailAddress NVARCHAR(256) NULL
)

-------------------------------------------------------------------------------
-- Set Up Slowly Changing Dimensions (SCD) for DimSuppliers
ALTER TABLE DimSuppliers
ADD EffectiveDate DATE DEFAULT GETDATE(),
    ExpirationDate DATE DEFAULT '9999-12-31',
    IsCurrent BIT DEFAULT 1

------------------------------------------------------------------------------------
--  Create Indexes for Optimization 
--  Indexes for DimSuppliers
CREATE INDEX idx_DimSuppliers_FullName ON DimSuppliers(FullName)
CREATE INDEX idx_DimSuppliers_SupplierReference ON DimSuppliers(SupplierReference)

-- Indexes for DimCustomers
CREATE INDEX idx_DimCustomers_CustomerName ON DimCustomers(CustomerName)
CREATE INDEX idx_DimCustomers_DeliveryCityName ON DimCustomers(DeliveryCityName)

-- Indexes for DimProducts
CREATE INDEX idx_DimProducts_ProductName ON DimProducts(ProductName)
CREATE INDEX idx_DimProducts_ProductBrand ON DimProducts(ProductBrand)

-- Indexes for DimSalesPeople
CREATE INDEX idx_DimSalesPeople_FullName ON DimSalesPeople(FullName)
CREATE INDEX idx_DimSalesPeople_LogonName ON DimSalesPeople(LogonName)

-- Indexes for DimCities
CREATE INDEX idx_DimCities_CityName ON DimCities(CityName)
CREATE INDEX idx_DimCities_StateProvinceName ON DimCities(StateProvinceName)

-- Indexes for DimDates
CREATE INDEX idx_DimDate_DateValue ON DimDate(DateValue)

-- Indexes for DimFactOrders
CREATE INDEX idx_FactOrders_CustomerKey ON FactOrders(CustomerKey)
CREATE INDEX idx_FactOrders_CityKey ON FactOrders(CityKey)
CREATE INDEX idx_FactOrders_ProductKey ON FactOrders(ProductKey)
CREATE INDEX idx_FactOrders_SalespersonKey ON FactOrders(SalespersonKey)
CREATE INDEX idx_FactOrders_DateKey ON FactOrders(DateKey)
CREATE INDEX idx_FactOrders_SupplierKey ON FactOrders(SupplierKey)

-----------------------------------------------------------------------------------

-- IKINATA ADESANYA REQUIREMENT 2
-- STORED PROCEDURE FOR DATE
CREATE PROCEDURE DimDate_Load
    @DateValue DATE
AS
BEGIN;

    INSERT INTO WWIDM.dbo.DimDate
    SELECT CAST( YEAR(@DateValue) * 10000 + MONTH(@DateValue) * 100 + DAY(@DateValue) AS INT),
           @DateValue,
           YEAR(@DateValue),
           MONTH(@DateValue),
           DAY(@DateValue),
           DATEPART(qq,@DateValue),
           DATEADD(DAY,1,EOMONTH(@DateValue,-1)),
           EOMONTH(@DateValue),
           DATENAME(mm,@DateValue),
           DATENAME(dw,@DateValue);
END

-----------------------------------------------------------------------------------------

-- IKINATA ADESNAYA REQUIREMENT 3
-- CREATING STAGE TABLES

CREATE TABLE dbo.Customers_Stage(
	CustomerName NVARCHAR(100) NOT NULL,
	CustomerCategoryName NVARCHAR(50) NULL,
	DeliveryCityName NVARCHAR(50) NULL,
	DeliveryStateProvinceCode NVARCHAR(5) NULL,
	DeliveryStateProvinceName NVARCHAR(50) NULL,
	DeliveryCountryName NVARCHAR(60) NULL,
	DeliveryFormalName NVARCHAR(60) NULL,
	PostalCityName NVARCHAR(50) NULL,
	PostalStateProvinceCode NVARCHAR(5) NULL,
	PostalStateProvinceName NVARCHAR(50) NULL,
	PostalCountryName NVARCHAR(60) NULL,
	PostalFormalName NVARCHAR(60) NULL
) 

CREATE TABLE Products_Stage(
	ProductName NVARCHAR(100) NOT NULL,
	ProductColour NVARCHAR(20) NULL,
	ProductBrand NVARCHAR(50) NULL,
	ProductSize NVARCHAR(20) NULL
) 

CREATE TABLE SalesPeople_Stage(
	FullName NVARCHAR(50) NOT NULL,
	PreferredName NVARCHAR(50) NOT NULL,
	LogonName NVARCHAR(50) NULL,
	PhoneNumber NVARCHAR(20) NULL,
	FaxNumber NVARCHAR(20) NULL,
	EmailAddress NVARCHAR(256) NULL
) 

CREATE TABLE Suppliers_Stage(
	FullName NVARCHAR(100) NOT NULL,
	SupplierCategoryName NVARCHAR(50) NULL,
	PhoneNumber NVARCHAR(20) NOT NULL,
	FaxNumber NVARCHAR(20) NOT NULL,
	WebsiteURL NVARCHAR(256) NOT NULL,
	SupplierReference NVARCHAR(20) NULL,
	BankAccountName NVARCHAR(50) NULL
) 

CREATE TABLE Orders_Stage(
	OrderDate DATE NOT NULL,
	Quantity INT NULL,
	UnitPrice DECIMAL(18, 2) NULL,
	TaxRate DECIMAL(18, 3) NULL,
	CustomerName NVARCHAR(100) NULL,
	CityName NVARCHAR(50) NULL,
	StateProvinceName NVARCHAR(50) NULL,
	CountryName NVARCHAR(60) NULL,
	StockItemName NVARCHAR(100) NULL,
	LogonName NVARCHAR(50) NULL,
	SupplierName NVARCHAR(100) NULL
) 

--- STORED PROCEDURES FOR THE STAGE TABLES
--  Stored Procedure for Products
CREATE PROCEDURE Products_Extract
AS
BEGIN
    TRUNCATE TABLE Products_Stage

    INSERT INTO Products_Stage (ProductName,
	ProductColour,ProductBrand,ProductSize)
    SELECT  si.StockItemName As ProductName, c.ColorName As ProductColour,si.Brand As ProductBrand,
		si.Size As ProductSize
    FROM 
        WideWorldImporters.Warehouse.StockItems si
        LEFT JOIN WideWorldImporters.Warehouse.Colors c ON si.ColorID = c.ColorID
END


-- Stored Procedure for Salespeople
CREATE PROCEDURE SalesPeople_Extract
AS
BEGIN
    TRUNCATE TABLE Salespeople_Stage;

    INSERT INTO Salespeople_Stage (FullName, PreferredName,
	LogonName, PhoneNumber,FaxNumber,EmailAddress)
    SELECT  p.FullName, p.PreferredName, p.LogonName,p.PhoneNumber,p.FaxNumber, p.EmailAddress
    FROM 
        WideWorldImporters.Application.People p
    WHERE 
        p.IsSalesperson = 1
END

-- Stored Procedure for Suppliers
CREATE PROCEDURE Suppliers_Extract
AS
BEGIN
    TRUNCATE TABLE Suppliers_Stage;

    INSERT INTO Suppliers_Stage (FullName,
	SupplierCategoryName,PhoneNumber,FaxNumber,WebsiteURL,
	SupplierReference,BankAccountName)
    SELECT  s.SupplierName As FullName, sc.SupplierCategoryName,s.PhoneNumber,
		s.FaxNumber,s.WebsiteURL,s.SupplierReference,s.BankAccountName
    FROM 
        WideWorldImporters.Purchasing.Suppliers s
        JOIN WideWorldImporters.Purchasing.SupplierCategories sc ON s.SupplierCategoryID = sc.SupplierCategoryID
END

--  Stored Procedure for Customers
CREATE PROCEDURE Customers_Extract
AS
BEGIN
    TRUNCATE TABLE Customers_Stage

    
    INSERT INTO WWIDM.dbo.Customers_Stage (
        CustomerName,
        CustomerCategoryName,
        DeliveryCityName,
        DeliveryStateProvinceCode,
        DeliveryStateProvinceName,
        DeliveryCountryName,
        DeliveryFormalName,
        PostalCityName,
        PostalStateProvinceCode,
        PostalStateProvinceName,
        PostalCountryName,
        PostalFormalName )
    SELECT c.CustomerName,
           cc.CustomerCategoryName,
           ci.CityName DeliveryCityName,
           sp.StateProvinceCode DeliveryStateProvinceCode,
           sp.StateProvinceName DeliveryStateProvinceName,
           co.CountryName DeliveryCountryName,
           co.FormalName DeliveryFormalName,
           ci.CityName PostalCityName,
           sp.StateProvinceCode PostalStateProvinceCode,
           sp.StateProvinceName PostalStateProvinceName,
           co.CountryName PostalCountryName,
           co.FormalName PostalFormalName
    FROM 
        WideWorldImporters.Sales.Customers c
        JOIN WideWorldImporters.Sales.CustomerCategories cc ON c.CustomerCategoryID = cc.CustomerCategoryID
        JOIN WideWorldImporters.Application.Cities ci ON c.DeliveryCityID = ci.CityID
        JOIN WideWorldImporters.Application.StateProvinces sp ON ci.StateProvinceID = sp.StateProvinceID
        JOIN WideWorldImporters.Application.Countries co ON sp.CountryID = co.CountryID;
END

--  Stored Procedure for Orders
CREATE PROCEDURE Orders_Extract
    @DateValue DATE
AS
BEGIN
    TRUNCATE TABLE Orders_Stage

	INSERT INTO Orders_Stage (OrderDate,Quantity,UnitPrice,TaxRate,CustomerName,
	CityName,StateProvinceName,CountryName,StockItemName,LogonName,
	ps.SupplierName)
	SELECT 	o.OrderDate,ol.Quantity,ol.UnitPrice,ol.TaxRate,c.CustomerName,cc.CityName,
	ss.StateProvinceName,co.CountryName,ws.StockItemName,p.LogonName,
	ps.SupplierName

	FROM 
        WideWorldImporters.Sales.Orders o
        JOIN WideWorldImporters.Sales.OrderLines ol ON o.OrderID = ol.OrderID
        JOIN WideWorldImporters.Sales.Customers c ON o.CustomerID = c.CustomerID
		JOIN WideWorldImporters.Application.Cities cc ON c.LastEditedBy = cc.LastEditedBy
		JOIN WideWorldImporters.Application.StateProvinces ss ON cc.StateProvinceID = ss.StateProvinceID
		JOIN WideWorldImporters.Application.Countries co ON ss.CountryID = co.CountryID
		JOIN WideWorldImporters.Warehouse.StockItems ws ON ol.StockItemID = ws.StockItemID
		JOIN WideWorldImporters.Application.People p ON o.SalespersonPersonID = p.PersonID
		JOIN WideWorldImporters.Purchasing.Suppliers ps ON ws.SupplierID = ps.SupplierID
    WHERE 
        o.OrderDate = @DateValue;
END;

EXECUTE DimDate_Load '2013-01-02'
EXECUTE Customers_Extract
EXECUTE SalesPeople_Extract
EXECUTE Suppliers_Extract
EXECUTE Products_Extract
EXECUTE Orders_Extract '2013-01-02'

----------------------------------------------------------------------------

-- IKINATA ADESANYA REQUIREMENT 4
-- CREATING PRELOAD TABLES

CREATE TABLE Cities_Preload(
	CityKey INT NOT NULL,
	CityName NVARCHAR(50) NULL,
	StateProvinceCode NVARCHAR(5) NULL,
	StateProvinceName NVARCHAR(50) NULL,
	CountryName NVARCHAR(60) NULL,
	CountryFormalName NVARCHAR(60) NULL
)

CREATE TABLE Customers_Preload(
	CustomerKey INT NOT NULL,
	CustomerName NVARCHAR(100) NULL,
	CustomerCategoryName NVARCHAR(50) NULL,
	DeliveryCityName NVARCHAR(50) NULL,
	DeliveryStateProvinceCode NVARCHAR(5) NULL,
	DeliveryCountryName NVARCHAR(50) NULL,
	PostalCityName NVARCHAR(50) NULL,
	PostalStateProvinceCode NVARCHAR(5) NULL,
	PostalCountryName NVARCHAR(50) NULL,
	StartDate DATE NOT NULL,
	EndDate DATE NULL
)

CREATE TABLE Orders_Preload(
    CustomerKey    INT NOT NULL,
    CityKey        INT NOT NULL,
    ProductKey     INT NOT NULL,
	SalesPersonKey INT NOT NULL,
    DateKey        INT NOT NULL,
	SupplierKey	   INT NOT NULL,
    Quantity       INT NOT NULL,
    UnitPrice      DECIMAL(18,2) NOT NULL,
    TaxRate        DECIMAL(18,3) NOT NULL,
    TotalBeforeTax DECIMAL(18,2) NOT NULL,
    TotalAfterTax  DECIMAL(18,2) NOT NULL
)

CREATE TABLE Products_Preload(
	ProductKey INT NOT NULL,
	ProductName NVARCHAR(100) NULL,
	ProductColour NVARCHAR(20) NULL,
	ProductBrand NVARCHAR(50) NULL,
	ProductSize NVARCHAR(20) NULL,
	StartDate DATE NOT NULL,
	EndDate DATE NULL
)

CREATE TABLE Suppliers_Preload(
	SupplierKey INT NOT NULL,
	FullName NVARCHAR(100) NOT NULL,
	PhoneNumber NVARCHAR(20) NOT NULL,
	FaxNumber NVARCHAR(20) NOT NULL,
	WebsiteURL NVARCHAR(256) NOT NULL,
	SupplierReference NVARCHAR(20) NULL,
	BankAccountName NVARCHAR(50) NULL
)
CREATE TABLE SalesPeople_Preload(
	SalesPersonKey INT NOT NULL,
	FullName NVARCHAR(50) NOT NULL,
	PreferredName NVARCHAR(50) NOT NULL,
	LogonName NVARCHAR(50) NULL,
	PhoneNumber NVARCHAR(20) NULL,
	FaxNumber NVARCHAR(20) NULL,
	EmailAddress NVARCHAR(256) NULL
)

-----------------------------------------------------------------------------

--- CREATING STORED PROCEDURES FOR THE TRANSFORMATION OF THE PRELOAD TABLES
CREATE SEQUENCE dbo.CityKey
    START WITH 1
    INCREMENT BY 1;

-- STORED PROCEDURE FOR CITIES
CREATE PROCEDURE dbo.Cities_Transform
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- Truncate the preload table to clear any existing data
    TRUNCATE TABLE dbo.Cities_Preload;

    BEGIN TRANSACTION;

    -- Insert new cities that do not already exist in DimCities
    INSERT INTO dbo.Cities_Preload(CityKey, CityName, StateProvinceCode, StateProvinceName, CountryName, CountryFormalName)
    SELECT NEXT VALUE FOR dbo.CityKey AS CityKey,
           cu.DeliveryCityName,
           cu.DeliveryStateProvinceCode,
           cu.DeliveryStateProvinceName,
           cu.DeliveryCountryName,
           cu.DeliveryFormalName
    FROM dbo.Customers_Stage cu
    WHERE NOT EXISTS ( SELECT 1 
                       FROM dbo.DimCities ci
                       WHERE cu.DeliveryCityName = ci.CityName
                             AND cu.DeliveryStateProvinceName = ci.StateProvinceName
                             AND cu.DeliveryCountryName = ci.CountryName );

    -- Insert existing cities from DimCities into Cities_Preload
    INSERT INTO dbo.Cities_Preload(CityKey, CityName, StateProvinceCode, StateProvinceName, CountryName, CountryFormalName)
    SELECT ci.CityKey,
           cu.DeliveryCityName,
           cu.DeliveryStateProvinceCode,
           cu.DeliveryStateProvinceName,
           cu.DeliveryCountryName,
           cu.DeliveryFormalName
    FROM dbo.Customers_Stage cu
    JOIN dbo.DimCities ci
        ON cu.DeliveryCityName = ci.CityName
        AND cu.DeliveryStateProvinceName = ci.StateProvinceName
        AND cu.DeliveryCountryName = ci.CountryName;

    COMMIT TRANSACTION;
END;
----------------------------------------------------------------------
CREATE SEQUENCE CustomerKey 
  START WITH 1 INCREMENT BY 1;

CREATE PROCEDURE dbo.Customers_Transform
(@StartDate DATE)
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    TRUNCATE TABLE dbo.Customers_Preload;

 --   DECLARE @StartDate DATE = GETDATE();
    DECLARE @EndDate DATE = DATEADD(dd,-1, @STARTDATE);

    BEGIN TRANSACTION;

    -- Add updated records
    INSERT INTO dbo.Customers_Preload (CustomerKey,CustomerName,CustomerCategoryName,DeliveryCityName,
	DeliveryStateProvinceCode,DeliveryCountryName,PostalCityName,PostalStateProvinceCode,PostalCountryName,
	StartDate,EndDate)
    SELECT NEXT VALUE FOR dbo.CustomerKey AS CustomerKey,
           stg.CustomerName,
           stg.CustomerCategoryName,
           stg.DeliveryCityName,
           stg.DeliveryStateProvinceCode,
           stg.DeliveryCountryName,
           stg.PostalCityName,
           stg.PostalStateProvinceCode,
           stg.PostalCountryName,
           @StartDate,
           NULL
    FROM dbo.Customers_Stage stg
    JOIN dbo.DimCustomers cu
        ON stg.CustomerName = cu.CustomerName
        AND cu.EndDate IS NULL
    WHERE stg.CustomerCategoryName <> cu.CustomerCategoryName
          OR stg.DeliveryCityName <> cu.DeliveryCityName
          OR stg.DeliveryStateProvinceCode <> cu.DeliveryStateProvinceCode
          OR stg.DeliveryCountryName <> cu.DeliveryCountryName
          OR stg.PostalCityName <> cu.PostalCityName
          OR stg.PostalStateProvinceCode <> cu.PostalStateProvinceCode
          OR stg.PostalCountryName <> cu.PostalCountryName;

    -- Add existing records, and expire as necessary
    INSERT INTO dbo.Customers_Preload (CustomerKey,CustomerName,CustomerCategoryName,DeliveryCityName,
	DeliveryStateProvinceCode,DeliveryCountryName,PostalCityName,PostalStateProvinceCode,PostalCountryName,
	StartDate,EndDate)
    SELECT cu.CustomerKey,
           cu.CustomerName,
           cu.CustomerCategoryName,
           cu.DeliveryCityName,
           cu.DeliveryStateProvinceCode,
           cu.DeliveryCountryName,
           cu.PostalCityName,
           cu.PostalStateProvinceCode,
           cu.PostalCountryName,
           cu.StartDate,
           CASE 
               WHEN pl.CustomerName IS NULL THEN NULL
               ELSE @EndDate
           END AS EndDate
    FROM dbo.DimCustomers cu
    JOIN dbo.Customers_Preload pl    
        ON pl.CustomerName = cu.CustomerName
        AND cu.EndDate IS NULL;
    
    -- Create new records
    INSERT INTO dbo.Customers_Preload (CustomerKey,CustomerName,CustomerCategoryName,DeliveryCityName,
	DeliveryStateProvinceCode,DeliveryCountryName,PostalCityName,PostalStateProvinceCode,PostalCountryName,
	StartDate,EndDate)
    SELECT NEXT VALUE FOR dbo.CustomerKey AS CustomerKey,
           stg.CustomerName,
           stg.CustomerCategoryName,
           stg.DeliveryCityName,
           stg.DeliveryStateProvinceCode,
           stg.DeliveryCountryName,
           stg.PostalCityName,
           stg.PostalStateProvinceCode,
           stg.PostalCountryName,
           @StartDate,
           NULL
    FROM dbo.Customers_Stage stg
    WHERE NOT EXISTS ( SELECT 1 FROM dbo.DimCustomers cu WHERE stg.CustomerName = cu.CustomerName );

    -- Expire missing records
    INSERT INTO dbo.Customers_Preload (CustomerKey,CustomerName,CustomerCategoryName,DeliveryCityName,
	DeliveryStateProvinceCode,DeliveryCountryName,PostalCityName,PostalStateProvinceCode,PostalCountryName,
	StartDate,EndDate)
    SELECT cu.CustomerKey,
           cu.CustomerName,
           cu.CustomerCategoryName,
           cu.DeliveryCityName,
           cu.DeliveryStateProvinceCode,
           cu.DeliveryCountryName,
           cu.PostalCityName,
           cu.PostalStateProvinceCode,
           cu.PostalCountryName,
           cu.StartDate,
           @EndDate
    FROM dbo.DimCustomers cu
    WHERE NOT EXISTS ( SELECT 1 FROM dbo.Customers_Stage stg WHERE stg.CustomerName = cu.CustomerName )
          AND cu.EndDate IS NULL;

    COMMIT TRANSACTION;
END

-------------------------------------------------------------------------
CREATE SEQUENCE SalesPersonKey 
 START WITH 1 INCREMENT BY 1;

CREATE PROCEDURE dbo.SalesPeople_Transform
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- Truncate the preload table to clear any existing data
    TRUNCATE TABLE dbo.SalesPeople_Preload;

    BEGIN TRANSACTION;

    -- Insert new salespeople that do not already exist in DimSalesPeople
    INSERT INTO dbo.SalesPeople_Preload(SalesPersonKey, FullName, PreferredName, LogonName, PhoneNumber, FaxNumber, EmailAddress)
    SELECT NEXT VALUE FOR dbo.SalesPersonKey AS SalesPersonKey,
           sp.FullName,
           sp.PreferredName,
           sp.LogonName,
           sp.PhoneNumber,
           sp.FaxNumber,
           sp.EmailAddress
    FROM dbo.SalesPeople_Stage sp
    WHERE NOT EXISTS ( SELECT 1 
                       FROM dbo.DimSalesPeople dsp
                       WHERE sp.FullName = dsp.FullName
                             AND sp.LogonName = dsp.LogonName );

    -- Insert existing salespeople from DimSalesPeople into SalesPeople_Preload
    INSERT INTO dbo.SalesPeople_Preload(SalesPersonKey, FullName, PreferredName, LogonName, PhoneNumber, FaxNumber, EmailAddress)
    SELECT dsp.SalesPersonKey,
           sp.FullName,
           sp.PreferredName,
           sp.LogonName,
           sp.PhoneNumber,
           sp.FaxNumber,
           sp.EmailAddress
    FROM dbo.SalesPeople_Stage sp
    JOIN dbo.DimSalesPeople dsp
        ON sp.FullName = dsp.FullName
        AND sp.LogonName = dsp.LogonName;

    COMMIT TRANSACTION;
END;
-------------------------------------------------------------------------------
CREATE SEQUENCE dbo.SupplierKey
  START WITH 1
    INCREMENT BY 1;

CREATE PROCEDURE dbo.Suppliers_Transform
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- Truncate the preload table to clear any existing data
    TRUNCATE TABLE dbo.Suppliers_Preload;

    BEGIN TRANSACTION;

    -- Insert new suppliers that do not already exist in DimSuppliers
    INSERT INTO dbo.Suppliers_Preload(SupplierKey, FullName, PhoneNumber, FaxNumber, WebsiteURL, SupplierReference, BankAccountName)
    SELECT NEXT VALUE FOR dbo.SupplierKey AS SupplierKey,
           s.FullName,
           s.PhoneNumber,
           s.FaxNumber,
           s.WebsiteURL,
           s.SupplierReference,
           s.BankAccountName
    FROM dbo.Suppliers_Stage s
    WHERE NOT EXISTS ( SELECT 1 
                       FROM dbo.DimSuppliers ds
                       WHERE s.FullName = ds.FullName
                             AND s.PhoneNumber = ds.PhoneNumber );

   -- Insert existing suppliers from DimSuppliers into Suppliers_Preload
    INSERT INTO dbo.Suppliers_Preload(SupplierKey, FullName, PhoneNumber, FaxNumber,
	WebsiteURL, SupplierReference, BankAccountName)
    SELECT ds.SupplierKey,
           s.FullName,
           s.PhoneNumber,
           s.FaxNumber,
           s.WebsiteURL,
           s.SupplierReference,
           s.BankAccountName
    FROM dbo.Suppliers_Stage s
    JOIN dbo.DimSuppliers ds
        ON s.FullName = ds.FullName
        AND s.PhoneNumber = ds.PhoneNumber;

    COMMIT TRANSACTION;
END;

-------------------------------------------------------------------------------------
CREATE SEQUENCE dbo.ProductKey
    START WITH 1
    INCREMENT BY 1;

CREATE PROCEDURE dbo.Products_Transform
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- Truncate the preload table to clear any existing data
    TRUNCATE TABLE dbo.Products_Preload;

    BEGIN TRANSACTION;

    -- Insert new products that do not already exist in DimProducts
    INSERT INTO dbo.Products_Preload(ProductKey, ProductName, ProductColour, ProductBrand, ProductSize, StartDate, EndDate)
    SELECT NEXT VALUE FOR dbo.ProductKey AS ProductKey,
           p.ProductName,
           p.ProductColour,
           p.ProductBrand,
           p.ProductSize,
           GETDATE() AS StartDate,
           NULL AS EndDate
    FROM dbo.Products_Stage p
    WHERE NOT EXISTS ( SELECT 1 
                       FROM dbo.DimProducts dp
                       WHERE p.ProductName = dp.ProductName
                             AND p.ProductBrand = dp.ProductBrand );

    -- Insert existing products from DimProducts into Products_Preload
    INSERT INTO dbo.Products_Preload(ProductKey, ProductName, ProductColour, ProductBrand, ProductSize, StartDate, EndDate)
    SELECT dp.ProductKey,
           p.ProductName,
           p.ProductColour,
           p.ProductBrand,
           p.ProductSize,
           dp.StartDate,
           dp.EndDate
    FROM dbo.Products_Stage p
    JOIN dbo.DimProducts dp
        ON p.ProductName = dp.ProductName
        AND p.ProductBrand = dp.ProductBrand;

    COMMIT TRANSACTION;
END;


--------------------------------------------------------------------------
CREATE PROCEDURE dbo.Orders_Transform
AS
BEGIN;

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    TRUNCATE TABLE dbo.Orders_Preload;

    INSERT INTO dbo.Orders_Preload 
	(CustomerKey,
	CityKey,
	ProductKey,
	SalesPersonKey,
	DateKey,
	SupplierKey,
	Quantity,
	UnitPrice,
	TaxRate,
	TotalBeforeTax,
	TotalAfterTax)
    SELECT cu.CustomerKey,
           ci.CityKey,
           pr.ProductKey,
		   sp.SalesPersonKey,
           CAST(YEAR(ord.OrderDate) * 10000 + MONTH(ord.OrderDate) * 100 + DAY(ord.OrderDate) AS INT) AS DateKey,
           su.SupplierKey,
		   (ord.Quantity) AS Quantity,
           (ord.UnitPrice) AS UnitPrice,
           (ord.TaxRate) AS TaxRate,
           (ord.Quantity * ord.UnitPrice) AS TotalBeforeTax,
           (ord.Quantity * ord.UnitPrice * (1 + ord.TaxRate/100)) AS TotalAfterTax
    FROM dbo.Orders_Stage ord
    JOIN dbo.Customers_Preload cu
        ON ord.CustomerName = cu.CustomerName
    JOIN dbo.Cities_Preload ci
        ON ord.CityName = ci.CityName
        AND ord.StateProvinceName = ci.StateProvinceName
        AND ord.CountryName = ci.CountryName
    JOIN dbo.Products_Preload pr
        ON ord.StockItemName = pr.ProductName
	JOIN dbo.SalesPeople_Preload sp
		ON sp.logonName = ord.LogonName  
	JOIN dbo.Suppliers_Preload su
		ON ord.supplierName = su.FullName
END;

EXEC Cities_Transform
EXEC SalesPeople_Transform
EXEC Customers_Transform '2013-01-02'
EXEC Products_Transform 
EXEC Suppliers_Transform
EXEC Orders_Transform

-----------------------------------------------------------------------------------
--- IKINATA ADESANYA REQUIREMENT 5


CREATE PROCEDURE dbo.Customers_Load
AS
BEGIN;

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    -- Delete existing records that are being reloaded
    DELETE cu
    FROM dbo.DimCustomers cu
    JOIN dbo.Customers_Preload pl
        ON cu.CustomerKey = pl.CustomerKey;

    -- Insert new records from the preload table
    INSERT INTO dbo.DimCustomers (
        CustomerKey,
        CustomerName,
        CustomerCategoryName,
        DeliveryCityName,
        DeliveryStateProvinceCode,
        DeliveryCountryName,
        PostalCityName,
        PostalStateProvinceCode,
        PostalCountryName,
        StartDate,
        EndDate
    )
    SELECT 
        pl.CustomerKey,
        pl.CustomerName,
        pl.CustomerCategoryName,
        pl.DeliveryCityName,
        pl.DeliveryStateProvinceCode,
        pl.DeliveryCountryName,
        pl.PostalCityName,
        pl.PostalStateProvinceCode,
        pl.PostalCountryName,
        pl.StartDate,
        pl.EndDate
    FROM dbo.Customers_Preload pl;

    COMMIT TRANSACTION;
END;

-------------------------------------------------------------------------------
CREATE PROCEDURE dbo.DimCities_Load
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    -- Delete existing records that are being reloaded
    DELETE ci
    FROM dbo.DimCities ci
    JOIN dbo.Cities_Preload pl
        ON ci.CityKey = pl.CityKey;

    -- Insert new records from the preload table
    INSERT INTO dbo.DimCities (
        CityKey,
        CityName,
        StateProvinceCode,
        StateProvinceName,
        CountryName,
        CountryFormalName
    )
    SELECT 
        pl.CityKey,
        pl.CityName,
        pl.StateProvinceCode,
        pl.StateProvinceName,
        pl.CountryName,
        pl.CountryFormalName
    FROM dbo.Cities_Preload pl;

    COMMIT TRANSACTION;
END;

-------------------------------------------------------------------------------------
CREATE PROCEDURE dbo.DimProducts_Load
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    -- Delete existing records that are being reloaded
    DELETE dp
    FROM dbo.DimProducts dp
    JOIN dbo.Products_Preload pl
        ON dp.ProductKey = pl.ProductKey;

    -- Insert new records from the preload table
    INSERT INTO dbo.DimProducts (
        ProductKey,
        ProductName,
        ProductColour,
        ProductBrand,
        ProductSize,
        StartDate,
        EndDate
    )
    SELECT 
        pl.ProductKey,
        pl.ProductName,
        pl.ProductColour,
        pl.ProductBrand,
        pl.ProductSize,
        pl.StartDate,
        pl.EndDate
    FROM dbo.Products_Preload pl;

    COMMIT TRANSACTION;
END;

----------------------------------------------------------------------
CREATE PROCEDURE dbo.DimSuppliers_Load
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    -- Delete existing records that are being reloaded
    DELETE ds
    FROM dbo.DimSuppliers ds
    JOIN dbo.Suppliers_Preload pl
        ON ds.SupplierKey = pl.SupplierKey;

    -- Insert new records from the preload table
    INSERT INTO dbo.DimSuppliers (
        SupplierKey,
        FullName,
        PhoneNumber,
        FaxNumber,
        WebsiteURL,
        SupplierReference,
        BankAccountName,
        EffectiveDate,
        ExpirationDate,
        IsCurrent
    )
    SELECT 
        pl.SupplierKey,
        pl.FullName,
        pl.PhoneNumber,
        pl.FaxNumber,
        pl.WebsiteURL,
        pl.SupplierReference,
        pl.BankAccountName,
        GETDATE() AS EffectiveDate,
        '9999-12-31' AS ExpirationDate,
        1 AS IsCurrent
    FROM dbo.Suppliers_Preload pl;

    COMMIT TRANSACTION;
END;

------------------------------------------------------------------------------------
CREATE PROCEDURE dbo.DimSalesPeople_Load
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    -- Delete existing records that are being reloaded
    DELETE sp
    FROM dbo.DimSalesPeople sp
    JOIN dbo.SalesPeople_Preload pl
        ON sp.SalesPersonKey = pl.SalesPersonKey;

    -- Insert new records from the preload table
    INSERT INTO dbo.DimSalesPeople (
        SalesPersonKey,
        FullName,
        PreferredName,
        LogonName,
        PhoneNumber,
        FaxNumber,
        EmailAddress
    )
    SELECT 
        pl.SalesPersonKey,
        pl.FullName,
        pl.PreferredName,
        pl.LogonName,
        pl.PhoneNumber,
        pl.FaxNumber,
        pl.EmailAddress
    FROM dbo.SalesPeople_Preload pl;

    COMMIT TRANSACTION;
END;

-------------------------------------------------------------------------------------
CREATE PROCEDURE dbo.FactOrders_Load
AS
BEGIN;
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    -- Insert new records from the Orders_Preload table into FactOrders
    INSERT INTO dbo.FactOrders (
        CustomerKey,
        CityKey,
        ProductKey,
        SalesPersonKey,
        DateKey,
        SupplierKey,
        Quantity,
        UnitPrice,
        TaxRate,
        TotalBeforeTax,
        TotalAfterTax
    )
    SELECT 
        pl.CustomerKey,
        pl.CityKey,
        pl.ProductKey,
        pl.SalesPersonKey,
        pl.DateKey,
        pl.SupplierKey,
        pl.Quantity,
        pl.UnitPrice,
        pl.TaxRate,
        pl.TotalBeforeTax,
        pl.TotalAfterTax
    FROM dbo.Orders_Preload pl;

    COMMIT TRANSACTION;
END;

----------------------------------------------------------------------------------------
EXEC dbo.DimCities_Load;
EXEC dbo.Customers_Load;
EXEC dbo.DimProducts_Load;
EXEC dbo.DimSuppliers_Load;
EXEC dbo.DimSalesPeople_Load;
EXEC dbo.FactOrders_Load;
--------------------------------------------------------------------------------------------
--- IKINATA ADESANYA REQUIREMENT 6

-- Load data for 2013-01-01 to 2013-01-04
EXEC dbo.DimDate_Load '2013-01-01';
EXEC dbo.DimDate_Load '2013-01-02';
EXEC dbo.DimDate_Load '2013-01-03';
EXEC dbo.DimDate_Load '2013-01-04';

EXEC dbo.Customers_Extract;
EXEC dbo.SalesPeople_Extract;
EXEC dbo.Suppliers_Extract;
EXEC dbo.Products_Extract;

EXEC dbo.Orders_Extract '2013-01-01';
EXEC dbo.Orders_Extract '2013-01-02';
EXEC dbo.Orders_Extract '2013-01-03';
EXEC dbo.Orders_Extract '2013-01-04';

EXEC dbo.Cities_Transform;
EXEC dbo.Customers_Transform '2013-01-01';
EXEC dbo.SalesPeople_Transform;
EXEC dbo.Suppliers_Transform;
EXEC dbo.Products_Transform;
EXEC dbo.Orders_Transform;

EXEC dbo.DimCities_Load;
EXEC dbo.Customers_Load;
EXEC dbo.DimProducts_Load;
EXEC dbo.DimSuppliers_Load;
EXEC dbo.DimSalesPeople_Load;
EXEC dbo.FactOrders_Load;

----------------------------------------------------------------------------
SELECT 
    c.CustomerName,
    ct.CityName,
    sp.FullName AS SalesPersonName,
    pr.ProductName,
    su.FullName AS SupplierName,
    dd.DateValue AS OrderDate,
    fo.Quantity,
    fo.UnitPrice,
    fo.TotalBeforeTax,
    fo.TotalAfterTax,
    (fo.TotalAfterTax - fo.TotalBeforeTax) AS Profit
FROM 
    dbo.FactOrders fo
JOIN dbo.DimCustomers c ON fo.CustomerKey = c.CustomerKey
JOIN dbo.DimCities ct ON fo.CityKey = ct.CityKey
JOIN dbo.DimSalesPeople sp ON fo.SalesPersonKey = sp.SalesPersonKey
JOIN dbo.DimProducts pr ON fo.ProductKey = pr.ProductKey
JOIN dbo.DimSuppliers su ON fo.SupplierKey = su.SupplierKey
JOIN dbo.DimDate dd ON fo.DateKey = dd.DateKey
WHERE 
    dd.DateValue BETWEEN '2013-01-01' AND '2013-01-04'
ORDER BY 
    Profit DESC, 
    pr.ProductName ASC;

/* 
The above query allows the business to quickly identify the products that contributed most to profit during the
specified period, along with the suppliers and salespeople involved. This can help in making decisions related
to inventory management, supplier negotiations, and sales strategies.

The query also provides a comprehensive overview that can be used for forecasting and decision-making,
particularly in understanding product profitability, which is crucial for predicting future trends and
adjusting business strategies accordingly. */
