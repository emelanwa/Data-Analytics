/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [Employee_Name]
      ,[EmpID]
      ,[Salary]
      ,[Position]
      ,[State]
      ,[Zip]
      ,[DOB]
      ,[Sex]
      ,[MaritalDesc]
      ,[CitizenDesc]
      ,[HispanicLatino]
      ,[RaceDesc]
      ,[DateofHire]
      ,[DateofTermination]
      ,[TermReason]
      ,[EmploymentStatus]
      ,[Department]
      ,[ManagerName]
      ,[ManagerID]
      ,[RecruitmentSource]
      ,[PerformanceScore]
      ,[LastPerformanceReview_Date]
      ,[Absences]
  FROM [ANALYSIS].[dbo].[HR]

  /* COMPENSATION RATIO*/
  SELECT (MAX(SALARY) / MIN(SALARY)) AS [COMPENSATION RATIO ] FROM HR

  /* ACTIVE EMPLOYEES*/
  SELECT COUNT(*) AS [ACTIVE EMPLOYEE] FROM HR
  WHERE EMPLOYMENTSTATUS = 'ACTIVE'

  /* NON ACTIVE EMPLOYEES*/
 SELECT COUNT(*) AS [ACTIVE EMPLOYEE] FROM HR
  WHERE EMPLOYMENTSTATUS <> 'ACTIVE'

  /* REV. PER EMPLOYEE*/
 SELECT SUM(SALARY) / COUNT(*) AS [REV. PER EMPLOYEE] FROM HR

 /* ATTRITION RATE*/
  SELECT 
  COUNT(CASE WHEN EMPLOYMENTSTATUS <> 'Active' THEN 1 END) AS [NON ACTIVE EMPLOYEE],
  COUNT(CASE WHEN EMPLOYMENTSTATUS = 'Active' THEN 1 END) AS [ACTIVE EMPLOYEE],
  (COUNT(CASE WHEN EMPLOYMENTSTATUS <> 'Active' THEN 1 END) * 100.0 / COUNT(*)) AS [ATTRITION RATE]
  FROM HR

  /* EMPLOYEE TURNOVER*/
  SELECT
  COUNT(CASE WHEN EMPLOYMENTSTATUS <> 'Active' THEN 1 END)/ COUNT(*) AS [EMPLOYEE TURNOVER]
  FROM HR

  /* TOTAL EMPLOYEES*/
  SELECT COUNT(*) AS [TOTAL EMPLOYEES] FROM HR

  /* SUM OF SALARY*/
  SELECT SUM(SALARY) AS [SUM OF SALARY] FROM HR

  /*MINIMUM SALARY*/
  SELECT MIN(SALARY) AS [MINIMUM SALARY] FROM HR

    /*MAXIMUM SALARY*/
  SELECT MAX(SALARY) AS [MAXIMUM SALARY] FROM HR

  /*COUNT OF MALE*/
  SELECT COUNT(*) AS [TOTAL COUNT OF MALE] FROM HR
  WHERE Sex = 'M'

    /*COUNT OF FEMALE*/
  SELECT COUNT(*) AS [TOTAL COUNT OF FEMALE] FROM HR
  WHERE Sex = 'F'
