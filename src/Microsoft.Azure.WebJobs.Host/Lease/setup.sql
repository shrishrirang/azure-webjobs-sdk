IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'functions')
BEGIN
	EXEC('CREATE SCHEMA [functions]');
END
GO

IF OBJECT_ID(N'[functions].[Leases]', 'U') IS NULL
BEGIN
    CREATE TABLE [functions].[Leases]
    (
        [LeaseName] [nvarchar](127) NOT NULL,
        [RequestorName] [nvarchar](127) NOT NULL,
        [LastRenewal] [datetime2](7) NOT NULL,
        [HasLease] [bit] NOT NULL,
        [Metadata] [nvarchar](max) NULL,
        CONSTRAINT [PK_Leases] PRIMARY KEY CLUSTERED
        (
            [LeaseName] ASC,
            [RequestorName] ASC
        )
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
    )
END
GO

IF OBJECT_ID('[functions].[leases_tryAcquireOrRenew]') IS NULL
BEGIN
    EXEC('CREATE PROCEDURE [functions].[leases_tryAcquireOrRenew] AS SET NOCOUNT ON');
END
GO

ALTER PROCEDURE [functions].[leases_tryAcquireOrRenew]
@LeaseName NVARCHAR (127), @RequestorName NVARCHAR (127), @Metadata NVARCHAR(MAX), @LeaseExpirationTimeSpan INT, @HasLease BIT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE  [functions].[Leases] SET [LastRenewal] = CURRENT_TIMESTAMP WHERE [LeaseName] = @LeaseName AND [RequestorName] = @RequestorName
	IF @@ROWCOUNT = 0
        INSERT INTO [functions].[Leases] ([LeaseName], [RequestorName], [Metadata], [LastRenewal], [HasLease])
        VALUES (@LeaseName, @RequestorName, @Metadata, CURRENT_TIMESTAMP, 0)

    BEGIN TRANSACTION

    UPDATE [functions].[Leases]
    SET [HasLease] = 0
    WHERE [LeaseName] = @LeaseName AND [HasLease] = 1 AND [RequestorName] <> @RequestorName AND DATEDIFF(SECOND, [LastRenewal], CURRENT_TIMESTAMP) > @LeaseExpirationTimeSpan

    IF NOT EXISTS (SELECT * FROM [functions].[Leases] WHERE [LeaseName] = @LeaseName AND [HasLease] = 1)
        UPDATE [functions].[Leases]
        SET [HasLease] = 1
        WHERE [LeaseName] = @LeaseName AND [RequestorName] = @RequestorName

    COMMIT TRANSACTION

    SELECT @HasLease = [HasLease] FROM [functions].[Leases] WHERE [LeaseName] = @LeaseName AND [RequestorName] = @RequestorName
END
GO

IF OBJECT_ID('[functions].[leases_release]') IS NULL
BEGIN
    EXEC('CREATE PROCEDURE [functions].[leases_release] AS SET NOCOUNT ON');
END
GO

ALTER PROCEDURE [functions].[leases_release]
@LeaseName NVARCHAR (127), @RequestorName NVARCHAR (127)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [functions].[Leases] SET [HasLease] = 0 WHERE [LeaseName] = @LeaseName AND [RequestorName] = @RequestorName
END
GO

IF OBJECT_ID('[functions].[leases_updateMetadata]') IS NULL
BEGIN
    EXEC('CREATE PROCEDURE [functions].[leases_updateMetadata] AS SET NOCOUNT ON');
END
GO

ALTER PROCEDURE [functions].[leases_updateMetadata]
@LeaseName NVARCHAR (127), @RequestorName NVARCHAR (127), @Metadata NVARCHAR(MAX), @HasLease BIT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRANSACTION

	-- Assume lease is valid if HasLease = 1. Ignore lease duration.
    UPDATE  [functions].[Leases] SET [Metadata] = @Metadata
	WHERE [LeaseName] = @LeaseName AND [RequestorName] = @RequestorName AND [HasLease] = 1

    SELECT @HasLease = [HasLease] FROM [functions].[Leases] WHERE [LeaseName] = @LeaseName AND [RequestorName] = @RequestorName

    COMMIT TRANSACTION

END
GO

IF OBJECT_ID('[functions].[leases_getMetadata]') IS NULL
BEGIN
    EXEC('CREATE PROCEDURE [functions].[leases_getMetadata] AS SET NOCOUNT ON');
END
GO

ALTER PROCEDURE [functions].[leases_getMetadata]
@LeaseName NVARCHAR (127), @RequestorName NVARCHAR (127), @LeaseExpirationTimeSpan INT
AS
BEGIN
    SET NOCOUNT ON;

	-- Assume lease is valid if HasLease = 1. Ignore lease duration.
    SELECT [Metadata] FROM [functions].[Leases] 
	WHERE [LeaseName] = @LeaseName AND [RequestorName] = @RequestorName AND [HasLease] = 1 AND DATEDIFF(SECOND, [LastRenewal], CURRENT_TIMESTAMP) <= @LeaseExpirationTimeSpan
END
GO

