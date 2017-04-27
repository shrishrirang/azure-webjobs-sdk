-- Copyright (c) .NET Foundation. All rights reserved.
-- Licensed under the MIT License. See License.txt in the project root for license information.

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'functions')
    EXEC sys.sp_executesql N'CREATE SCHEMA [functions] AUTHORIZATION [dbo]'
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
        [LeaseExpirationTimeSpan] [int] NULL, -- unit is seconds
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
@LeaseName NVARCHAR (127), @RequestorName NVARCHAR (127), @LeaseExpirationTimeSpan INT, @HasLease BIT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRANSACTION

    UPDATE  [functions].[Leases] SET [LastRenewal] = CURRENT_TIMESTAMP, [LeaseExpirationTimeSpan] = @LeaseExpirationTimeSpan WHERE [LeaseName] = @LeaseName AND [RequestorName] = @RequestorName
    IF @@ROWCOUNT = 0
        INSERT INTO [functions].[Leases] ([LeaseName], [RequestorName], [LeaseExpirationTimeSpan], [LastRenewal], [HasLease])
        VALUES (@LeaseName, @RequestorName, @LeaseExpirationTimeSpan, CURRENT_TIMESTAMP, 0)

    COMMIT TRANSACTION
    
    UPDATE [functions].[Leases]
    SET [HasLease] = 0
    WHERE [LeaseName] = @LeaseName AND [HasLease] = 1 AND [RequestorName] <> @RequestorName AND DATEDIFF(SECOND, [LastRenewal], CURRENT_TIMESTAMP) > @LeaseExpirationTimeSpan

    BEGIN TRANSACTION
    
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
@LeaseName NVARCHAR (127), @RequestorName NVARCHAR (127), @Metadata NVARCHAR(MAX), @Successful BIT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRANSACTION

    UPDATE [functions].[Leases]
    SET [Metadata] = @Metadata
    WHERE [LeaseName] = @LeaseName AND [RequestorName] = @RequestorName AND [HasLease] = 1 AND DATEDIFF(SECOND, [LastRenewal], CURRENT_TIMESTAMP) <= [LeaseExpirationTimeSpan]

    -- COMMIT TRANSACTION will reset @@ROWCOUNT to 0, so we need to use it within the transaction
    IF @@ROWCOUNT = 1
        SET @Successful = 1
    ELSE
        SET @Successful = 0

    COMMIT TRANSACTION
END
GO

IF OBJECT_ID('[functions].[leases_getMetadata]') IS NULL
BEGIN
    EXEC('CREATE PROCEDURE [functions].[leases_getMetadata] AS SET NOCOUNT ON');
END
GO

ALTER PROCEDURE [functions].[leases_getMetadata]
@LeaseName NVARCHAR (127), @RequestorName NVARCHAR (127), @Metadata NVARCHAR(MAX) OUTPUT, @HasLease BIT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT @HasLease = [HasLease] FROM [functions].[Leases] WHERE [LeaseName] = @LeaseName AND [RequestorName] = @RequestorName AND DATEDIFF(SECOND, [LastRenewal], CURRENT_TIMESTAMP) <= [LeaseExpirationTimeSpan]

	-- If no row is selected, HasLease won't have any value assigned. Set it to 0.
	IF @HasLease <> 1
		SET @HasLease = 0
	
	-- Don't care whether the lease is active or expired
    SELECT @Metadata = [Metadata] FROM [functions].[Leases] 
	WHERE [LeaseName] = @LeaseName AND [RequestorName] = @RequestorName
END
GO
