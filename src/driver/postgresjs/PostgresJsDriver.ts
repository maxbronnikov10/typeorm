import { ObjectLiteral } from "../../common/ObjectLiteral"
import { DataSource } from "../../data-source/DataSource"
import { DriverPackageNotInstalledError } from "../../error/DriverPackageNotInstalledError"
import { PlatformTools } from "../../platform/PlatformTools"
import { QueryRunner } from "../../query-runner/QueryRunner"
import { Driver } from "../Driver"
import { PostgresDriver } from "../postgres/PostgresDriver"
import { PostgresJsConnectionOptions } from "./PostgresJsConnectionOptions"
import { PostgresJsQueryRunner } from "./PostgresJsQueryRunner"
import { ReplicationMode } from "../types/ReplicationMode"

/**
 * Driver for PostgreSQL using postgres.js library for enhanced performance.
 * 
 * This driver provides a performance-optimized alternative to the standard
 * PostgreSQL driver by using the postgres npm package (postgres.js) which offers:
 * - Promise-native API (no callbacks)
 * - Automatic connection pooling  
 * - Prepared statement caching
 * - Reduced memory allocations
 */
export class PostgresJsDriver implements Driver {
    // Delegate to PostgresDriver for PostgreSQL-specific behavior
    private readonly postgresDriver: PostgresDriver
    
    // postgres.js sql function instance
    private sqlInstance: any
    
    connection: DataSource
    options: PostgresJsConnectionOptions

    constructor(dataSource: DataSource) {
        this.connection = dataSource
        this.options = dataSource.options as PostgresJsConnectionOptions
        
        // Use PostgresDriver for PostgreSQL-specific logic
        this.postgresDriver = new PostgresDriver(dataSource)
        
        this.initializePostgresJs()
    }

    // Delegate all PostgreSQL-specific interface members
    get version() { return this.postgresDriver.version }
    set version(v: string | undefined) { this.postgresDriver.version = v }
    
    get database() { return this.postgresDriver.database }
    set database(db: string | undefined) { this.postgresDriver.database = db }
    
    get schema() { return this.postgresDriver.schema }
    set schema(s: string | undefined) { this.postgresDriver.schema = s }
    
    get isReplicated() { return this.postgresDriver.isReplicated }
    get treeSupport() { return this.postgresDriver.treeSupport }
    get transactionSupport() { return this.postgresDriver.transactionSupport }
    get supportedDataTypes() { return this.postgresDriver.supportedDataTypes }
    get supportedUpsertTypes() { return this.postgresDriver.supportedUpsertTypes }
    get dataTypeDefaults() { return this.postgresDriver.dataTypeDefaults }
    get spatialTypes() { return this.postgresDriver.spatialTypes }
    get withLengthColumnTypes() { return this.postgresDriver.withLengthColumnTypes }
    get withPrecisionColumnTypes() { return this.postgresDriver.withPrecisionColumnTypes }
    get withScaleColumnTypes() { return this.postgresDriver.withScaleColumnTypes }
    get mappedDataTypes() { return this.postgresDriver.mappedDataTypes }
    get maxAliasLength() { return this.postgresDriver.maxAliasLength }
    get cteCapabilities() { return this.postgresDriver.cteCapabilities }

    private initializePostgresJs(): void {
        try {
            const postgresLib = this.options.driver || PlatformTools.load("postgres")
            
            const config: any = {
                host: this.options.host,
                port: this.options.port,
                database: this.options.database,
                user: this.options.username,
                password: this.options.password,
                max: this.options.poolSize || 10,
                idle_timeout: this.options.idleTimeoutMillis,
                connect_timeout: this.options.connectTimeoutMS ? Math.floor(this.options.connectTimeoutMS / 1000) : undefined,
                ssl: this.options.ssl,
                prepare: this.options.prepare !== undefined ? this.options.prepare : true,
                onnotice: this.options.logNotifications ? 
                    (notice: any) => this.connection.logger.log("info", notice.message) : 
                    undefined,
            }
            
            if (this.options.url) {
                this.sqlInstance = postgresLib(this.options.url, config)
            } else {
                this.sqlInstance = postgresLib(config)
            }
        } catch (error) {
            throw new DriverPackageNotInstalledError("PostgresJS", "postgres")
        }
    }

    async connect(): Promise<void> {
        // Test connection
        await this.sqlInstance`SELECT 1 as test`
        
        // Get database metadata
        if (!this.version) {
            const versionResult = await this.sqlInstance`SELECT version() as version`
            this.version = versionResult[0].version
        }
        
        if (!this.database) {
            const dbResult = await this.sqlInstance`SELECT current_database() as db`
            this.database = dbResult[0].db
        }
        
        if (!this.schema) {
            const schemaResult = await this.sqlInstance`SELECT current_schema() as schema`
            this.schema = schemaResult[0].schema || "public"
        }
    }

    async afterConnect(): Promise<void> {
        if (this.options.installExtensions !== false) {
            await this.installRequiredExtensions()
        }
    }

    async disconnect(): Promise<void> {
        if (this.sqlInstance) {
            await this.sqlInstance.end({ timeout: 5 })
        }
    }

    createSchemaBuilder() {
        return this.postgresDriver.createSchemaBuilder()
    }

    createQueryRunner(mode: ReplicationMode = "master"): QueryRunner {
        return new PostgresJsQueryRunner(this, this.sqlInstance, mode)
    }

    // Delegate methods to PostgresDriver
    escapeQueryWithParameters(sql: string, parameters: ObjectLiteral, nativeParameters: ObjectLiteral) {
        return this.postgresDriver.escapeQueryWithParameters(sql, parameters, nativeParameters)
    }

    escape(name: string) {
        return this.postgresDriver.escape(name)
    }

    buildTableName(tableName: string, schema?: string, database?: string) {
        return this.postgresDriver.buildTableName(tableName, schema, database)
    }

    parseTableName(target: any) {
        return this.postgresDriver.parseTableName(target)
    }

    preparePersistentValue(value: any, columnMetadata: any) {
        return this.postgresDriver.preparePersistentValue(value, columnMetadata)
    }

    prepareHydratedValue(value: any, columnMetadata: any) {
        return this.postgresDriver.prepareHydratedValue(value, columnMetadata)
    }

    normalizeType(column: any) {
        return this.postgresDriver.normalizeType(column)
    }

    normalizeDefault(columnMetadata: any) {
        return this.postgresDriver.normalizeDefault(columnMetadata)
    }

    normalizeIsUnique(column: any) {
        return this.postgresDriver.normalizeIsUnique(column)
    }

    getColumnLength(column: any) {
        return this.postgresDriver.getColumnLength(column)
    }

    createFullType(column: any) {
        return this.postgresDriver.createFullType(column)
    }

    async obtainMasterConnection(): Promise<[any, Function]> {
        return [this.sqlInstance, async () => {}]
    }

    async obtainSlaveConnection(): Promise<[any, Function]> {
        return this.obtainMasterConnection()
    }

    createGeneratedMap(metadata: any, insertResult: any, entityIndex?: number, entityNum?: number) {
        return this.postgresDriver.createGeneratedMap(metadata, insertResult, entityIndex, entityNum)
    }

    findChangedColumns(tableColumns: any[], columnMetadatas: any[]) {
        return this.postgresDriver.findChangedColumns(tableColumns, columnMetadatas)
    }

    isReturningSqlSupported(returningType: any) {
        return this.postgresDriver.isReturningSqlSupported(returningType)
    }

    isUUIDGenerationSupported() {
        return this.postgresDriver.isUUIDGenerationSupported()
    }

    isFullTextColumnTypeSupported() {
        return this.postgresDriver.isFullTextColumnTypeSupported()
    }

    createParameter(parameterName: string, index: number) {
        return this.postgresDriver.createParameter(parameterName, index)
    }

    private async installRequiredExtensions(): Promise<void> {
        const extensions = this.options.extensions || []
        const uuidExt = this.options.uuidExtension || "uuid-ossp"
        
        for (const ext of extensions) {
            try {
                await this.sqlInstance.unsafe(`CREATE EXTENSION IF NOT EXISTS "${ext}"`)
            } catch (err) {
                this.connection.logger.log("warn", `Could not install extension ${ext}: ${err}`)
            }
        }
        
        // Install UUID extension if needed
        try {
            await this.sqlInstance.unsafe(`CREATE EXTENSION IF NOT EXISTS "${uuidExt}"`)
        } catch (err) {
            // Silent fail - extension may not be needed
        }
    }
}
