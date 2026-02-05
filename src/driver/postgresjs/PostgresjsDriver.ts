import { ObjectLiteral } from "../../common/ObjectLiteral"
import { DataSource } from "../../data-source/DataSource"
import { ConnectionIsNotSetError } from "../../error/ConnectionIsNotSetError"
import { DriverPackageNotInstalledError } from "../../error/DriverPackageNotInstalledError"
import { ColumnMetadata } from "../../metadata/ColumnMetadata"
import { EntityMetadata } from "../../metadata/EntityMetadata"
import { PlatformTools } from "../../platform/PlatformTools"
import { QueryRunner } from "../../query-runner/QueryRunner"
import { RdbmsSchemaBuilder } from "../../schema-builder/RdbmsSchemaBuilder"
import { TableColumn } from "../../schema-builder/table/TableColumn"
import { ApplyValueTransformers } from "../../util/ApplyValueTransformers"
import { DateUtils } from "../../util/DateUtils"
import { OrmUtils } from "../../util/OrmUtils"
import { Driver, ReturningType } from "../Driver"
import { ColumnType } from "../types/ColumnTypes"
import { CteCapabilities } from "../types/CteCapabilities"
import { DataTypeDefaults } from "../types/DataTypeDefaults"
import { MappedColumnTypes } from "../types/MappedColumnTypes"
import { ReplicationMode } from "../types/ReplicationMode"
import { VersionUtils } from "../../util/VersionUtils"
import { PostgresjsConnectionOptions } from "./PostgresjsConnectionOptions"
import { PostgresjsQueryRunner } from "./PostgresjsQueryRunner"
import { DriverUtils } from "../DriverUtils"
import { TypeORMError } from "../../error"
import { Table } from "../../schema-builder/table/Table"
import { View } from "../../schema-builder/view/View"
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey"
import { InstanceChecker } from "../../util/InstanceChecker"
import { UpsertType } from "../types/UpsertType"
import { IndexMetadata } from "../../metadata/IndexMetadata"
import { TableIndex } from "../../schema-builder/table/TableIndex"
import { TableIndexTypes } from "../../schema-builder/options/TableIndexTypes"

/**
 * Driver for PostgreSQL using postgres.js (the 'postgres' npm package).
 * 
 * Postgres.js is a modern, fast PostgreSQL client with:
 * - Tagged template literal queries
 * - Built-in connection pooling
 * - Native async/await support
 * - Automatic type conversion
 */
export class PostgresjsDriver implements Driver {
    // -------------------------------------------------------------------------
    // Public Properties
    // -------------------------------------------------------------------------

    /**
     * TypeORM DataSource this driver belongs to
     */
    connection: DataSource

    /**
     * Postgres.js SQL instance
     * This is the main interface for executing queries
     */
    sql: any

    /**
     * All active query runners created by this driver
     */
    connectedQueryRunners: QueryRunner[] = []

    // -------------------------------------------------------------------------
    // Public Implemented Properties
    // -------------------------------------------------------------------------

    /**
     * Connection options
     */
    options: PostgresjsConnectionOptions

    /**
     * PostgreSQL version string (e.g., "14.5")
     */
    version?: string

    /**
     * Current database name
     */
    database?: string

    /**
     * Current schema name
     */
    schema?: string

    /**
     * Schema used internally by PostgreSQL for object resolution
     */
    searchSchema?: string

    /**
     * Indicates if replication is enabled
     * Note: postgres.js handles multi-host connections internally
     */
    isReplicated = false

    /**
     * Indicates if tree tables are supported
     */
    treeSupport = true

    /**
     * Indicates if transactions are supported
     */
    transactionSupport = "nested" as const

    /**
     * Supported data types by this driver
     */
    supportedDataTypes: ColumnType[] = [
        "int",
        "int2",
        "int4",
        "int8",
        "smallint",
        "integer",
        "bigint",
        "decimal",
        "numeric",
        "real",
        "float",
        "float4",
        "float8",
        "double precision",
        "money",
        "character varying",
        "varchar",
        "character",
        "char",
        "text",
        "citext",
        "hstore",
        "bytea",
        "bit",
        "varbit",
        "bit varying",
        "timetz",
        "timestamptz",
        "timestamp",
        "timestamp without time zone",
        "timestamp with time zone",
        "date",
        "time",
        "time without time zone",
        "time with time zone",
        "interval",
        "bool",
        "boolean",
        "enum",
        "point",
        "line",
        "lseg",
        "box",
        "path",
        "polygon",
        "circle",
        "cidr",
        "inet",
        "macaddr",
        "macaddr8",
        "tsvector",
        "tsquery",
        "uuid",
        "xml",
        "json",
        "jsonb",
        "int4range",
        "int8range",
        "numrange",
        "tsrange",
        "tstzrange",
        "daterange",
        "int4multirange",
        "int8multirange",
        "nummultirange",
        "tsmultirange",
        "tstzmultirange",
        "datemultirange",
        "geometry",
        "geography",
        "cube",
        "ltree",
        "vector",
        "halfvec",
    ]

    /**
     * Returns type of upsert supported by driver
     */
    supportedUpsertTypes: UpsertType[] = ["on-conflict-do-update"]

    /**
     * Spatial column types
     */
    spatialTypes: ColumnType[] = ["geometry", "geography"]

    /**
     * Column types that support length
     */
    withLengthColumnTypes: ColumnType[] = [
        "character varying",
        "varchar",
        "character",
        "char",
        "bit",
        "varbit",
        "bit varying",
        "vector",
        "halfvec",
    ]

    /**
     * Column types that support precision
     */
    withPrecisionColumnTypes: ColumnType[] = [
        "numeric",
        "decimal",
        "interval",
        "time without time zone",
        "time with time zone",
        "timestamp without time zone",
        "timestamp with time zone",
    ]

    /**
     * Column types that support scale
     */
    withScaleColumnTypes: ColumnType[] = ["numeric", "decimal"]

    /**
     * Mapped column types for ORM-specific columns
     */
    mappedDataTypes: MappedColumnTypes = {
        createDate: "timestamp",
        createDateDefault: "now()",
        updateDate: "timestamp",
        updateDateDefault: "now()",
        deleteDate: "timestamp",
        deleteDateNullable: true,
        version: "int4",
        treeLevel: "int4",
        migrationId: "int4",
        migrationName: "varchar",
        migrationTimestamp: "int8",
        cacheId: "int4",
        cacheIdentifier: "varchar",
        cacheTime: "int8",
        cacheDuration: "int4",
        cacheQuery: "text",
        cacheResult: "text",
        metadataType: "varchar",
        metadataDatabase: "varchar",
        metadataSchema: "varchar",
        metadataTable: "varchar",
        metadataName: "varchar",
        metadataValue: "text",
    }

    /**
     * Supported index types
     */
    supportedIndexTypes: TableIndexTypes[] = [
        "brin",
        "btree",
        "gin",
        "gist",
        "hash",
        "spgist",
    ]

    /**
     * Parameter prefix for SQL queries
     */
    parametersPrefix: string = "$"

    /**
     * Default values for column type configurations
     */
    dataTypeDefaults: DataTypeDefaults = {
        character: { length: 1 },
        bit: { length: 1 },
        interval: { precision: 6 },
        "time without time zone": { precision: 6 },
        "time with time zone": { precision: 6 },
        "timestamp without time zone": { precision: 6 },
        "timestamp with time zone": { precision: 6 },
    }

    /**
     * Max alias length in characters
     */
    maxAliasLength = 63

    /**
     * Support for generated columns
     */
    isGeneratedColumnsSupported = false

    /**
     * Common Table Expression (CTE) capabilities
     */
    cteCapabilities: CteCapabilities = {
        enabled: true,
        writable: true,
        requiresRecursiveHint: true,
        materializedHint: true,
    }

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(connection?: DataSource) {
        if (!connection) {
            return
        }

        this.connection = connection
        this.options = connection.options as PostgresjsConnectionOptions

        // Load postgres.js library
        this.initializePostgresjsLibrary()

        // Extract database and schema from options
        this.database = DriverUtils.buildDriverOptions(this.options).database
        this.schema = DriverUtils.buildDriverOptions(this.options).schema
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    /**
     * Establish connection to the database
     */
    async connect(): Promise<void> {
        this.sql = await this.createSqlInstance()

        // Gather initial connection information
        const queryRunner = this.createQueryRunner("master")

        try {
            // Get PostgreSQL version
            if (!this.version) {
                this.version = await queryRunner.getVersion()
            }

            // Get current database
            if (!this.database) {
                this.database = await queryRunner.getCurrentDatabase()
            }

            // Get current schema
            if (!this.searchSchema) {
                this.searchSchema = await queryRunner.getCurrentSchema()
            }

            if (!this.schema) {
                this.schema = this.searchSchema
            }
        } finally {
            await queryRunner.release()
        }
    }

    /**
     * Perform post-connection initialization
     */
    async afterConnect(): Promise<void> {
        const queryRunner = this.createQueryRunner("master")

        try {
            // Install PostgreSQL extensions if needed
            const shouldInstallExtensions =
                this.options.installExtensions !== false

            if (shouldInstallExtensions) {
                const extensionMetadata =
                    await this.checkMetadataForExtensions()
                if (extensionMetadata.hasExtensions) {
                    await this.enableExtensions(extensionMetadata, queryRunner)
                }

                // Install custom extensions
                if (this.options.extensions) {
                    await this.enableCustomExtensions(
                        this.options.extensions,
                        queryRunner,
                    )
                }
            }

            // Check for generated column support (PostgreSQL 12+)
            this.isGeneratedColumnsSupported = VersionUtils.isGreaterOrEqual(
                this.version,
                "12.0",
            )
        } finally {
            await queryRunner.release()
        }
    }

    /**
     * Close database connection
     */
    async disconnect(): Promise<void> {
        if (!this.sql) {
            throw new ConnectionIsNotSetError("postgresjs")
        }

        // Release all active query runners
        while (this.connectedQueryRunners.length > 0) {
            await this.connectedQueryRunners[0].release()
        }

        // Close postgres.js connection
        await this.sql.end({ timeout: 5 })
        this.sql = undefined
    }

    /**
     * Create schema builder
     */
    createSchemaBuilder() {
        return new RdbmsSchemaBuilder(this.connection)
    }

    /**
     * Create query runner
     */
    createQueryRunner(mode: ReplicationMode): QueryRunner {
        return new PostgresjsQueryRunner(this, mode)
    }

    /**
     * Prepare value for persistence
     */
    preparePersistentValue(value: any, columnMetadata: ColumnMetadata): any {
        if (columnMetadata.transformer) {
            value = ApplyValueTransformers.transformTo(
                columnMetadata.transformer,
                value,
            )
        }

        if (value === null || value === undefined) return value

        // Boolean handling
        if (columnMetadata.type === Boolean) {
            return value === true ? 1 : 0
        }

        // Date types
        if (columnMetadata.type === "date") {
            return DateUtils.mixedDateToDateString(value, {
                utc: columnMetadata.utc,
            })
        }

        if (columnMetadata.type === "time") {
            return DateUtils.mixedDateToTimeString(value)
        }

        const timestampTypes = [
            "datetime",
            Date,
            "timestamp",
            "timestamptz",
            "timestamp with time zone",
            "timestamp without time zone",
        ]
        if (timestampTypes.includes(columnMetadata.type as any)) {
            return DateUtils.mixedDateToDate(value)
        }

        // Geometric types
        if (columnMetadata.type === "point") {
            if (
                typeof value === "object" &&
                value.x !== undefined &&
                value.y !== undefined
            ) {
                return `(${value.x},${value.y})`
            }
            return value
        }

        if (columnMetadata.type === "circle") {
            if (
                typeof value === "object" &&
                value.x !== undefined &&
                value.y !== undefined &&
                value.radius !== undefined
            ) {
                return `<(${value.x},${value.y}),${value.radius}>`
            }
            return value
        }

        // JSON types
        if (
            ["json", "jsonb", ...this.spatialTypes].includes(
                columnMetadata.type as any,
            )
        ) {
            return JSON.stringify(value)
        }

        // Vector types
        if (
            columnMetadata.type === "vector" ||
            columnMetadata.type === "halfvec"
        ) {
            if (Array.isArray(value)) {
                return `[${value.join(",")}]`
            }
            return value
        }

        // HStore type
        if (columnMetadata.type === "hstore") {
            if (typeof value === "string") {
                return value
            }
            const quoteString = (val: unknown) => {
                if (val === null || typeof val === "undefined") {
                    return "NULL"
                }
                return `"${`${val}`.replace(/(?=["\\])/g, "\\")}"`
            }
            return Object.keys(value)
                .map((key) => quoteString(key) + "=>" + quoteString(value[key]))
                .join(",")
        }

        // Simple array/JSON
        if (columnMetadata.type === "simple-array") {
            return DateUtils.simpleArrayToString(value)
        }

        if (columnMetadata.type === "simple-json") {
            return DateUtils.simpleJsonToString(value)
        }

        // Cube type
        if (columnMetadata.type === "cube") {
            if (columnMetadata.isArray) {
                return `{${value
                    .map((cube: number[]) => `"(${cube.join(",")})"`)
                    .join(",")}}`
            }
            return `(${value.join(",")})`
        }

        // Ltree type
        if (columnMetadata.type === "ltree") {
            return value
                .split(".")
                .filter(Boolean)
                .join(".")
                .replace(/[\s]+/g, "_")
        }

        // Enum handling
        if (
            (columnMetadata.type === "enum" ||
                columnMetadata.type === "simple-enum") &&
            !columnMetadata.isArray
        ) {
            return "" + value
        }

        return value
    }

    /**
     * Prepare value after hydration from database
     */
    prepareHydratedValue(value: any, columnMetadata: ColumnMetadata): any {
        if (value === null || value === undefined) {
            return columnMetadata.transformer
                ? ApplyValueTransformers.transformFrom(
                      columnMetadata.transformer,
                      value,
                  )
                : value
        }

        // Boolean conversion
        if (columnMetadata.type === Boolean) {
            value = value ? true : false
        }

        // Numeric types
        else if (columnMetadata.type === "bigint") {
            value = String(value)
        }

        // JSON types
        else if (
            columnMetadata.type === "json" ||
            columnMetadata.type === "jsonb"
        ) {
            if (typeof value === "string") {
                try {
                    value = JSON.parse(value)
                } catch (e) {}
            }
        }

        // Simple array/JSON
        else if (columnMetadata.type === "simple-array") {
            value = DateUtils.stringToSimpleArray(value)
        } else if (columnMetadata.type === "simple-json") {
            value = DateUtils.stringToSimpleJson(value)
        }

        // HStore
        else if (columnMetadata.type === "hstore") {
            if (typeof value === "string") {
                value = this.parseHstore(value)
            }
        }

        // Cube
        else if (columnMetadata.type === "cube") {
            if (columnMetadata.isArray) {
                if (typeof value === "string") {
                    value = this.parseCubeArray(value)
                }
            } else {
                if (typeof value === "string") {
                    value = this.parseCube(value)
                }
            }
        }

        // Apply custom transformer
        if (columnMetadata.transformer) {
            value = ApplyValueTransformers.transformFrom(
                columnMetadata.transformer,
                value,
            )
        }

        return value
    }

    /**
     * Replace parameters in SQL query
     */
    escapeQueryWithParameters(
        sql: string,
        parameters: ObjectLiteral,
        nativeParameters: ObjectLiteral,
    ): [string, any[]] {
        const builtParameters: any[] = Object.keys(nativeParameters).map(
            (key) => nativeParameters[key],
        )
        return [sql, builtParameters]
    }

    /**
     * Escape table name (schema + table)
     */
    escape(columnName: string): string {
        return '"' + columnName + '"'
    }

    /**
     * Build full table name with schema
     */
    buildTableName(tableName: string, schema?: string, database?: string): string {
        let tablePath = [tableName]

        if (schema) {
            tablePath.unshift(schema)
        }

        return tablePath.map((part) => this.escape(part)).join(".")
    }

    /**
     * Parse table name into components
     */
    parseTableName(
        target: EntityMetadata | Table | View | TableForeignKey | string,
    ): { database?: string; schema?: string; tableName: string } {
        const driverDatabase = this.database
        const driverSchema = this.schema

        if (InstanceChecker.isTable(target) || InstanceChecker.isView(target)) {
            const parsed = this.parseTableName(target.name)

            return {
                database: target.database || parsed.database || driverDatabase,
                schema: target.schema || parsed.schema || driverSchema,
                tableName: parsed.tableName,
            }
        }

        if (InstanceChecker.isTableForeignKey(target)) {
            const parsed = this.parseTableName(target.referencedTableName)

            return {
                database:
                    target.referencedDatabase ||
                    parsed.database ||
                    driverDatabase,
                schema:
                    target.referencedSchema || parsed.schema || driverSchema,
                tableName: parsed.tableName,
            }
        }

        if (InstanceChecker.isEntityMetadata(target)) {
            return {
                database: target.database || driverDatabase,
                schema: target.schema || driverSchema,
                tableName: target.tableName,
            }
        }

        const parts = target.split(".")

        if (parts.length === 3) {
            return {
                database: parts[0],
                schema: parts[1],
                tableName: parts[2],
            }
        } else if (parts.length === 2) {
            return {
                schema: parts[0],
                tableName: parts[1],
            }
        } else {
            return {
                tableName: target,
            }
        }
    }

    /**
     * Create parameter placeholder (e.g., $1, $2)
     */
    createParameter(parameterName: string, index: number): string {
        return "$" + (index + 1)
    }

    /**
     * Normalize database type
     */
    normalizeType(column: {
        type?: ColumnType
        length?: number | string
        precision?: number | null
        scale?: number
    }): string {
        if (column.type === Number || column.type === "int") {
            return "integer"
        } else if (column.type === String) {
            return "character varying"
        } else if (column.type === Date) {
            return "timestamp without time zone"
        } else if (column.type === Boolean) {
            return "boolean"
        } else if (column.type === "uuid") {
            return "uuid"
        } else if (column.type === "simple-array") {
            return "text"
        } else if (column.type === "simple-json") {
            return "text"
        } else if (column.type === "simple-enum") {
            return "enum"
        }

        return column.type as string || ""
    }

    /**
     * Normalize default value
     */
    normalizeDefault(columnMetadata: ColumnMetadata): string | undefined {
        const defaultValue = columnMetadata.default

        if (defaultValue === null || defaultValue === undefined) {
            return undefined
        }

        if (typeof defaultValue === "number") {
            return "" + defaultValue
        }

        if (typeof defaultValue === "boolean") {
            return defaultValue === true ? "true" : "false"
        }

        if (typeof defaultValue === "function") {
            const functionResult = defaultValue()
            return this.normalizeDatetimeFunction(functionResult)
        }

        if (typeof defaultValue === "string") {
            return this.normalizeDatetimeFunction(defaultValue)
        }

        return defaultValue
    }

    /**
     * Normalize boolean expression
     */
    normalizeIsUnique(column: ColumnMetadata): boolean {
        return column.isUnique || !!column.uniqueConstraint
    }

    /**
     * Get returning strategy based on the column
     */
    getReturningStrategy(): ReturningType {
        return "returning"
    }

    /**
     * Create full column type definition
     */
    createFullType(column: TableColumn): string {
        let type = column.type

        if (column.enum) {
            return (
                column.type +
                '("' +
                column.enum.map((val) => val.replace(/"/g, '""')).join('","') +
                '")'
            )
        }

        if (column.length) {
            type += "(" + column.length + ")"
        } else if (
            column.precision !== null &&
            column.precision !== undefined &&
            column.scale !== null &&
            column.scale !== undefined
        ) {
            type += "(" + column.precision + "," + column.scale + ")"
        } else if (
            column.precision !== null &&
            column.precision !== undefined
        ) {
            type += "(" + column.precision + ")"
        }

        if (column.type === "time without time zone") {
            type = "TIME" + (column.precision ? "(" + column.precision + ")" : "")
        } else if (column.type === "time with time zone") {
            type =
                "TIME" +
                (column.precision ? "(" + column.precision + ")" : "") +
                " WITH TIME ZONE"
        } else if (column.type === "timestamp without time zone") {
            type =
                "TIMESTAMP" +
                (column.precision ? "(" + column.precision + ")" : "")
        } else if (column.type === "timestamp with time zone") {
            type =
                "TIMESTAMP" +
                (column.precision ? "(" + column.precision + ")" : "") +
                " WITH TIME ZONE"
        }

        if (column.isArray) {
            type += " array"
        }

        return type
    }

    /**
     * Create generated column definition
     */
    createGeneratedMap(
        metadata: EntityMetadata,
        insertResult: ObjectLiteral,
        entityIndex: number,
    ) {
        const generatedMap = metadata.generatedColumns.reduce(
            (map, generatedColumn) => {
                const value =
                    insertResult[0] ||
                    (Array.isArray(insertResult)
                        ? insertResult[entityIndex]
                        : insertResult)

                if (!value) return map

                return OrmUtils.mergeDeep(
                    map,
                    generatedColumn.createValueMap(
                        value[generatedColumn.databaseName],
                    ),
                )
            },
            {} as ObjectLiteral,
        )

        return Object.keys(generatedMap).length > 0 ? generatedMap : undefined
    }

    /**
     * Find changed columns between table definitions
     */
    findChangedColumns(
        tableColumns: TableColumn[],
        columnMetadatas: ColumnMetadata[],
    ): ColumnMetadata[] {
        return columnMetadatas.filter((columnMetadata) => {
            const tableColumn = tableColumns.find(
                (c) => c.name === columnMetadata.databaseName,
            )
            if (!tableColumn) return false

            const isColumnChanged =
                tableColumn.name !== columnMetadata.databaseName ||
                tableColumn.type !== this.normalizeType(columnMetadata) ||
                tableColumn.length !== columnMetadata.length?.toString() ||
                tableColumn.precision !== columnMetadata.precision ||
                tableColumn.scale !== columnMetadata.scale ||
                tableColumn.isNullable !== columnMetadata.isNullable ||
                tableColumn.isUnique !==
                    this.normalizeIsUnique(columnMetadata) ||
                tableColumn.isGenerated !== columnMetadata.isGenerated

            return isColumnChanged
        })
    }

    /**
     * Check if column type is JSON
     */
    isReturningSqlSupported(returningType: ReturningType): boolean {
        return returningType === "returning"
    }

    /**
     * Check if full-text search is supported
     */
    isFullTextColumnType(columnType: ColumnType): boolean {
        return columnType === "tsvector" || columnType === "tsquery"
    }

    /**
     * Check if UUID generation is supported
     */
    isUUIDGenerationSupported(): boolean {
        return true
    }

    /**
     * Create UUID generator expression
     */
    createUuidGeneratorExpression(): string {
        if (this.options.uuidExtension === "pgcrypto") {
            return "gen_random_uuid()"
        }
        return "uuid_generate_v4()"
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * Load postgres.js library
     */
    protected initializePostgresjsLibrary(): void {
        try {
            const postgresLibrary =
                this.options.driver || PlatformTools.load("postgres")
            
            // Ensure we have the default export
            this.postgresLib = postgresLibrary.default || postgresLibrary
        } catch (e) {
            throw new DriverPackageNotInstalledError(
                "Postgres.js",
                "postgres",
            )
        }
    }

    /**
     * Postgres.js library reference
     */
    protected postgresLib: any

    /**
     * Create postgres.js SQL instance
     */
    protected async createSqlInstance(): Promise<any> {
        // Build connection configuration
        const config: any = {}

        // Connection string takes precedence
        const connectionUrl = this.options.url

        if (this.options.host) config.host = this.options.host
        if (this.options.port) config.port = this.options.port
        if (this.options.path) config.path = this.options.path
        if (this.options.database) config.database = this.options.database
        
        // Username handling
        const username = this.options.username || this.options.user
        if (username) config.user = username
        
        if (this.options.password) config.password = this.options.password
        if (this.options.ssl !== undefined) config.ssl = this.options.ssl
        if (this.options.max !== undefined) config.max = this.options.max
        if (this.options.idle_timeout !== undefined)
            config.idle_timeout = this.options.idle_timeout
        if (this.options.connect_timeout !== undefined)
            config.connect_timeout = this.options.connect_timeout
        if (this.options.prepare !== undefined)
            config.prepare = this.options.prepare
        if (this.options.target_session_attrs)
            config.target_session_attrs = this.options.target_session_attrs
        if (this.options.fetch_types !== undefined)
            config.fetch_types = this.options.fetch_types
        if (this.options.debug !== undefined) config.debug = this.options.debug
        if (this.options.keep_alive !== undefined)
            config.keep_alive = this.options.keep_alive
        if (this.options.max_lifetime !== undefined)
            config.max_lifetime = this.options.max_lifetime
        if (this.options.transform) config.transform = this.options.transform
        if (this.options.connection) config.connection = this.options.connection
        if (this.options.onclose) config.onclose = this.options.onclose
        if (this.options.onnotice) config.onnotice = this.options.onnotice
        if (this.options.onparameter)
            config.onparameter = this.options.onparameter
        if (this.options.backoff !== undefined)
            config.backoff = this.options.backoff
        if (this.options.types) config.types = this.options.types

        // Create postgres.js SQL instance
        const sql = connectionUrl
            ? this.postgresLib(connectionUrl, config)
            : this.postgresLib(config)

        // Test connection
        await sql`SELECT 1`

        return sql
    }

    /**
     * Check which PostgreSQL extensions are needed
     */
    protected async checkMetadataForExtensions() {
        const hasUuidColumns = this.connection.entityMetadatas.some(
            (metadata) =>
                metadata.generatedColumns.filter(
                    (column) => column.generationStrategy === "uuid",
                ).length > 0,
        )

        const hasCitextColumns = this.connection.entityMetadatas.some(
            (metadata) =>
                metadata.columns.filter((column) => column.type === "citext")
                    .length > 0,
        )

        const hasHstoreColumns = this.connection.entityMetadatas.some(
            (metadata) =>
                metadata.columns.filter((column) => column.type === "hstore")
                    .length > 0,
        )

        const hasCubeColumns = this.connection.entityMetadatas.some(
            (metadata) =>
                metadata.columns.filter((column) => column.type === "cube")
                    .length > 0,
        )

        const hasGeometryColumns = this.connection.entityMetadatas.some(
            (metadata) =>
                metadata.columns.filter((column) =>
                    this.spatialTypes.includes(column.type as any),
                ).length > 0,
        )

        const hasLtreeColumns = this.connection.entityMetadatas.some(
            (metadata) =>
                metadata.columns.filter((column) => column.type === "ltree")
                    .length > 0,
        )

        const hasVectorColumns = this.connection.entityMetadatas.some(
            (metadata) =>
                metadata.columns.some(
                    (column) =>
                        column.type === "vector" || column.type === "halfvec",
                ),
        )

        const hasExclusionConstraints = this.connection.entityMetadatas.some(
            (metadata) => metadata.exclusions.length > 0,
        )

        return {
            hasUuidColumns,
            hasCitextColumns,
            hasHstoreColumns,
            hasCubeColumns,
            hasGeometryColumns,
            hasLtreeColumns,
            hasVectorColumns,
            hasExclusionConstraints,
            hasExtensions:
                hasUuidColumns ||
                hasCitextColumns ||
                hasHstoreColumns ||
                hasGeometryColumns ||
                hasCubeColumns ||
                hasLtreeColumns ||
                hasVectorColumns ||
                hasExclusionConstraints,
        }
    }

    /**
     * Enable required PostgreSQL extensions
     */
    protected async enableExtensions(
        extensionMetadata: any,
        queryRunner: QueryRunner,
    ) {
        const { logger } = this.connection
        const {
            hasUuidColumns,
            hasCitextColumns,
            hasHstoreColumns,
            hasCubeColumns,
            hasGeometryColumns,
            hasLtreeColumns,
            hasVectorColumns,
            hasExclusionConstraints,
        } = extensionMetadata

        if (hasUuidColumns) {
            try {
                const uuidExt = this.options.uuidExtension || "uuid-ossp"
                await queryRunner.query(
                    `CREATE EXTENSION IF NOT EXISTS "${uuidExt}"`,
                )
            } catch (_) {
                logger.log(
                    "warn",
                    `Cannot install UUID extension automatically. Install manually with superuser.`,
                )
            }
        }

        if (hasCitextColumns) {
            try {
                await queryRunner.query(
                    `CREATE EXTENSION IF NOT EXISTS "citext"`,
                )
            } catch (_) {
                logger.log("warn", "Cannot install citext extension")
            }
        }

        if (hasHstoreColumns) {
            try {
                await queryRunner.query(
                    `CREATE EXTENSION IF NOT EXISTS "hstore"`,
                )
            } catch (_) {
                logger.log("warn", "Cannot install hstore extension")
            }
        }

        if (hasCubeColumns) {
            try {
                await queryRunner.query(`CREATE EXTENSION IF NOT EXISTS "cube"`)
            } catch (_) {
                logger.log("warn", "Cannot install cube extension")
            }
        }

        if (hasGeometryColumns) {
            try {
                await queryRunner.query(
                    `CREATE EXTENSION IF NOT EXISTS "postgis"`,
                )
            } catch (_) {
                logger.log("warn", "Cannot install postgis extension")
            }
        }

        if (hasLtreeColumns) {
            try {
                await queryRunner.query(
                    `CREATE EXTENSION IF NOT EXISTS "ltree"`,
                )
            } catch (_) {
                logger.log("warn", "Cannot install ltree extension")
            }
        }

        if (hasVectorColumns) {
            try {
                await queryRunner.query(
                    `CREATE EXTENSION IF NOT EXISTS "vector"`,
                )
            } catch (_) {
                logger.log("warn", "Cannot install vector extension")
            }
        }

        if (hasExclusionConstraints) {
            try {
                await queryRunner.query(
                    `CREATE EXTENSION IF NOT EXISTS "btree_gist"`,
                )
            } catch (_) {
                logger.log("warn", "Cannot install btree_gist extension")
            }
        }
    }

    /**
     * Enable custom extensions
     */
    protected async enableCustomExtensions(
        extensions: string[],
        queryRunner: QueryRunner,
    ) {
        const { logger } = this.connection

        for (const extension of extensions) {
            try {
                await queryRunner.query(
                    `CREATE EXTENSION IF NOT EXISTS "${extension}"`,
                )
            } catch (_) {
                logger.log(
                    "warn",
                    `Extension "${extension}" could not be installed`,
                )
            }
        }
    }

    /**
     * Normalize datetime functions
     */
    protected normalizeDatetimeFunction(value: string): string {
        const upperValue = value.toUpperCase()
        const isDatetimeFunction =
            upperValue.includes("CURRENT_TIMESTAMP") ||
            upperValue.includes("CURRENT_DATE") ||
            upperValue.includes("CURRENT_TIME") ||
            upperValue.includes("LOCALTIMESTAMP") ||
            upperValue.includes("LOCALTIME")

        if (!isDatetimeFunction) {
            return value
        }

        const precision = value.match(/\(\d+\)/)

        if (upperValue.includes("CURRENT_TIMESTAMP")) {
            return precision
                ? `('now'::text)::timestamp${precision[0]} with time zone`
                : "now()"
        } else if (upperValue === "CURRENT_DATE") {
            return "('now'::text)::date"
        } else if (upperValue.includes("CURRENT_TIME")) {
            return precision
                ? `('now'::text)::time${precision[0]} with time zone`
                : "('now'::text)::time with time zone"
        } else if (upperValue.includes("LOCALTIMESTAMP")) {
            return precision
                ? `('now'::text)::timestamp${precision[0]} without time zone`
                : "('now'::text)::timestamp without time zone"
        } else if (upperValue.includes("LOCALTIME")) {
            return precision
                ? `('now'::text)::time${precision[0]} without time zone`
                : "('now'::text)::time without time zone"
        }

        return value
    }

    /**
     * Parse HStore string to object
     */
    protected parseHstore(value: string): Record<string, string | null> {
        const result: Record<string, string | null> = {}
        const regex = /"([^"\\]|\\.)*"=>("([^"\\]|\\.)*"|NULL)/g
        let match: RegExpExecArray | null

        while ((match = regex.exec(value)) !== null) {
            const keyMatch = match[0].match(/^"((?:[^"\\]|\\.)*)"/)
            const valueMatch = match[0].match(/=>"((?:[^"\\]|\\.)*)"$/)
            const nullMatch = match[0].match(/=>NULL$/)

            if (keyMatch) {
                const key = keyMatch[1].replace(/\\"/g, '"').replace(/\\\\/g, "\\")
                if (nullMatch) {
                    result[key] = null
                } else if (valueMatch) {
                    result[key] = valueMatch[1]
                        .replace(/\\"/g, '"')
                        .replace(/\\\\/g, "\\")
                }
            }
        }

        return result
    }

    /**
     * Parse cube string to array
     */
    protected parseCube(value: string): number[] {
        return value
            .replace(/[\(\)]/g, "")
            .split(",")
            .map((v) => parseFloat(v.trim()))
    }

    /**
     * Parse cube array string to nested array
     */
    protected parseCubeArray(value: string): number[][] {
        return value
            .replace(/[{}]/g, "")
            .split('),(')
            .map((cube) => this.parseCube(`(${cube})`))
    }
}
