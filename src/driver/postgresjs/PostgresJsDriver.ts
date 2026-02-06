import { ObjectLiteral } from "../../common/ObjectLiteral"
import { DataSource } from "../../data-source/DataSource"
import { ConnectionIsNotSetError } from "../../error/ConnectionIsNotSetError"
import { DriverPackageNotInstalledError } from "../../error/DriverPackageNotInstalledError"
import { TypeORMError } from "../../error/TypeORMError"
import { ColumnMetadata } from "../../metadata/ColumnMetadata"
import { EntityMetadata } from "../../metadata/EntityMetadata"
import { IndexMetadata } from "../../metadata/IndexMetadata"
import { PlatformTools } from "../../platform/PlatformTools"
import { QueryRunner } from "../../query-runner/QueryRunner"
import { RdbmsSchemaBuilder } from "../../schema-builder/RdbmsSchemaBuilder"
import { TableIndexTypes } from "../../schema-builder/options/TableIndexTypes"
import { TableColumn } from "../../schema-builder/table/TableColumn"
import { Table } from "../../schema-builder/table/Table"
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey"
import { View } from "../../schema-builder/view/View"
import { ApplyValueTransformers } from "../../util/ApplyValueTransformers"
import { DateUtils } from "../../util/DateUtils"
import { DriverUtils } from "../DriverUtils"
import { InstanceChecker } from "../../util/InstanceChecker"
import { OrmUtils } from "../../util/OrmUtils"
import { VersionUtils } from "../../util/VersionUtils"
import { Driver, ReturningType } from "../Driver"
import { ColumnType } from "../types/ColumnTypes"
import { CteCapabilities } from "../types/CteCapabilities"
import { DataTypeDefaults } from "../types/DataTypeDefaults"
import { MappedColumnTypes } from "../types/MappedColumnTypes"
import { ReplicationMode } from "../types/ReplicationMode"
import { UpsertType } from "../types/UpsertType"
import { PostgresJsConnectionCredentialsOptions } from "./PostgresJsConnectionCredentialsOptions"
import { PostgresJsConnectionOptions } from "./PostgresJsConnectionOptions"
import { PostgresJsQueryRunner } from "./PostgresJsQueryRunner"
import { PostgresJsUtils } from "./PostgresJsUtils"

/**
 * Driver implementation for postgres.js library.
 * postgres.js uses a function-based API unlike traditional pool-based drivers.
 */
export class PostgresJsDriver implements Driver {
    // -------------------------------------------------------------------------
    // Public Properties
    // -------------------------------------------------------------------------

    connection: DataSource

    /**
     * postgres.js library reference.
     */
    postgresLib: any

    /**
     * Master SQL function for write operations.
     * In postgres.js, this is a function, not a pool.
     */
    masterSql: any

    /**
     * Slave SQL functions for read operations.
     */
    slaveSqls: any[] = []

    /**
     * Active query runners tracked for cleanup.
     */
    connectedQueryRunners: QueryRunner[] = []

    // -------------------------------------------------------------------------
    // Public Implemented Properties
    // -------------------------------------------------------------------------

    options: PostgresJsConnectionOptions

    version?: string

    database?: string

    schema?: string

    searchSchema?: string

    isReplicated: boolean = false

    treeSupport = true

    transactionSupport = "nested" as const

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
        "jsonpath",
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

    supportedUpsertTypes: UpsertType[] = ["on-conflict-do-update"]

    spatialTypes: ColumnType[] = ["geometry", "geography"]

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

    withPrecisionColumnTypes: ColumnType[] = [
        "numeric",
        "decimal",
        "interval",
        "time without time zone",
        "time with time zone",
        "timestamp without time zone",
        "timestamp with time zone",
    ]

    withScaleColumnTypes: ColumnType[] = ["numeric", "decimal"]

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

    supportedIndexTypes: TableIndexTypes[] = [
        "brin",
        "btree",
        "gin",
        "gist",
        "hash",
        "spgist",
    ]

    parametersPrefix: string = "$"

    dataTypeDefaults: DataTypeDefaults = {
        character: { length: 1 },
        bit: { length: 1 },
        interval: { precision: 6 },
        "time without time zone": { precision: 6 },
        "time with time zone": { precision: 6 },
        "timestamp without time zone": { precision: 6 },
        "timestamp with time zone": { precision: 6 },
    }

    maxAliasLength = 63

    isGeneratedColumnsSupported: boolean = false

    cteCapabilities: CteCapabilities = {
        enabled: true,
        writable: true,
        requiresRecursiveHint: true,
        materializedHint: true,
    }

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(datasource?: DataSource) {
        if (!datasource) {
            return
        }

        this.connection = datasource
        this.options = datasource.options as unknown as PostgresJsConnectionOptions
        this.isReplicated = this.options.replication ? true : false

        if (this.options.useUTC) {
            process.env.PGTZ = "UTC"
        }

        this.initializeLibrary()

        this.database = DriverUtils.buildDriverOptions(
            this.options.replication ? this.options.replication.master : this.options,
        ).database
        this.schema = DriverUtils.buildDriverOptions(this.options).schema
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    async connect(): Promise<void> {
        if (this.options.replication) {
            this.slaveSqls = await Promise.all(
                this.options.replication.slaves.map((slaveConfig) => {
                    return this.initializeSqlFunction(this.options, slaveConfig)
                }),
            )
            this.masterSql = await this.initializeSqlFunction(
                this.options,
                this.options.replication.master,
            )
        } else {
            this.masterSql = await this.initializeSqlFunction(this.options, this.options)
        }

        if (!this.version || !this.database || !this.searchSchema) {
            const qr = this.createQueryRunner("master")

            if (!this.version) {
                this.version = await qr.getVersion()
            }

            if (!this.database) {
                this.database = await qr.getCurrentDatabase()
            }

            if (!this.searchSchema) {
                this.searchSchema = await qr.getCurrentSchema()
            }

            await qr.release()
        }

        if (!this.schema) {
            this.schema = this.searchSchema
        }
    }

    async afterConnect(): Promise<void> {
        const installExtensions =
            this.options.installExtensions === undefined ||
            this.options.installExtensions

        if (installExtensions) {
            const extensionsMetadata = await this.findRequiredExtensions()
            const extensionsToInstall = this.options.extensions

            if (extensionsMetadata.hasExtensions) {
                await this.setupExtensions(extensionsMetadata)
            }

            if (extensionsToInstall) {
                const availableExts = await this.queryAvailableExtensions()
                await this.setupCustomExtensions(availableExts, extensionsToInstall)
            }
        }

        this.isGeneratedColumnsSupported = VersionUtils.isGreaterOrEqual(
            this.version,
            "12.0",
        )
    }

    async disconnect(): Promise<void> {
        if (!this.masterSql) {
            throw new ConnectionIsNotSetError("postgresjs")
        }

        await this.terminateSqlFunction(this.masterSql)
        await Promise.all(this.slaveSqls.map((sql) => this.terminateSqlFunction(sql)))

        this.masterSql = undefined
        this.slaveSqls = []
    }

    createSchemaBuilder() {
        return new RdbmsSchemaBuilder(this.connection)
    }

    createQueryRunner(mode: ReplicationMode): PostgresJsQueryRunner {
        return new PostgresJsQueryRunner(this, mode)
    }

    preparePersistentValue(value: any, columnMetadata: ColumnMetadata): any {
        if (columnMetadata.transformer)
            value = ApplyValueTransformers.transformTo(
                columnMetadata.transformer,
                value,
            )

        if (value === null || value === undefined) return value

        if (columnMetadata.type === Boolean) {
            return value === true ? 1 : 0
        } else if (columnMetadata.type === "date") {
            return DateUtils.mixedDateToDateString(value, {
                utc: columnMetadata.utc,
            })
        } else if (columnMetadata.type === "time") {
            return DateUtils.mixedDateToTimeString(value)
        } else if (
            columnMetadata.type === "datetime" ||
            columnMetadata.type === Date ||
            columnMetadata.type === "timestamp" ||
            columnMetadata.type === "timestamptz" ||
            columnMetadata.type === "timestamp with time zone" ||
            columnMetadata.type === "timestamp without time zone"
        ) {
            return DateUtils.mixedDateToDate(value)
        } else if (columnMetadata.type === "point") {
            if (
                typeof value === "object" &&
                value.x !== undefined &&
                value.y !== undefined
            ) {
                return `(${value.x},${value.y})`
            }
            return value
        } else if (columnMetadata.type === "circle") {
            if (
                typeof value === "object" &&
                value.x !== undefined &&
                value.y !== undefined &&
                value.radius !== undefined
            ) {
                return `<(${value.x},${value.y}),${value.radius}>`
            }
            return value
        } else if (
            ["json", "jsonb", ...this.spatialTypes].indexOf(columnMetadata.type) >= 0
        ) {
            return JSON.stringify(value)
        } else if (
            columnMetadata.type === "vector" ||
            columnMetadata.type === "halfvec"
        ) {
            if (Array.isArray(value)) {
                return `[${value.join(",")}]`
            } else {
                return value
            }
        } else if (columnMetadata.type === "hstore") {
            if (typeof value === "string") {
                return value
            } else {
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
        } else if (columnMetadata.type === "simple-array") {
            return DateUtils.simpleArrayToString(value)
        } else if (columnMetadata.type === "simple-json") {
            return DateUtils.simpleJsonToString(value)
        } else if (columnMetadata.type === "cube") {
            if (columnMetadata.isArray) {
                return `{${value
                    .map((cube: number[]) => `"(${cube.join(",")})"`)
                    .join(",")}}`
            }
            return `(${value.join(",")})`
        } else if (columnMetadata.type === "ltree") {
            return value.split(".").filter(Boolean).join(".").replace(/[\s]+/g, "_")
        } else if (
            (columnMetadata.type === "enum" ||
                columnMetadata.type === "simple-enum") &&
            !columnMetadata.isArray
        ) {
            return "" + value
        }

        return value
    }

    prepareHydratedValue(value: any, columnMetadata: ColumnMetadata): any {
        if (value === null || value === undefined)
            return columnMetadata.transformer
                ? ApplyValueTransformers.transformFrom(
                      columnMetadata.transformer,
                      value,
                  )
                : value

        if (columnMetadata.type === Boolean) {
            value = value ? true : false
        } else if (
            columnMetadata.type === "datetime" ||
            columnMetadata.type === Date ||
            columnMetadata.type === "timestamp" ||
            columnMetadata.type === "timestamptz" ||
            columnMetadata.type === "timestamp with time zone" ||
            columnMetadata.type === "timestamp without time zone"
        ) {
            value = DateUtils.normalizeHydratedDate(value)
        } else if (columnMetadata.type === "date") {
            value = DateUtils.mixedDateToDateString(value, {
                utc: columnMetadata.utc,
            })
        } else if (columnMetadata.type === "time") {
            value = DateUtils.mixedTimeToString(value)
        } else if (
            columnMetadata.type === "vector" ||
            columnMetadata.type === "halfvec"
        ) {
            if (
                typeof value === "string" &&
                value.startsWith("[") &&
                value.endsWith("]")
            ) {
                if (value === "[]") return []
                return value.slice(1, -1).split(",").map(Number)
            }
        } else if (columnMetadata.type === "hstore") {
            if (columnMetadata.hstoreType === "object") {
                const unescapeString = (str: string) =>
                    str.replace(/\\./g, (m) => m[1])
                const regexp =
                    /"([^"\\]*(?:\\.[^"\\]*)*)"=>(?:(NULL)|"([^"\\]*(?:\\.[^"\\]*)*)")(?:,|$)/g
                const object: ObjectLiteral = {}
                ;`${value}`.replace(regexp, (_, key, nullValue, stringValue) => {
                    object[unescapeString(key)] = nullValue
                        ? null
                        : unescapeString(stringValue)
                    return ""
                })
                value = object
            }
        } else if (columnMetadata.type === "simple-array") {
            value = DateUtils.stringToSimpleArray(value)
        } else if (columnMetadata.type === "simple-json") {
            value = DateUtils.stringToSimpleJson(value)
        } else if (columnMetadata.type === "cube") {
            value = value.replace(/[()\s]+/g, "")
            if (columnMetadata.isArray) {
                const regexp = /(?:"((?:[\d\s.,])*)")|(?:(NULL))/g
                const unparsedArrayString = value
                value = []
                let cube: RegExpExecArray | null = null
                while ((cube = regexp.exec(unparsedArrayString)) !== null) {
                    if (cube[1] !== undefined) {
                        value.push(cube[1].split(",").filter(Boolean).map(Number))
                    } else {
                        value.push(undefined)
                    }
                }
            } else {
                value = value.split(",").filter(Boolean).map(Number)
            }
        } else if (
            columnMetadata.type === "enum" ||
            columnMetadata.type === "simple-enum"
        ) {
            if (columnMetadata.isArray) {
                if (value === "{}") return []
                value = (value as string)
                    .slice(1, -1)
                    .split(",")
                    .map((val) => {
                        if (val.startsWith(`"`) && val.endsWith(`"`))
                            val = val.slice(1, -1)
                        return val.replace(/\\(\\|")/g, "$1")
                    })
                value = value.map((val: string) => {
                    return !isNaN(+val) &&
                        columnMetadata.enum!.indexOf(parseInt(val)) >= 0
                        ? parseInt(val)
                        : val
                })
            } else {
                value =
                    !isNaN(+value) &&
                    columnMetadata.enum!.indexOf(parseInt(value)) >= 0
                        ? parseInt(value)
                        : value
            }
        } else if (columnMetadata.type === Number) {
            value = !isNaN(+value) ? parseInt(value) : value
        }

        if (columnMetadata.transformer)
            value = ApplyValueTransformers.transformFrom(
                columnMetadata.transformer,
                value,
            )
        return value
    }

    escapeQueryWithParameters(
        sql: string,
        parameters: ObjectLiteral,
        nativeParameters: ObjectLiteral,
    ): [string, any[]] {
        const escapedParameters: any[] = Object.keys(nativeParameters).map(
            (key) => nativeParameters[key],
        )
        if (!parameters || !Object.keys(parameters).length)
            return [sql, escapedParameters]

        const parameterIndexMap = new Map<string, number>()
        sql = sql.replace(
            /:(\.\.\.)?([A-Za-z0-9_.]+)/g,
            (full, isArray: string, key: string): string => {
                if (!parameters.hasOwnProperty(key)) {
                    return full
                }

                if (parameterIndexMap.has(key)) {
                    return this.parametersPrefix + parameterIndexMap.get(key)
                }

                const value: any = parameters[key]

                if (isArray) {
                    return (value as any[])
                        .map((v: any) => {
                            escapedParameters.push(v)
                            return this.parametersPrefix + escapedParameters.length
                        })
                        .join(",")
                }

                escapedParameters.push(value)
                parameterIndexMap.set(key, escapedParameters.length)
                return this.parametersPrefix + escapedParameters.length
            },
        )

        return [sql, escapedParameters]
    }

    escape(columnName: string): string {
        return '"' + columnName + '"'
    }

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
            // EntityMetadata tableName is never a path

            return {
                database: target.database || driverDatabase,
                schema: target.schema || driverSchema,
                tableName: target.tableName,
            }
        }

        const parts = target.split(".")

        return {
            database: driverDatabase,
            schema: (parts.length > 1 ? parts[0] : undefined) || driverSchema,
            tableName: parts.length > 1 ? parts[1] : parts[0],
        }
    }

    buildTableName(tableName: string, schema?: string, database?: string): string {
        const schemaToUse =
            schema || this.schema || (this.searchSchema !== "public" ? this.searchSchema : undefined)

        if (schemaToUse && schemaToUse !== "public") {
            return `${this.escape(schemaToUse)}.${this.escape(tableName)}`
        }

        return this.escape(tableName)
    }

    normalizeType(column: {
        type?: ColumnType | string
        length?: number | string
        precision?: number | null
        scale?: number
    }): string {
        if (column.type === Number || column.type === "integer") {
            return "integer"
        } else if (column.type === String) {
            return "character varying"
        } else if (column.type === Date) {
            return "timestamp without time zone"
        } else if ((column.type as any) === Buffer) {
            return "bytea"
        } else if (column.type === Boolean) {
            return "boolean"
        } else {
            return (column.type as string) || ""
        }
    }

    normalizeDefault(columnMetadata: ColumnMetadata): string | undefined {
        const defaultValue = columnMetadata.default

        if (
            defaultValue === null ||
            defaultValue === undefined ||
            typeof defaultValue === "function"
        ) {
            return undefined
        }

        if (typeof defaultValue === "number") {
            return `${defaultValue}`
        }

        if (typeof defaultValue === "boolean") {
            return defaultValue ? "true" : "false"
        }

        if (typeof defaultValue === "string") {
            return `'${defaultValue}'`
        }

        if (typeof defaultValue === "object") {
            return `'${JSON.stringify(defaultValue)}'`
        }

        return undefined
    }

    normalizeIsUnique(column: ColumnMetadata): boolean {
        return column.entityMetadata.uniques.some(
            (uq) => uq.columns.length === 1 && uq.columns[0] === column,
        )
    }

    getColumnLength(column: ColumnMetadata): string {
        return column.length ? column.length.toString() : ""
    }

    createFullType(column: TableColumn): string {
        let type = column.type

        if (column.length) {
            type += "(" + column.length + ")"
        } else if (
            column.precision !== null &&
            column.precision !== undefined &&
            column.scale !== null &&
            column.scale !== undefined
        ) {
            type += "(" + column.precision + "," + column.scale + ")"
        } else if (column.precision !== null && column.precision !== undefined) {
            type += "(" + column.precision + ")"
        }

        if (column.type === "time without time zone") {
            type =
                "TIME" +
                (column.precision !== null && column.precision !== undefined
                    ? "(" + column.precision + ")"
                    : "")
        } else if (column.type === "time with time zone") {
            type =
                "TIME" +
                (column.precision !== null && column.precision !== undefined
                    ? "(" + column.precision + ")"
                    : "") +
                " WITH TIME ZONE"
        } else if (column.type === "timestamp without time zone") {
            type =
                "TIMESTAMP" +
                (column.precision !== null && column.precision !== undefined
                    ? "(" + column.precision + ")"
                    : "")
        } else if (column.type === "timestamp with time zone") {
            type =
                "TIMESTAMP" +
                (column.precision !== null && column.precision !== undefined
                    ? "(" + column.precision + ")"
                    : "") +
                " WITH TIME ZONE"
        } else if (this.spatialTypes.indexOf(column.type as ColumnType) >= 0) {
            if (column.spatialFeatureType != null && column.srid != null) {
                type = `${column.type}(${column.spatialFeatureType},${column.srid})`
            } else if (column.spatialFeatureType != null) {
                type = `${column.type}(${column.spatialFeatureType})`
            } else {
                type = column.type
            }
        } else if (column.type === "vector" || column.type === "halfvec") {
            type = column.type + (column.length ? "(" + column.length + ")" : "")
        }

        if (column.isArray) type += " array"

        return type
    }

    async obtainMasterConnection(): Promise<any> {
        if (!this.masterSql) {
            throw new TypeORMError("Driver not connected")
        }
        return this.masterSql
    }

    async obtainSlaveConnection(): Promise<any> {
        if (!this.slaveSqls.length) {
            return this.obtainMasterConnection()
        }

        const randomIndex = Math.floor(Math.random() * this.slaveSqls.length)
        return this.slaveSqls[randomIndex]
    }

    createGeneratedMap(metadata: EntityMetadata, insertResult: ObjectLiteral) {
        if (!insertResult) return undefined

        return Object.keys(insertResult).reduce((map, key) => {
            const column = metadata.findColumnWithDatabaseName(key)
            if (column) {
                OrmUtils.mergeDeep(map, column.createValueMap(insertResult[key]))
            }
            return map
        }, {} as ObjectLiteral)
    }

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
                tableColumn.length !== columnMetadata.length ||
                tableColumn.isArray !== columnMetadata.isArray ||
                tableColumn.precision !== columnMetadata.precision ||
                (columnMetadata.scale !== undefined &&
                    tableColumn.scale !== columnMetadata.scale) ||
                tableColumn.comment !== this.escapeComment(columnMetadata.comment) ||
                (!tableColumn.isGenerated &&
                    this.lowerDefaultValueIfNecessary(
                        this.normalizeDefault(columnMetadata),
                    ) !== tableColumn.default) ||
                tableColumn.isPrimary !== columnMetadata.isPrimary ||
                tableColumn.isNullable !== columnMetadata.isNullable ||
                tableColumn.isUnique !== this.normalizeIsUnique(columnMetadata) ||
                (columnMetadata.generationStrategy !== "uuid" &&
                    tableColumn.isGenerated !== columnMetadata.isGenerated)

            return isColumnChanged
        })
    }

    isReturningSqlSupported(returningType: ReturningType): boolean {
        return true
    }

    isUUIDGenerationSupported(): boolean {
        return true
    }

    isArraysSupported(): boolean {
        return true
    }

    createParameter(parameterName: string, index: number): string {
        return "$" + (index + 1)
    }

    supportsTransactionIsolationLevel(): boolean {
        return true
    }

    supportSavepoints(): boolean {
        return true
    }

    supportBulkInsert(): boolean {
        return true
    }

    supportReturnValues(returningType: ReturningType): boolean {
        return true
    }

    createIndexUniqueName(entityName: string, columns: string[]): string {
        return `IDX_${columns.join("_")}`
    }

    createForeignKeyUniqueName(
        entityName: string,
        columns: string[],
        referencedEntityName: string,
        referencedColumns: string[],
    ): string {
        return `FK_${columns.join("_")}_${referencedColumns.join("_")}`
    }

    async queriesForIndex(index: IndexMetadata): Promise<string[]> {
        const queries: string[] = []
        const indexName = index.name || this.createIndexUniqueName(index.entityMetadata.tableName, index.columns.map(c => c.databaseName))
        
        const columnNames = index.columns.map(c => this.escape(c.databaseName))
        const indexType = index.type ? ` USING ${index.type}` : ""
        const uniqueStr = index.isUnique ? "UNIQUE " : ""
        const tableName = this.buildTableName(
            index.entityMetadata.tableName,
            index.entityMetadata.schema,
            index.entityMetadata.database
        )

        queries.push(
            `CREATE ${uniqueStr}INDEX ${this.escape(indexName)} ON ${tableName}${indexType} (${columnNames.join(", ")})`
        )

        return queries
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    protected initializeLibrary(): void {
        try {
            const postgres = this.options.driver || PlatformTools.load("postgres")
            this.postgresLib = postgres
        } catch (e) {
            throw new DriverPackageNotInstalledError("Postgres.js", "postgres")
        }
    }

    protected async initializeSqlFunction(
        options: PostgresJsConnectionOptions,
        credentials: PostgresJsConnectionCredentialsOptions,
    ): Promise<any> {
        const config = PostgresJsUtils.buildSqlFunctionConfig(credentials, options)

        // postgres.js returns a function that manages connections internally
        const sql = this.postgresLib(config)

        // Test the connection
        try {
            await sql`SELECT 1`
        } catch (error) {
            throw new TypeORMError(
                `Failed to initialize postgres.js connection: ${error.message}`,
            )
        }

        return sql
    }

    protected async terminateSqlFunction(sql: any): Promise<void> {
        while (this.connectedQueryRunners.length) {
            await this.connectedQueryRunners[0].release()
        }

        // postgres.js uses .end() to close all connections
        await sql.end({ timeout: 5 })
    }

    protected async findRequiredExtensions() {
        const hasUuidColumns = this.connection.entityMetadatas.some((metadata) => {
            return (
                metadata.generatedColumns.filter(
                    (column) => column.generationStrategy === "uuid",
                ).length > 0
            )
        })

        const hasCitextColumns = this.connection.entityMetadatas.some((metadata) => {
            return (
                metadata.columns.filter((column) => column.type === "citext")
                    .length > 0
            )
        })

        const hasHstoreColumns = this.connection.entityMetadatas.some((metadata) => {
            return (
                metadata.columns.filter((column) => column.type === "hstore")
                    .length > 0
            )
        })

        const hasCubeColumns = this.connection.entityMetadatas.some((metadata) => {
            return (
                metadata.columns.filter((column) => column.type === "cube").length > 0
            )
        })

        const hasGeometryColumns = this.connection.entityMetadatas.some((metadata) => {
            return (
                metadata.columns.filter(
                    (column) => this.spatialTypes.indexOf(column.type) >= 0,
                ).length > 0
            )
        })

        const hasLtreeColumns = this.connection.entityMetadatas.some((metadata) => {
            return (
                metadata.columns.filter((column) => column.type === "ltree").length >
                0
            )
        })

        const hasVectorColumns = this.connection.entityMetadatas.some((metadata) => {
            return metadata.columns.some(
                (column) => column.type === "vector" || column.type === "halfvec",
            )
        })

        const hasExclusionConstraints = this.connection.entityMetadatas.some(
            (metadata) => {
                return metadata.exclusions.length > 0
            },
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

    protected async setupExtensions(extensionsMetadata: any): Promise<void> {
        const sql = await this.obtainMasterConnection()
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
        } = extensionsMetadata

        if (hasUuidColumns) {
            try {
                const ext = this.options.uuidExtension || "uuid-ossp"
                await sql.unsafe(`CREATE EXTENSION IF NOT EXISTS "${ext}"`)
            } catch (_) {
                logger.log(
                    "warn",
                    `Unable to install uuid extension '${
                        this.options.uuidExtension || "uuid-ossp"
                    }'. Requires superuser privileges.`,
                )
            }
        }

        if (hasCitextColumns) {
            try {
                await sql.unsafe(`CREATE EXTENSION IF NOT EXISTS "citext"`)
            } catch (_) {
                logger.log(
                    "warn",
                    "Unable to install citext extension. Requires superuser privileges.",
                )
            }
        }

        if (hasHstoreColumns) {
            try {
                await sql.unsafe(`CREATE EXTENSION IF NOT EXISTS "hstore"`)
            } catch (_) {
                logger.log(
                    "warn",
                    "Unable to install hstore extension. Requires superuser privileges.",
                )
            }
        }

        if (hasCubeColumns) {
            try {
                await sql.unsafe(`CREATE EXTENSION IF NOT EXISTS "cube"`)
            } catch (_) {
                logger.log(
                    "warn",
                    "Unable to install cube extension. Requires superuser privileges.",
                )
            }
        }

        if (hasGeometryColumns) {
            try {
                await sql.unsafe(`CREATE EXTENSION IF NOT EXISTS "postgis"`)
            } catch (_) {
                logger.log(
                    "warn",
                    "Unable to install postgis extension. Requires superuser privileges.",
                )
            }
        }

        if (hasLtreeColumns) {
            try {
                await sql.unsafe(`CREATE EXTENSION IF NOT EXISTS "ltree"`)
            } catch (_) {
                logger.log(
                    "warn",
                    "Unable to install ltree extension. Requires superuser privileges.",
                )
            }
        }

        if (hasVectorColumns) {
            try {
                await sql.unsafe(`CREATE EXTENSION IF NOT EXISTS "vector"`)
            } catch (_) {
                logger.log(
                    "warn",
                    "Unable to install vector extension. Requires superuser privileges.",
                )
            }
        }

        if (hasExclusionConstraints) {
            try {
                await sql.unsafe(`CREATE EXTENSION IF NOT EXISTS "btree_gist"`)
            } catch (_) {
                logger.log(
                    "warn",
                    "Unable to install btree_gist extension. Requires superuser privileges.",
                )
            }
        }
    }

    protected async queryAvailableExtensions(): Promise<Set<string>> {
        const sql = await this.obtainMasterConnection()
        const availableExtensions = new Set<string>()
        const { logger } = this.connection

        try {
            const result = await sql.unsafe(`SELECT name FROM pg_available_extensions`)
            result.forEach((row: any) => {
                availableExtensions.add(row.name)
            })
        } catch (_) {
            logger.log(
                "warn",
                "Could not query available extensions. Custom extension installation may fail.",
            )
        }

        return availableExtensions
    }

    protected async setupCustomExtensions(
        availableExtensions: Set<string>,
        extensionsToInstall: string[],
    ): Promise<void> {
        const sql = await this.obtainMasterConnection()
        const { logger } = this.connection

        for (const extension of extensionsToInstall) {
            if (!availableExtensions.has(extension)) {
                logger.log(
                    "warn",
                    `Extension "${extension}" is not available on this database.`,
                )
                continue
            }

            try {
                await sql.unsafe(`CREATE EXTENSION IF NOT EXISTS "${extension}"`)
            } catch (_) {
                logger.log(
                    "warn",
                    `Unable to install extension "${extension}". Requires superuser privileges.`,
                )
            }
        }
    }

    protected lowerDefaultValueIfNecessary(value: string | undefined): string | undefined {
        if (!value) return value
        return value.toLowerCase()
    }

    protected escapeComment(comment?: string): string | undefined {
        if (!comment) return comment
        return comment.replace(/\u0000/g, "")
    }

    isFullTextColumnTypeSupported(): boolean {
        return false
    }
}
